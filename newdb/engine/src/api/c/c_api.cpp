#include <newdb/c_api.h>
#include <newdb/schema.h>
#include <newdb/schema_io.h>

#include "cli/shell/dispatch/router/dispatch.h"
#include "cli/shell/bootstrap/demo_runner.h"
#include "cli/shell/state/shell_state.h"

#include <cstdio>
#include <cstring>
#include <atomic>
#include <filesystem>
#include <fstream>
#include <memory>
#include <exception>
#include <string>
#include <sstream>
#include <chrono>
#include <cctype>

namespace {

#define NEWDB_STR_IMPL(x) #x
#define NEWDB_STR(x) NEWDB_STR_IMPL(x)

static constexpr const char* kNewdbCApiVersionString =
    "newdb-c-api/"
    NEWDB_STR(NEWDB_C_API_VERSION_MAJOR) "."
    NEWDB_STR(NEWDB_C_API_VERSION_MINOR) "."
    NEWDB_STR(NEWDB_C_API_VERSION_PATCH);

struct CApiSession {
    ShellState shell;
    std::string table_name;
    std::string log_path;
    int last_error{NEWDB_OK};
};

newdb_schema_check_result make_result(int ok, const std::string& msg) {
    newdb_schema_check_result out{};
    out.ok = ok;
    if (msg.empty()) {
        out.message[0] = '\0';
        return out;
    }

    const size_t cap = sizeof(out.message);
    std::strncpy(out.message, msg.c_str(), cap - 1);
    out.message[cap - 1] = '\0';
    return out;
}

struct TailReadResult {
    std::string data;
    bool ok{true};
};

TailReadResult read_file_tail(const std::string& path, std::uintmax_t start_pos) {
    TailReadResult out{};
    std::error_code ec;
    const auto file_size = std::filesystem::file_size(path, ec);
    if (ec || file_size <= start_pos) {
        out.ok = !ec;
        return out;
    }
    std::ifstream in(path, std::ios::binary);
    if (!in.good()) {
        out.ok = false;
        return out;
    }
    in.seekg(static_cast<std::streamoff>(start_pos), std::ios::beg);
    out.data.assign(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
    return out;
}

bool apply_table(CApiSession& s, const char* table_name) {
    if (table_name == nullptr || table_name[0] == '\0') {
        return false;
    }
    s.table_name = table_name;
    const std::string data_file = s.table_name + ".bin";
    s.shell.session.table_name = s.table_name;
    s.shell.session.data_path = resolve_table_file(s.shell, data_file);
    reload_schema_from_data_path(s.shell, s.shell.session.data_path);
    return true;
}

void set_last_error(CApiSession* s, int code) {
    if (s != nullptr) {
        s->last_error = code;
    }
}

bool output_indicates_business_error(const std::string& out) {
    if (out.find("[ERROR]") != std::string::npos) {
        return true;
    }
    if (out.find("expects ") != std::string::npos && out.find(", got '") != std::string::npos) {
        return true;
    }
    if (out.find(" failed") != std::string::npos || out.find(" invalid") != std::string::npos) {
        return true;
    }
    if (out.find("duplicate ") != std::string::npos || out.find(" missing ") != std::string::npos) {
        return true;
    }
    if (out.find("usage:") != std::string::npos) {
        return true;
    }
    return false;
}

std::string trim_copy(const std::string& in) {
    size_t begin = 0;
    while (begin < in.size() && std::isspace(static_cast<unsigned char>(in[begin])) != 0) {
        ++begin;
    }
    size_t end = in.size();
    while (end > begin && std::isspace(static_cast<unsigned char>(in[end - 1])) != 0) {
        --end;
    }
    return in.substr(begin, end - begin);
}

bool starts_with_ci(const std::string& text, const std::string& prefix_upper) {
    if (text.size() < prefix_upper.size()) {
        return false;
    }
    for (size_t i = 0; i < prefix_upper.size(); ++i) {
        const char c = static_cast<char>(std::toupper(static_cast<unsigned char>(text[i])));
        if (c != prefix_upper[i]) {
            return false;
        }
    }
    return true;
}

std::string normalize_paren_txn_command(const std::string& raw) {
    const std::string trimmed = trim_copy(raw);
    if (trimmed.empty()) {
        return raw;
    }

    auto normalize_after_prefix = [&](const char* canonical_prefix) -> std::string {
        const std::string prefix = canonical_prefix;
        std::string rest = trim_copy(trimmed.substr(prefix.size()));
        if (rest.empty() || rest.front() != '(' || rest.back() != ')') {
            return raw;
        }
        rest = trim_copy(rest.substr(1, rest.size() - 2));
        if (rest.empty()) {
            return raw;
        }
        return prefix + " " + rest;
    };

    if (starts_with_ci(trimmed, "SAVEPOINT")) {
        return normalize_after_prefix("SAVEPOINT");
    }
    if (starts_with_ci(trimmed, "ROLLBACK TO SAVEPOINT")) {
        return normalize_after_prefix("ROLLBACK TO SAVEPOINT");
    }
    if (starts_with_ci(trimmed, "ROLLBACK TO")) {
        return normalize_after_prefix("ROLLBACK TO");
    }
    if (starts_with_ci(trimmed, "RELEASE SAVEPOINT")) {
        return normalize_after_prefix("RELEASE SAVEPOINT");
    }
    return raw;
}

void prepend_capi_error_line(std::string& out, int code) {
    if (code == NEWDB_OK) {
        return;
    }
    const std::string prefix =
        std::string("[CAPI_ERROR] code=") + newdb_error_code_string(code) + " numeric=" + std::to_string(code) + "\n";
    out.insert(0, prefix);
}

std::string json_escape_local(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 8);
    for (unsigned char c : s) {
        switch (c) {
            case '\"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\b': out += "\\b"; break;
            case '\f': out += "\\f"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (c < 0x20) {
                    char buf[7];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned int>(c));
                    out += buf;
                } else {
                    out.push_back(static_cast<char>(c));
                }
        }
    }
    return out;
}

std::string build_runtime_stats_json(const CApiSession& s) {
    const auto stats = s.shell.txn.runtimeStats();
    const auto isolation = s.shell.txn.txnIsolationLevel();
    const char* isolation_s = (isolation == TxnIsolationLevel::ReadCommitted) ? "read_committed" : "snapshot";
    std::ostringstream oss;
    oss << "{"
        << "\"txn_isolation\":\"" << isolation_s << "\","
        << "\"vacuum_trigger_count\":" << stats.vacuum_trigger_count << ","
        << "\"vacuum_execute_count\":" << stats.vacuum_execute_count << ","
        << "\"vacuum_cooldown_skip_count\":" << stats.vacuum_cooldown_skip_count << ","
        << "\"vacuum_compact_success_count\":" << stats.vacuum_compact_success_count << ","
        << "\"vacuum_compact_failure_count\":" << stats.vacuum_compact_failure_count << ","
        << "\"vacuum_compact_bytes_reclaimed\":" << stats.vacuum_compact_bytes_reclaimed << ","
        << "\"vacuum_compact_last_elapsed_ms\":" << stats.vacuum_compact_last_elapsed_ms << ","
        << "\"vacuum_queue_depth\":" << stats.vacuum_queue_depth << ","
        << "\"vacuum_queue_depth_peak\":" << stats.vacuum_queue_depth_peak << ","
        << "\"maintenance_checkpoint_trigger_count\":" << stats.maintenance_checkpoint_trigger_count << ","
        << "\"maintenance_checkpoint_vacuum_enqueue_count\":" << stats.maintenance_checkpoint_vacuum_enqueue_count << ","
        << "\"write_conflicts\":" << stats.write_conflict_count << ","
        << "\"write_conflict_wait_count\":" << stats.write_conflict_wait_count << ","
        << "\"write_conflict_wait_timeout_count\":" << stats.write_conflict_wait_timeout_count << ","
        << "\"lock_wait_ms_total\":" << stats.lock_wait_ms_total << ","
        << "\"lock_wait_max_ms\":" << stats.lock_wait_max_ms << ","
        << "\"lock_deadlock_detect_count\":" << stats.lock_deadlock_detect_count << ","
        << "\"lock_deadlock_victim_count\":" << stats.lock_deadlock_victim_count << ","
        << "\"txn_begin_lock_conflicts\":" << stats.txn_begin_lock_conflict_count << ","
        << "\"wal_compact_count\":" << stats.wal_compact_count << ","
        << "\"wal_recovery_runs\":" << stats.wal_recovery_runs << ","
        << "\"wal_recovery_undo_ops\":" << stats.wal_recovery_undo_ops << ","
        << "\"wal_recovery_last_elapsed_ms\":" << stats.wal_recovery_last_elapsed_ms << ","
        << "\"wal_recovery_analyze_ms\":" << stats.wal_recovery_analyze_ms << ","
        << "\"wal_recovery_undo_ms\":" << stats.wal_recovery_undo_ms << ","
        << "\"wal_recovery_finalize_ms\":" << stats.wal_recovery_finalize_ms << ","
        << "\"wal_recovery_records_scanned\":" << stats.wal_recovery_records_scanned << ","
        << "\"wal_recovery_dangling_txns\":" << stats.wal_recovery_dangling_txns << ","
        << "\"wal_recovery_redo_ms\":" << stats.wal_recovery_redo_ms << ","
        << "\"wal_recovery_checkpoint_begin_count\":" << stats.wal_recovery_checkpoint_begin_count << ","
        << "\"wal_recovery_checkpoint_end_count\":" << stats.wal_recovery_checkpoint_end_count << ","
        << "\"wal_recovery_policy\":\"" << json_escape_local(stats.wal_recovery_policy) << "\","
        << "\"write_conflict_last_sample\":\"" << json_escape_local(stats.write_conflict_last_sample) << "\","
        << "\"file_lock_acquire_fail_count\":" << stats.file_lock_acquire_fail_count << ","
        << "\"file_lock_same_process_reuse_count\":" << stats.file_lock_same_process_reuse_count << ","
        << "\"file_lock_stale_marker_count\":" << stats.file_lock_stale_marker_count << ","
        << "\"sidecar_invalidate_count\":" << stats.sidecar_invalidate_count << ","
        << "\"sidecar_invalidate_fail_count\":" << stats.sidecar_invalidate_fail_count << ","
        << "\"wal_group_commit_count\":" << stats.wal_group_commit_count << ","
        << "\"wal_group_commit_batch_commits\":" << stats.wal_group_commit_batch_commits << ","
        << "\"wal_group_commit_pending_commits\":" << stats.wal_group_commit_pending_commits << ","
        << "\"txn_commit_count\":" << stats.txn_commit_count << ","
        << "\"txn_commit_p95_ms\":" << stats.txn_commit_p95_ms << ","
        << "\"txn_commit_max_ms\":" << stats.txn_commit_max_ms << ","
        << "\"wal_bytes_since_start\":" << stats.wal_bytes_since_start << ","
        << "\"wal_bytes_per_commit_avg\":" << stats.wal_bytes_per_commit_avg << ","
        << "\"lock_wait_p95_ms\":" << stats.lock_wait_p95_ms << ","
        << "\"scheduler_throttle_count\":" << stats.scheduler_throttle_count << ","
        << "\"hot_index_enabled\":" << (stats.hot_index_enabled ? "true" : "false") << ","
        << "\"segment_target_bytes\":" << stats.segment_target_bytes << ","
        << "\"lsm_memtable_flush_count\":" << stats.lsm_memtable_flush_count << ","
        << "\"lsm_compaction_count\":" << stats.lsm_compaction_count << ","
        << "\"lsm_segment_count\":" << stats.lsm_segment_count << ","
        << "\"lsm_memtable_bytes\":" << stats.lsm_memtable_bytes << ","
        << "\"lsm_read_segments_scanned\":" << stats.lsm_read_segments_scanned << ","
        << "\"lsm_read_segments_scanned_p95\":" << stats.lsm_read_segments_scanned_p95 << ","
        << "\"lsm_compaction_bytes_in\":" << stats.lsm_compaction_bytes_in << ","
        << "\"lsm_compaction_bytes_out\":" << stats.lsm_compaction_bytes_out << ","
        << "\"lsm_compaction_queue_pending\":" << stats.lsm_compaction_queue_pending << ","
        << "\"lsm_compaction_queue_inflight\":" << stats.lsm_compaction_queue_inflight << ","
        << "\"lsm_compaction_enqueue_skipped_backpressure\":"
        << stats.lsm_compaction_enqueue_skipped_backpressure << ","
        << "\"lsm_segment_cache_hits\":" << stats.lsm_segment_cache_hits << ","
        << "\"lsm_segment_cache_misses\":" << stats.lsm_segment_cache_misses << ","
        << "\"rollback_savepoint_count\":" << stats.rollback_savepoint_count << ","
        << "\"rollback_partial_ops\":" << stats.rollback_partial_ops << ","
        << "\"pitr_runs\":" << stats.pitr_runs << ","
        << "\"pitr_target_lsn\":" << stats.pitr_target_lsn << ","
        << "\"pitr_elapsed_ms\":" << stats.pitr_elapsed_ms << ","
        << "\"undo_chain_fallback_count\":" << stats.undo_chain_fallback_count << ","
        << "\"lazy_materialize_count\":" << stats.lazy_materialize_count << ","
        << "\"lazy_materialize_rows_total\":" << stats.lazy_materialize_rows_total << ","
        << "\"lazy_materialize_max_rows\":" << stats.lazy_materialize_max_rows << ","
        << "\"lazy_materialize_elapsed_ms\":" << stats.lazy_materialize_elapsed_ms << ","
        << "\"where_query_cache_lookups\":" << s.shell.where_ctx.cache_lookups.load(std::memory_order_relaxed) << ","
        << "\"where_query_cache_hits\":" << s.shell.where_ctx.cache_hits.load(std::memory_order_relaxed) << ","
        << "\"where_policy_checks\":" << s.shell.where_ctx.policy_checks.load(std::memory_order_relaxed) << ","
        << "\"where_policy_rejects\":" << s.shell.where_ctx.policy_rejects.load(std::memory_order_relaxed) << ","
        << "\"where_heap_scan_budget_binding_events\":"
        << s.shell.where_ctx.where_heap_scan_budget_binding_events.load(std::memory_order_relaxed) << ","
        << "\"where_fallback_scans\":" << s.shell.where_ctx.fallback_scans.load(std::memory_order_relaxed) << ","
        << "\"where_plan_eq_sidecar_count\":" << s.shell.where_ctx.plan_eq_sidecar_count.load(std::memory_order_relaxed) << ","
        << "\"where_plan_id_pk_count\":" << s.shell.where_ctx.plan_id_pk_count.load(std::memory_order_relaxed) << ","
        << "\"where_plan_fallback_count\":" << s.shell.where_ctx.plan_fallback_count.load(std::memory_order_relaxed) << ","
        << "\"where_query_count\":" << s.shell.where_ctx.query_count.load(std::memory_order_relaxed) << ","
        << "\"where_query_rows_scanned_total\":" << s.shell.where_ctx.query_rows_scanned_total.load(std::memory_order_relaxed) << ","
        << "\"where_query_rows_returned_total\":" << s.shell.where_ctx.query_rows_returned_total.load(std::memory_order_relaxed) << ","
        << "\"where_eq_sidecar_disk_bytes_read_total\":"
        << s.shell.where_ctx.where_eq_sidecar_disk_bytes_read_total.load(std::memory_order_relaxed) << ","
        << "\"where_eq_sidecar_disk_loads\":" << s.shell.where_ctx.where_eq_sidecar_disk_loads.load(std::memory_order_relaxed) << ","
        << "\"heap_decode_slot_calls\":" << s.shell.session.table.decode_heap_slot_calls << ","
        << "\"heap_decode_slot_hits\":" << s.shell.session.table.decode_heap_slot_hits << ","
        << "\"heap_decode_slot_misses\":" << s.shell.session.table.decode_heap_slot_misses << ","
        << "\"vacuum_priority_score\":" << stats.vacuum_priority_score << ","
        << "\"vacuum_health_bonus_last\":" << stats.vacuum_health_bonus_last << ","
        << "\"vacuum_score_file_bytes_term\":" << stats.vacuum_score_file_bytes_term << ","
        << "\"vacuum_score_health_bonus_term\":" << stats.vacuum_score_health_bonus_term << ","
        << "\"vacuum_score_wal_since_term\":" << stats.vacuum_score_wal_since_term << ","
        << "\"compact_debt_bytes\":" << stats.compact_debt_bytes << ","
        << "\"compact_debt_rows\":" << stats.compact_debt_rows << ","
        << "\"compact_debt_ratio\":" << stats.compact_debt_ratio << ","
        << "\"compact_debt_priority\":" << stats.compact_debt_priority << ","
        << "\"page_cache_hits\":" << stats.page_cache_hits << ","
        << "\"page_cache_misses\":" << stats.page_cache_misses << ","
        << "\"page_cache_evictions\":" << stats.page_cache_evictions << ","
        << "\"page_cache_bytes_in_cache\":" << stats.page_cache_bytes_in_cache << ","
        << "\"memory_budget_max_bytes\":" << stats.memory_budget_max_bytes << ","
        << "\"memory_budget_used_bytes\":" << stats.memory_budget_used_bytes << ","
        << "\"memory_budget_reject_count\":" << stats.memory_budget_reject_count << ","
        << "\"memory_budget_bytes_evicted_total\":" << stats.memory_budget_bytes_evicted_total << ","
        << "\"memory_budget_sidecar_load_skipped_total\":" << stats.memory_budget_sidecar_load_skipped_total << ","
        << "\"txn_snapshot_refresh_count\":" << stats.txn_snapshot_refresh_count << ","
        << "\"txn_snapshot_pinned_count\":" << stats.txn_snapshot_pinned_count << ","
        << "\"txn_readpath_disabled_count\":" << stats.txn_readpath_disabled_count << ","
        << "\"last_snapshot_source\":\"" << json_escape_local(stats.last_snapshot_source) << "\","
        << "\"transaction_snapshot_lsn\":" << stats.transaction_snapshot_lsn << ","
        << "\"statement_snapshot_lsn\":" << stats.statement_snapshot_lsn << ","
        << "\"table_storage_health_logical_rows\":" << stats.table_storage_health_logical_rows << ","
        << "\"table_storage_health_physical_rows\":" << stats.table_storage_health_physical_rows << ","
        << "\"table_storage_health_tombstone_rows\":" << stats.table_storage_health_tombstone_rows << ","
        << "\"table_storage_health_data_file_bytes\":" << stats.table_storage_health_data_file_bytes << ","
        << "\"table_storage_health_live_bytes\":" << stats.table_storage_health_live_bytes << ","
        << "\"table_storage_health_dead_bytes\":" << stats.table_storage_health_dead_bytes << ","
        << "\"table_storage_health_fragmentation_ratio\":" << stats.table_storage_health_fragmentation_ratio << ","
        << "\"table_storage_health_last_vacuum_lsn\":" << stats.table_storage_health_last_vacuum_lsn << ","
        << "\"table_storage_health_last_vacuum_elapsed_ms\":" << stats.table_storage_health_last_vacuum_elapsed_ms
        << ","
        << "\"table_storage_health_tier\":\"" << json_escape_local(stats.table_storage_health_tier) << "\","
        << "\"write_heap_append_p95_ms\":" << stats.write_heap_append_p95_ms << ","
        << "\"write_heap_append_max_ms\":" << stats.write_heap_append_max_ms << ","
        << "\"write_hot_index_p95_ms\":" << stats.write_hot_index_p95_ms << ","
        << "\"write_hot_index_max_ms\":" << stats.write_hot_index_max_ms << ","
        << "\"write_sidecar_invalidate_p95_ms\":" << stats.write_sidecar_invalidate_p95_ms << ","
        << "\"write_sidecar_invalidate_max_ms\":" << stats.write_sidecar_invalidate_max_ms << ","
        << "\"write_wal_append_p95_ms\":" << stats.write_wal_append_p95_ms << ","
        << "\"write_wal_append_max_ms\":" << stats.write_wal_append_max_ms << ","
        << "\"write_lsm_track_p95_ms\":" << stats.write_lsm_track_p95_ms << ","
        << "\"write_lsm_track_max_ms\":" << stats.write_lsm_track_max_ms << ","
        << "\"write_lsm_flush_p95_ms\":" << stats.write_lsm_flush_p95_ms << ","
        << "\"write_lsm_flush_max_ms\":" << stats.write_lsm_flush_max_ms << ","
        << "\"write_lsm_compaction_p95_ms\":" << stats.write_lsm_compaction_p95_ms << ","
        << "\"write_lsm_compaction_max_ms\":" << stats.write_lsm_compaction_max_ms << ","
        << "\"write_lsm_rotate_compact_p95_ms\":" << stats.write_lsm_rotate_compact_p95_ms << ","
        << "\"write_lsm_rotate_compact_max_ms\":" << stats.write_lsm_rotate_compact_max_ms << ","
        << "\"wal_adaptive_enabled\":" << (s.shell.txn.walAdaptiveEnabled() ? "true" : "false") << ","
        << "\"group_commit_window_ms\":" << s.shell.txn.groupCommitWindowMs() << ","
        << "\"group_commit_max_batch_commits\":" << s.shell.txn.groupCommitMaxBatchCommits() << ","
        << "\"vacuum_running\":" << (s.shell.txn.vacuumRunning() ? "true" : "false") << ","
        << "\"vacuum_ops_threshold\":" << s.shell.txn.vacuumOpsThreshold() << ","
        << "\"vacuum_min_interval_sec\":" << s.shell.txn.vacuumMinIntervalSec()
        << "}";
    return oss.str();
}

std::string build_runtime_snapshot_jsonl_line(const CApiSession& s, const std::string& label) {
    const auto now = std::chrono::system_clock::now();
    const auto ts_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    std::ostringstream oss;
    oss << "{"
        << "\"schema_version\":\"newdb.runtime_stats.v1\","
        << "\"ts_ms\":" << ts_ms << ","
        << "\"label\":\"" << json_escape_local(label) << "\","
        << "\"stats\":" << build_runtime_stats_json(s)
        << "}";
    return oss.str();
}

}  // namespace

extern "C" {

const char* newdb_version_string(void) {
    return kNewdbCApiVersionString;
}

int newdb_api_version_major(void) { return NEWDB_C_API_VERSION_MAJOR; }
int newdb_api_version_minor(void) { return NEWDB_C_API_VERSION_MINOR; }
int newdb_api_version_patch(void) { return NEWDB_C_API_VERSION_PATCH; }
int newdb_abi_version(void) { return NEWDB_C_API_ABI_VERSION; }
int newdb_negotiate_abi(int requested_abi) { return requested_abi == NEWDB_C_API_ABI_VERSION ? 1 : 0; }

const char* newdb_error_code_string(int code) {
    switch (code) {
        case NEWDB_OK: return "ok";
        case NEWDB_ERR_INVALID_ARGUMENT: return "invalid_argument";
        case NEWDB_ERR_INVALID_HANDLE: return "invalid_handle";
        case NEWDB_ERR_EXECUTION_FAILED: return "execution_failed";
        case NEWDB_ERR_INTERNAL: return "internal";
        case NEWDB_ERR_LOG_IO: return "log_io";
        case NEWDB_ERR_SESSION_TERMINATED: return "session_terminated";
        default: return "unknown";
    }
}

int newdb_sum(int lhs, int rhs) {
    return lhs + rhs;
}

newdb_schema_check_result newdb_check_schema_file(const char* attr_file_path) {
    if (attr_file_path == nullptr || attr_file_path[0] == '\0') {
        return make_result(0, "attr file path is empty");
    }

    newdb::TableSchema schema{};
    const newdb::Status st = newdb::load_schema_text(attr_file_path, schema);
    if (!st.ok) {
        return make_result(0, st.message);
    }
    return make_result(1, "");
}

newdb_session_handle newdb_session_create(const char* data_dir,
                                          const char* table_name,
                                          const char* log_file_path) {
    auto ptr = std::make_unique<CApiSession>();

    try {
        DemoCliWorkspace ws{};
        ws.data_dir = (data_dir == nullptr) ? "" : data_dir;
        ws.table_name = (table_name == nullptr || table_name[0] == '\0') ? "users" : table_name;
        ws.log_file = (log_file_path == nullptr) ? "" : log_file_path;
        const std::string default_log_name = demo_default_log_spec(ws);
        demo_init_session_logging(ptr->shell, ws, default_log_name, false, false);
        ptr->log_path = ptr->shell.log_file_path;
        (void)apply_table(*ptr, ws.table_name.c_str());
        ptr->last_error = NEWDB_OK;
        return ptr.release();
    } catch (...) {
        return nullptr;
    }
}

void newdb_session_destroy(newdb_session_handle handle) {
    auto* ptr = static_cast<CApiSession*>(handle);
    delete ptr;
}

int newdb_session_set_table(newdb_session_handle handle, const char* table_name) {
    auto* ptr = static_cast<CApiSession*>(handle);
    if (ptr == nullptr) {
        return 0;
    }
    if (table_name == nullptr || table_name[0] == '\0') {
        set_last_error(ptr, NEWDB_ERR_INVALID_ARGUMENT);
        return 0;
    }
    if (!apply_table(*ptr, table_name)) {
        set_last_error(ptr, NEWDB_ERR_EXECUTION_FAILED);
        return 0;
    }
    set_last_error(ptr, NEWDB_OK);
    return 1;
}

int newdb_session_last_error(newdb_session_handle handle) {
    auto* ptr = static_cast<CApiSession*>(handle);
    if (ptr == nullptr) {
        return NEWDB_ERR_INVALID_HANDLE;
    }
    return ptr->last_error;
}

int newdb_session_execute(newdb_session_handle handle,
                          const char* command_line,
                          char* output_buf,
                          size_t output_buf_size) {
    auto* ptr = static_cast<CApiSession*>(handle);
    if (ptr == nullptr || command_line == nullptr || output_buf == nullptr || output_buf_size == 0) {
        set_last_error(ptr, ptr == nullptr ? NEWDB_ERR_INVALID_HANDLE : NEWDB_ERR_INVALID_ARGUMENT);
        return 0;
    }
    std::error_code ec;
    const auto before = std::filesystem::file_size(ptr->log_path, ec);
    const std::uintmax_t start_pos = ec ? 0 : before;
    std::string out;
    int rc = 1;
    try {
        const std::string normalized = normalize_paren_txn_command(command_line);
        const bool keep_going = process_command_line(ptr->shell, normalized.c_str());
        if (!keep_going) {
            out = "[INFO] session terminated by command\n";
            set_last_error(ptr, NEWDB_ERR_SESSION_TERMINATED);
            rc = 0;
        } else {
            const TailReadResult tail = read_file_tail(ptr->log_path, start_pos);
            if (!tail.ok) {
                out = "[ERROR] command output log read failed\n";
                set_last_error(ptr, NEWDB_ERR_LOG_IO);
                rc = 0;
            } else {
                out = tail.data;
                set_last_error(ptr, NEWDB_OK);
                if (output_indicates_business_error(out)) {
                    set_last_error(ptr, NEWDB_ERR_EXECUTION_FAILED);
                    rc = 0;
                }
            }
        }
    } catch (const std::exception& e) {
        out = std::string("[ERROR] command failed: ") + e.what() + "\n";
        set_last_error(ptr, NEWDB_ERR_EXECUTION_FAILED);
        rc = 0;
    } catch (...) {
        out = "[ERROR] command failed: unknown exception\n";
        set_last_error(ptr, NEWDB_ERR_INTERNAL);
        rc = 0;
    }
    if (out.empty()) {
        out = "[INFO] command executed with no log output\n";
    }
    prepend_capi_error_line(out, ptr->last_error);
    const size_t copy_len = (out.size() < output_buf_size - 1) ? out.size() : (output_buf_size - 1);
    std::memcpy(output_buf, out.data(), copy_len);
    output_buf[copy_len] = '\0';
    return rc;
}

int newdb_session_runtime_stats(newdb_session_handle handle,
                                char* output_buf,
                                size_t output_buf_size) {
    auto* ptr = static_cast<CApiSession*>(handle);
    if (ptr == nullptr || output_buf == nullptr || output_buf_size == 0) {
        set_last_error(ptr, ptr == nullptr ? NEWDB_ERR_INVALID_HANDLE : NEWDB_ERR_INVALID_ARGUMENT);
        return 0;
    }
    const std::string out = build_runtime_stats_json(*ptr) + "\n";
    const size_t copy_len = (out.size() < output_buf_size - 1) ? out.size() : (output_buf_size - 1);
    std::memcpy(output_buf, out.data(), copy_len);
    output_buf[copy_len] = '\0';
    set_last_error(ptr, NEWDB_OK);
    return 1;
}

int newdb_session_append_runtime_snapshot(newdb_session_handle handle,
                                          const char* output_jsonl_path,
                                          const char* label) {
    auto* ptr = static_cast<CApiSession*>(handle);
    if (ptr == nullptr) {
        set_last_error(ptr, NEWDB_ERR_INVALID_HANDLE);
        return 0;
    }
    if (output_jsonl_path == nullptr || output_jsonl_path[0] == '\0') {
        set_last_error(ptr, NEWDB_ERR_INVALID_ARGUMENT);
        return 0;
    }
    const std::string label_s = (label == nullptr) ? std::string() : std::string(label);
    const std::string line = build_runtime_snapshot_jsonl_line(*ptr, label_s) + "\n";
    std::ofstream out(output_jsonl_path, std::ios::out | std::ios::app);
    if (!out.good()) {
        set_last_error(ptr, NEWDB_ERR_LOG_IO);
        return 0;
    }
    out << line;
    if (!out.good()) {
        set_last_error(ptr, NEWDB_ERR_LOG_IO);
        return 0;
    }
    set_last_error(ptr, NEWDB_OK);
    return 1;
}

}  // extern "C"
