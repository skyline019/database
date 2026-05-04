#include <waterfall/config.h>

#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <optional>
#include <sstream>
#include <string>

#include "cli/shell/dispatch/shared/dispatch_internal.h"
#include "cli/modules/common/logging/logging.h"
#include "cli/shell/state/shell_state.h"
#include "cli/modules/sidecar/common/sidecar_wal_lsn.h"
#include "cli/modules/txn/coordinator/txn_manager.h"
#include "cli/modules/where/executor/where.h"
#include "cli/modules/common/util/utils.h"
#include "cli/shell/dispatch/services/lsm/lsm_lite_service.h"

namespace {
std::optional<bool> parse_on_off(const std::string& s) {
    if (s == "1" || s == "on" || s == "true" || s == "yes") return true;
    if (s == "0" || s == "off" || s == "false" || s == "no") return false;
    return std::nullopt;
}

std::string runtime_stats_json_for_tuning(ShellState& st) {
    const auto stats = st.txn.runtimeStats();
    const auto isolation = st.txn.txnIsolationLevel();
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
        << "\"wal_recovery_policy\":\"" << stats.wal_recovery_policy << "\","
        << "\"write_conflict_last_sample\":\"" << stats.write_conflict_last_sample << "\","
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
        << "\"lsm_compaction_enqueue_skipped_backpressure\":" << stats.lsm_compaction_enqueue_skipped_backpressure << ","
        << "\"lsm_segment_cache_hits\":" << stats.lsm_segment_cache_hits << ","
        << "\"lsm_segment_cache_misses\":" << stats.lsm_segment_cache_misses << ","
        << "\"lsm_compaction_bytes_amp_efficiency_min_window\":" << stats.lsm_compaction_bytes_amp_efficiency_min_window << ","
        << "\"lsm_read_segments_scanned_p95_window\":" << stats.lsm_read_segments_scanned_p95_window << ","
        << "\"hybrid_mode\":\"" << stats.hybrid_mode << "\","
        << "\"hybrid_mode_switch_count\":" << stats.hybrid_mode_switch_count << ","
        << "\"hybrid_last_switch_reason\":\"" << stats.hybrid_last_switch_reason << "\","
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
        << "\"where_query_cache_lookups\":" << st.where_ctx.cache_lookups.load(std::memory_order_relaxed) << ","
        << "\"where_query_cache_hits\":" << st.where_ctx.cache_hits.load(std::memory_order_relaxed) << ","
        << "\"where_policy_checks\":" << st.where_ctx.policy_checks.load(std::memory_order_relaxed) << ","
        << "\"where_policy_rejects\":" << st.where_ctx.policy_rejects.load(std::memory_order_relaxed) << ","
        << "\"where_heap_scan_budget_binding_events\":"
        << st.where_ctx.where_heap_scan_budget_binding_events.load(std::memory_order_relaxed) << ","
        << "\"where_fallback_scans\":" << st.where_ctx.fallback_scans.load(std::memory_order_relaxed) << ","
        << "\"where_plan_eq_sidecar_count\":" << st.where_ctx.plan_eq_sidecar_count.load(std::memory_order_relaxed) << ","
        << "\"where_plan_id_pk_count\":" << st.where_ctx.plan_id_pk_count.load(std::memory_order_relaxed) << ","
        << "\"where_plan_fallback_count\":" << st.where_ctx.plan_fallback_count.load(std::memory_order_relaxed) << ","
        << "\"where_query_count\":" << st.where_ctx.query_count.load(std::memory_order_relaxed) << ","
        << "\"where_query_rows_scanned_total\":" << st.where_ctx.query_rows_scanned_total.load(std::memory_order_relaxed) << ","
        << "\"where_query_rows_returned_total\":" << st.where_ctx.query_rows_returned_total.load(std::memory_order_relaxed) << ","
        << "\"where_eq_sidecar_disk_bytes_read_total\":"
        << st.where_ctx.where_eq_sidecar_disk_bytes_read_total.load(std::memory_order_relaxed) << ","
        << "\"where_eq_sidecar_disk_loads\":" << st.where_ctx.where_eq_sidecar_disk_loads.load(std::memory_order_relaxed) << ","
        << "\"heap_decode_slot_calls\":" << st.session.table.decode_heap_slot_calls << ","
        << "\"heap_decode_slot_hits\":" << st.session.table.decode_heap_slot_hits << ","
        << "\"heap_decode_slot_misses\":" << st.session.table.decode_heap_slot_misses << ","
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
        << "\"last_snapshot_source\":\"" << stats.last_snapshot_source << "\","
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
        << "\"table_storage_health_tier\":\"" << stats.table_storage_health_tier << "\","
        << "\"wal_adaptive_enabled\":" << (st.txn.walAdaptiveEnabled() ? "true" : "false") << ","
        << "\"group_commit_window_ms\":" << st.txn.groupCommitWindowMs() << ","
        << "\"group_commit_max_batch_commits\":" << st.txn.groupCommitMaxBatchCommits() << ","
        << "\"vacuum_running\":" << (st.txn.vacuumRunning() ? "true" : "false") << ","
        << "\"vacuum_ops_threshold\":" << st.txn.vacuumOpsThreshold() << ","
        << "\"vacuum_min_interval_sec\":" << st.txn.vacuumMinIntervalSec()
        << "}";
    return oss.str();
}
} // namespace

bool handle_txn_commands(ShellState& st, const char* line, const char* log_file, const std::string& current_table) {
    namespace fs = std::filesystem;
    auto parse_single_arg = [&](const char* raw, const char* keyword, const std::size_t keyword_len) -> std::string {
        if (strncasecmp_ascii(raw, keyword, static_cast<int>(keyword_len)) != 0) {
            return {};
        }
        std::string rest = trim(raw + static_cast<std::ptrdiff_t>(keyword_len));
        if (rest.size() >= 2 && rest.front() == '(' && rest.back() == ')') {
            rest = trim(rest.substr(1, rest.size() - 2));
        }
        return rest;
    };
    auto txn_backup_enabled = [&]() {
        const char* env = std::getenv("NEWDB_TXN_SNAPSHOT_BACKUP");
        if (!env) return false;
        std::string v = env;
        for (auto& ch : v) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
        return v == "1" || v == "on" || v == "true" || v == "yes";
    };
    auto txn_backup_path = [&]() { return effective_data_path(st) + ".txn.bak"; };
    // BEGIN [table_name] - ????
    if (strncasecmp_ascii(line, "BEGIN", 5) == 0) {
        std::string table = current_table;
        if (std::strlen(line) > 5) {
            std::string arg = trim(line + 5);
            if (!arg.empty()) table = arg;
        }
        if (st.txn.begin(table)) {
            if (txn_backup_enabled()) {
                // Optional snapshot backup (off by default to reduce write amplification).
                std::error_code ec;
                const std::string data_file = effective_data_path(st);
                const std::string bak_file = txn_backup_path();
                if (!data_file.empty() && fs::exists(data_file, ec)) {
                    fs::copy_file(data_file, bak_file, fs::copy_options::overwrite_existing, ec);
                }
            }
            log_and_print(log_file, "[TXN] transaction started (id=%lld, table=%s)\n",
                          (long long)st.txn.getTxnId(), table.c_str());
        } else {
            log_and_print(log_file, "[TXN] failed to start transaction\n");
        }
        return true;
    }
    // COMMIT - ????
    if (strcasecmp_ascii(line, "COMMIT") == 0) {
        const std::int64_t txn_id = st.txn.getTxnId();
        const std::string data_file = effective_data_path(st);
        if (st.txn.commit()) {
            lsm_lite_on_txn_commit(st, data_file, txn_id);
            std::error_code ec;
            fs::remove(txn_backup_path(), ec);
            log_and_print(log_file, "[TXN] transaction committed\n");
            st.txn.flushWAL();
        } else {
            log_and_print(log_file, "[TXN] no active transaction to commit\n");
        }
        return true;
    }
    // ROLLBACK - ????
    if (strcasecmp_ascii(line, "ROLLBACK") == 0) {
        const std::int64_t txn_id = st.txn.getTxnId();
        const std::string data_file = effective_data_path(st);
        if (st.txn.rollback()) {
            lsm_lite_on_txn_rollback(st, data_file, txn_id);
            std::error_code ec;
            const std::string bak_file = txn_backup_path();
            if (!data_file.empty() && fs::exists(bak_file, ec)) {
                fs::copy_file(bak_file, data_file, fs::copy_options::overwrite_existing, ec);
                fs::remove(bak_file, ec);
            }
            log_and_print(log_file, "[TXN] transaction rolled back\n");
            // Rollback appends compensation records to disk; invalidate cache so
            // subsequent reads (e.g. FIND/PAGE) observe rolled-back state.
            shell_invalidate_session_table(st);
        } else {
            log_and_print(log_file, "[TXN] no active transaction to rollback\n");
        }
        return true;
    }
    {
        const std::string name = parse_single_arg(line, "SAVEPOINT ", 10);
        const bool maybe_savepoint_cmd = (strncasecmp_ascii(line, "SAVEPOINT", 9) == 0);
        if (!name.empty()) {
            if (st.txn.savepoint(name)) {
                log_and_print(log_file, "[TXN] savepoint set: %s\n", name.c_str());
            } else {
                log_and_print(log_file, "[TXN] savepoint failed: %s\n", name.c_str());
            }
            return true;
        }
        if (maybe_savepoint_cmd) {
            const std::string alt = parse_single_arg(line, "SAVEPOINT(", 10);
            if (!alt.empty()) {
                if (st.txn.savepoint(alt)) {
                    log_and_print(log_file, "[TXN] savepoint set: %s\n", alt.c_str());
                } else {
                    log_and_print(log_file, "[TXN] savepoint failed: %s\n", alt.c_str());
                }
            } else {
                log_and_print(log_file, "[TXN] savepoint name required\n");
            }
            return true;
        }
    }
    {
        const std::string name = parse_single_arg(line, "ROLLBACK TO ", 12);
        const bool maybe_rb_to_cmd = (strncasecmp_ascii(line, "ROLLBACK TO", 11) == 0);
        if (!name.empty()) {
            if (st.txn.rollbackToSavepoint(name)) {
                log_and_print(log_file, "[TXN] rolled back to savepoint: %s\n", name.c_str());
                shell_invalidate_session_table(st);
            } else {
                log_and_print(log_file, "[TXN] rollback to savepoint failed: %s\n", name.c_str());
            }
            return true;
        }
        if (maybe_rb_to_cmd) {
            const std::string alt = parse_single_arg(line, "ROLLBACK TO(", 12);
            if (!alt.empty()) {
                if (st.txn.rollbackToSavepoint(alt)) {
                    log_and_print(log_file, "[TXN] rolled back to savepoint: %s\n", alt.c_str());
                    shell_invalidate_session_table(st);
                } else {
                    log_and_print(log_file, "[TXN] rollback to savepoint failed: %s\n", alt.c_str());
                }
            } else {
                log_and_print(log_file, "[TXN] rollback target savepoint required\n");
            }
            return true;
        }
    }
    {
        const std::string name = parse_single_arg(line, "RELEASE SAVEPOINT ", 18);
        const bool maybe_rel_sp_cmd = (strncasecmp_ascii(line, "RELEASE SAVEPOINT", 17) == 0);
        if (!name.empty()) {
            if (st.txn.releaseSavepoint(name)) {
                log_and_print(log_file, "[TXN] savepoint released: %s\n", name.c_str());
            } else {
                log_and_print(log_file, "[TXN] release savepoint failed: %s\n", name.c_str());
            }
            return true;
        }
        if (maybe_rel_sp_cmd) {
            const std::string alt = parse_single_arg(line, "RELEASE SAVEPOINT(", 18);
            if (!alt.empty()) {
                if (st.txn.releaseSavepoint(alt)) {
                    log_and_print(log_file, "[TXN] savepoint released: %s\n", alt.c_str());
                } else {
                    log_and_print(log_file, "[TXN] release savepoint failed: %s\n", alt.c_str());
                }
            } else {
                log_and_print(log_file, "[TXN] release savepoint name required\n");
            }
            return true;
        }
    }
    if (strncasecmp_ascii(line, "RECOVER TO LSN ", 15) == 0) {
        const std::string arg = trim(line + 15);
        std::uint64_t target = 0;
        try { target = static_cast<std::uint64_t>(std::stoull(arg)); } catch (...) { target = 0; }
        if (target == 0) {
            log_and_print(log_file, "[TXN] invalid RECOVER TO LSN target\n");
            return true;
        }
        if (st.txn.recoverToLsn(target)) {
            log_and_print(log_file, "[TXN] recover to lsn success: %llu\n", static_cast<unsigned long long>(target));
        } else {
            log_and_print(log_file, "[TXN] recover to lsn failed: %llu\n", static_cast<unsigned long long>(target));
        }
        return true;
    }
    if (strncasecmp_ascii(line, "RECOVER TO TIME ", 16) == 0) {
        const std::string arg = trim(line + 16);
        std::uint64_t target = 0;
        try { target = static_cast<std::uint64_t>(std::stoull(arg)); } catch (...) { target = 0; }
        if (target == 0) {
            log_and_print(log_file, "[TXN] invalid RECOVER TO TIME target\n");
            return true;
        }
        if (st.txn.recoverToTime(target)) {
            log_and_print(log_file, "[TXN] recover to time success: %llu\n", static_cast<unsigned long long>(target));
        } else {
            log_and_print(log_file, "[TXN] recover to time failed: %llu\n", static_cast<unsigned long long>(target));
        }
        return true;
    }
    // SHOW TUNING - ?????????
    if (strcasecmp_ascii(line, "SHOW TUNING") == 0 || strcasecmp_ascii(line, "SHOW STATUS") == 0) {
        const TxnRuntimeStats stats = st.txn.runtimeStats();
        const auto mode = st.txn.walSyncMode();
        const char* mode_s = (mode == newdb::WalSyncMode::Off) ? "off"
                            : (mode == newdb::WalSyncMode::Normal) ? "normal"
                                                                   : "full";
        log_and_print(log_file,
                      "[TUNING] WALSYNC=%s normal_interval_ms=%llu AUTOVACUUM=%s ops_threshold=%zu min_interval_sec=%zu trigger_count=%llu execute_count=%llu cooldown_skips=%llu write_conflicts=%llu begin_lock_conflicts=%llu wal_compacts=%llu\n",
                      mode_s,
                      static_cast<unsigned long long>(st.txn.walNormalSyncIntervalMs()),
                      st.txn.vacuumRunning() ? "on" : "off",
                      st.txn.vacuumOpsThreshold(),
                      st.txn.vacuumMinIntervalSec(),
                      static_cast<unsigned long long>(stats.vacuum_trigger_count),
                      static_cast<unsigned long long>(stats.vacuum_execute_count),
                      static_cast<unsigned long long>(stats.vacuum_cooldown_skip_count),
                      static_cast<unsigned long long>(stats.write_conflict_count),
                      static_cast<unsigned long long>(stats.txn_begin_lock_conflict_count),
                      static_cast<unsigned long long>(stats.wal_compact_count));
        return true;
    }
    if (strcasecmp_ascii(line, "SHOW TUNING JSON") == 0 || strcasecmp_ascii(line, "SHOW STATUS JSON") == 0) {
        const std::string json = runtime_stats_json_for_tuning(st);
        log_and_print(log_file, "%s\n", json.c_str());
        return true;
    }
    if (strcasecmp_ascii(line, "SHOW STORAGE") == 0) {
        std::error_code ec;
        const fs::path ws = workspace_directory(st);
        const fs::path wal = ws / "demodb.wal";
        std::uint64_t wal_bytes{0};
        if (fs::exists(wal, ec)) {
            const auto sz = fs::file_size(wal, ec);
            if (!ec) {
                wal_bytes = static_cast<std::uint64_t>(sz);
            }
        }
        const std::uint64_t lsn = read_wal_lsn_for_workspace(ws.string());
        std::uint64_t bin_bytes{0};
        std::uint32_t bin_files{0};
        if (fs::is_directory(ws, ec)) {
            for (const auto& ent : fs::directory_iterator(ws, ec)) {
                if (ec) break;
                if (!ent.is_regular_file(ec)) continue;
                if (ent.path().extension() == ".bin") {
                    const auto fsz = fs::file_size(ent.path(), ec);
                    if (!ec) {
                        bin_bytes += static_cast<std::uint64_t>(fsz);
                    }
                    ++bin_files;
                }
            }
        }
        log_and_print(log_file,
                      "[STORAGE] workspace=%s demodb.wal bytes=%llu demodb.wal_lsn=%llu total *.bin files=%u bytes=%llu\n",
                      ws.string().c_str(),
                      static_cast<unsigned long long>(wal_bytes),
                      static_cast<unsigned long long>(lsn),
                      static_cast<unsigned int>(bin_files),
                      static_cast<unsigned long long>(bin_bytes));
        return true;
    }
    // AUTOVACUUM - ???? VACUUM
    if (strncasecmp_ascii(line, "AUTOVACUUM", 10) == 0) {
        if (std::strlen(line) > 10) {
            std::string arg = trim(line + 10);
            std::istringstream iss(arg);
            std::string mode;
            std::string threshold_token;
            iss >> mode >> threshold_token;
            if (mode == "1" || mode == "on") {
                if (!threshold_token.empty()) {
                    try {
                        st.txn.setVacuumOpsThreshold(static_cast<std::size_t>(std::stoull(threshold_token)));
                    } catch (...) {
                        log_and_print(log_file, "[VACUUM] invalid threshold: %s\n", threshold_token.c_str());
                        return true;
                    }
                }
                st.txn.startVacuumThread();
                log_and_print(log_file, "[VACUUM] auto vacuum enabled (ops_threshold=%zu)\n",
                              st.txn.vacuumOpsThreshold());
            } else if (mode == "0" || mode == "off") {
                st.txn.stopVacuumThread();
                log_and_print(log_file, "[VACUUM] auto vacuum disabled\n");
            } else if (mode == "threshold") {
                if (threshold_token.empty()) {
                    log_and_print(log_file, "[VACUUM] usage: AUTOVACUUM threshold <ops>\n");
                    return true;
                }
                try {
                    st.txn.setVacuumOpsThreshold(static_cast<std::size_t>(std::stoull(threshold_token)));
                    log_and_print(log_file, "[VACUUM] ops threshold set to %zu\n", st.txn.vacuumOpsThreshold());
                } catch (...) {
                    log_and_print(log_file, "[VACUUM] invalid threshold: %s\n", threshold_token.c_str());
                }
            } else if (mode == "interval") {
                if (threshold_token.empty()) {
                    log_and_print(log_file, "[VACUUM] usage: AUTOVACUUM interval <sec>\n");
                    return true;
                }
                try {
                    st.txn.setVacuumMinIntervalSec(static_cast<std::size_t>(std::stoull(threshold_token)));
                    log_and_print(log_file, "[VACUUM] min interval set to %zu sec\n", st.txn.vacuumMinIntervalSec());
                } catch (...) {
                    log_and_print(log_file, "[VACUUM] invalid interval: %s\n", threshold_token.c_str());
                }
            } else {
                log_and_print(log_file,
                              "[VACUUM] usage: AUTOVACUUM [0|1|on|off] [ops_threshold] | AUTOVACUUM threshold <ops> | AUTOVACUUM interval <sec>\n");
            }
        } else {
            log_and_print(log_file, "[VACUUM] auto=%s ops_threshold=%zu min_interval_sec=%zu\n",
                          st.txn.vacuumRunning() ? "on" : "off",
                          st.txn.vacuumOpsThreshold(),
                          st.txn.vacuumMinIntervalSec());
        }
        return true;
    }
    if (strncasecmp_ascii(line, "WALSYNC", 7) == 0) {
        std::string arg = trim(line + 7);
        if (arg.empty()) {
            const auto mode = st.txn.walSyncMode();
            const char* mode_s = (mode == newdb::WalSyncMode::Off) ? "off"
                                : (mode == newdb::WalSyncMode::Normal) ? "normal"
                                                                       : "full";
            log_and_print(log_file, "[WAL] sync mode=%s normal_interval_ms=%llu\n",
                          mode_s,
                          static_cast<unsigned long long>(st.txn.walNormalSyncIntervalMs()));
            return true;
        }
        std::istringstream iss(arg);
        std::string lower;
        std::string interval_token;
        iss >> lower >> interval_token;
        for (auto& ch : lower) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
        if (lower == "off") {
            st.txn.setWalSyncMode(newdb::WalSyncMode::Off);
        } else if (lower == "normal") {
            st.txn.setWalSyncMode(newdb::WalSyncMode::Normal);
            if (!interval_token.empty()) {
                try {
                    st.txn.setWalNormalSyncIntervalMs(static_cast<std::uint64_t>(std::stoull(interval_token)));
                } catch (...) {
                    log_and_print(log_file, "[WAL] invalid normal interval: %s\n", interval_token.c_str());
                    return true;
                }
            }
        } else if (lower == "full") {
            st.txn.setWalSyncMode(newdb::WalSyncMode::Full);
        } else {
            log_and_print(log_file, "[WAL] usage: WALSYNC [full|normal [interval_ms]|off]\n");
            return true;
        }
        log_and_print(log_file, "[WAL] sync mode set to %s (normal_interval_ms=%llu)\n",
                      lower.c_str(),
                      static_cast<unsigned long long>(st.txn.walNormalSyncIntervalMs()));
        return true;
    }
    if (strncasecmp_ascii(line, "TXNISOLATION", 12) == 0) {
        std::string arg = trim(line + 12);
        for (auto& ch : arg) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
        if (arg.empty()) {
            const auto level = st.txn.txnIsolationLevel();
            log_and_print(log_file, "[TXN] isolation=%s\n",
                          level == TxnIsolationLevel::ReadCommitted ? "read_committed" : "snapshot");
            return true;
        }
        if (arg == "read_committed" || arg == "rc") {
            st.txn.setTxnIsolationLevel(TxnIsolationLevel::ReadCommitted);
            log_and_print(log_file, "[TXN] isolation set to read_committed\n");
            return true;
        }
        if (arg == "snapshot" || arg == "si") {
            st.txn.setTxnIsolationLevel(TxnIsolationLevel::Snapshot);
            log_and_print(log_file, "[TXN] isolation set to snapshot\n");
            return true;
        }
        log_and_print(log_file, "[TXN] usage: TXNISOLATION [snapshot|read_committed]\n");
        return true;
    }
    if (strncasecmp_ascii(line, "WRITECONFLICT", 13) == 0) {
        std::string arg = trim(line + 13);
        std::istringstream iss(arg);
        std::string mode;
        std::string timeout_token;
        iss >> mode >> timeout_token;
        for (auto& ch : mode) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
        if (mode.empty()) {
            const auto p = st.txn.writeConflictPolicy();
            log_and_print(log_file, "[TXN] write_conflict=%s timeout_ms=%llu\n",
                          p == WriteConflictPolicy::Wait ? "wait" : "reject",
                          static_cast<unsigned long long>(st.txn.writeConflictWaitTimeoutMs()));
            return true;
        }
        if (mode == "reject") {
            st.txn.setWriteConflictPolicy(WriteConflictPolicy::Reject);
            log_and_print(log_file, "[TXN] write_conflict policy set to reject\n");
            return true;
        }
        if (mode == "wait") {
            st.txn.setWriteConflictPolicy(WriteConflictPolicy::Wait);
            if (!timeout_token.empty()) {
                try {
                    st.txn.setWriteConflictWaitTimeoutMs(static_cast<std::uint64_t>(std::stoull(timeout_token)));
                } catch (...) {
                    log_and_print(log_file, "[TXN] invalid wait timeout: %s\n", timeout_token.c_str());
                    return true;
                }
            }
            log_and_print(log_file, "[TXN] write_conflict policy set to wait (timeout_ms=%llu)\n",
                          static_cast<unsigned long long>(st.txn.writeConflictWaitTimeoutMs()));
            return true;
        }
        log_and_print(log_file, "[TXN] usage: WRITECONFLICT [reject|wait [timeout_ms]]\n");
        return true;
    }
    if (strncasecmp_ascii(line, "HOTINDEX", 8) == 0) {
        std::string arg = trim(line + 8);
        for (auto& ch : arg) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
        if (arg.empty()) {
            log_and_print(log_file, "[LSM] hotindex=%s\n", st.txn.hotIndexEnabled() ? "on" : "off");
            return true;
        }
        const auto on = parse_on_off(arg);
        if (!on.has_value()) {
            log_and_print(log_file, "[LSM] usage: HOTINDEX [on|off]\n");
            return true;
        }
        st.txn.setHotIndexEnabled(*on);
        log_and_print(log_file, "[LSM] hotindex set to %s\n", *on ? "on" : "off");
        return true;
    }
    if (strncasecmp_ascii(line, "SEGMENT", 7) == 0) {
        std::string arg = trim(line + 7);
        if (arg.empty()) {
            log_and_print(log_file, "[LSM] segment_target_bytes=%llu\n",
                          static_cast<unsigned long long>(st.txn.segmentTargetBytes()));
            return true;
        }
        try {
            st.txn.setSegmentTargetBytes(static_cast<std::uint64_t>(std::stoull(arg)));
            log_and_print(log_file, "[LSM] segment_target_bytes set to %llu\n",
                          static_cast<unsigned long long>(st.txn.segmentTargetBytes()));
        } catch (...) {
            log_and_print(log_file, "[LSM] invalid segment target bytes: %s\n", arg.c_str());
        }
        return true;
    }
    if (strncasecmp_ascii(line, "WALADAPTIVE", 11) == 0) {
        std::string arg = trim(line + 11);
        for (auto& ch : arg) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
        if (arg.empty()) {
            log_and_print(log_file, "[WAL] adaptive=%s\n", st.txn.walAdaptiveEnabled() ? "on" : "off");
            return true;
        }
        const auto on = parse_on_off(arg);
        if (!on.has_value()) {
            log_and_print(log_file, "[WAL] usage: WALADAPTIVE [on|off]\n");
            return true;
        }
        st.txn.setWalAdaptiveEnabled(*on);
        log_and_print(log_file, "[WAL] adaptive set to %s\n", *on ? "on" : "off");
        return true;
    }
    if (strncasecmp_ascii(line, "GROUPCOMMIT", 11) == 0) {
        std::string arg = trim(line + 11);
        std::istringstream iss(arg);
        std::string window_ms_token;
        std::string max_batch_token;
        iss >> window_ms_token >> max_batch_token;
        if (window_ms_token.empty()) {
            log_and_print(log_file, "[WAL] group_commit window_ms=%llu max_batch=%llu\n",
                          static_cast<unsigned long long>(st.txn.groupCommitWindowMs()),
                          static_cast<unsigned long long>(st.txn.groupCommitMaxBatchCommits()));
            return true;
        }
        try {
            st.txn.setGroupCommitWindowMs(static_cast<std::uint64_t>(std::stoull(window_ms_token)));
            if (!max_batch_token.empty()) {
                st.txn.setGroupCommitMaxBatchCommits(static_cast<std::uint64_t>(std::stoull(max_batch_token)));
            }
            log_and_print(log_file, "[WAL] group_commit set window_ms=%llu max_batch=%llu\n",
                          static_cast<unsigned long long>(st.txn.groupCommitWindowMs()),
                          static_cast<unsigned long long>(st.txn.groupCommitMaxBatchCommits()));
        } catch (...) {
            log_and_print(log_file, "[WAL] usage: GROUPCOMMIT <window_ms> [max_batch]\n");
        }
        return true;
    }
    return false;
}




