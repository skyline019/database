#include <cstdlib>
#include <algorithm>
#include <cstdint>
#include <cmath>
#include <fstream>
#include <iostream>
#include <numeric>
#include <string>
#include <vector>

namespace {

struct Args {
    std::string input_jsonl;
    std::string output_json;
    std::string label_prefix;
    std::string run_id;
    double min_vacuum_efficiency{-1.0};
    double max_conflict_rate{-1.0};
    double min_vacuum_efficiency_p50{-1.0};
    double max_conflict_rate_p95{-1.0};
    double max_txn_begin_lock_conflict_delta{-1.0};
    double max_wal_compact_delta{-1.0};
    double max_vacuum_compact_failure_delta{-1.0};
    double min_vacuum_compact_reclaimed_bytes_delta{-1.0};
    double max_vacuum_queue_depth_peak{-1.0};
    double max_wal_recovery_last_elapsed_ms{-1.0};
    double max_lock_deadlock_detect_delta{-1.0};
    double max_lock_deadlock_victim_delta{-1.0};
    double max_lock_wait_max_ms_delta{-1.0};
    double max_scheduler_throttle_delta{-1.0};
    double min_wal_group_commit_batch_commits_delta{-1.0};
    double max_lsm_segment_count{-1.0};
    double min_lsm_memtable_flush_delta{-1.0};
    double max_lsm_read_segments_scanned_p95{-1.0};
    double min_lsm_compaction_bytes_amp_efficiency{-1.0};
    double max_lazy_materialize_count_delta{-1.0};
    double max_lazy_materialize_rows_total_delta{-1.0};
    /// When >= 0, fail if any snapshot row with a non-zero health sample exceeds this fragmentation ratio.
    double max_table_storage_health_fragmentation_ratio{-1.0};
    /// When >= 0, fail if any health sample row exceeds this dead-bytes estimate (compact-debt proxy; governance doc).
    double max_table_storage_health_dead_bytes{-1.0};
    /// When >= 0, fail if max observed vacuum_health_bonus_last across snapshots exceeds this (health-weighted debt bonus).
    double max_vacuum_health_bonus_last{-1.0};
    /// When >= 0, fail if any snapshot exceeds this `compact_debt_bytes` (vacuum enqueue debt).
    double max_compact_debt_bytes_peak{-1.0};
    /// When >= 0, fail if page-cache lookups exist and hit ratio (last snapshot) is below threshold.
    double min_page_cache_hit_ratio{-1.0};
    /// When >= 0, fail if scan amplification exceeds threshold (window delta).
    double max_where_scan_amplification{-1.0};
    /// When >= 0, fail if memory_budget_reject_count delta exceeds threshold.
    double max_memory_budget_reject_delta{-1.0};
    int last_n{0};
    /// When true, stdout emits only the single-line summary JSON (gates still write to stderr on failure).
    bool json_only_stdout{false};
};

struct Row {
    std::string label;
    std::string run_id;
    std::uint64_t trigger_count{0};
    std::uint64_t execute_count{0};
    std::uint64_t cooldown_skips{0};
    std::uint64_t compact_success_count{0};
    std::uint64_t compact_failure_count{0};
    std::uint64_t compact_bytes_reclaimed{0};
    std::uint64_t compact_last_elapsed_ms{0};
    std::uint64_t vacuum_queue_depth{0};
    std::uint64_t vacuum_queue_depth_peak{0};
    std::uint64_t write_conflicts{0};
    std::uint64_t lock_wait_ms_total{0};
    std::uint64_t lock_wait_max_ms{0};
    std::uint64_t lock_deadlock_detect_count{0};
    std::uint64_t lock_deadlock_victim_count{0};
    std::uint64_t txn_begin_lock_conflicts{0};
    std::uint64_t wal_compact_count{0};
    std::uint64_t wal_recovery_runs{0};
    std::uint64_t wal_recovery_undo_ops{0};
    std::uint64_t wal_recovery_last_elapsed_ms{0};
    std::uint64_t wal_recovery_analyze_ms{0};
    std::uint64_t wal_recovery_undo_ms{0};
    std::uint64_t wal_recovery_finalize_ms{0};
    std::uint64_t wal_recovery_records_scanned{0};
    std::uint64_t wal_recovery_dangling_txns{0};
    std::uint64_t wal_group_commit_count{0};
    std::uint64_t wal_group_commit_batch_commits{0};
    std::uint64_t scheduler_throttle_count{0};
    std::uint64_t lsm_memtable_flush_count{0};
    std::uint64_t lsm_compaction_count{0};
    std::uint64_t lsm_segment_count{0};
    std::uint64_t lsm_memtable_bytes{0};
    std::uint64_t lsm_read_segments_scanned{0};
    std::uint64_t lsm_read_segments_scanned_p95{0};
    std::uint64_t lsm_compaction_bytes_in{0};
    std::uint64_t lsm_compaction_bytes_out{0};
    std::uint64_t lazy_materialize_count{0};
    std::uint64_t lazy_materialize_rows_total{0};
    std::uint64_t lazy_materialize_max_rows{0};
    std::uint64_t lazy_materialize_elapsed_ms{0};
    std::uint64_t table_storage_health_logical_rows{0};
    std::uint64_t table_storage_health_dead_bytes{0};
    double table_storage_health_fragmentation_ratio{0.0};
    std::uint64_t vacuum_health_bonus_last{0};
    std::uint64_t compact_debt_bytes{0};
    std::uint64_t page_cache_hits{0};
    std::uint64_t page_cache_misses{0};
    std::uint64_t where_query_rows_scanned_total{0};
    std::uint64_t where_query_rows_returned_total{0};
    std::uint64_t wal_recovery_redo_ms{0};
    std::uint64_t memory_budget_reject_count{0};
    bool table_storage_health_sample_present{false};
    bool ok{false};
};

struct DistStats {
    double min{0.0};
    double max{0.0};
    double avg{0.0};
    double p50{0.0};
    double p95{0.0};
};

void print_usage() {
    std::cout
        << "newdb_runtime_report\n"
        << "  --input <runtime_stats.jsonl> [--output <summary.json>]\n"
        << "  [--run-id <id>|latest]\n"
        << "  [--label-prefix <prefix>] [--last-n <N>=2 for before/after]\n"
        << "  [--min-vacuum-efficiency <0..1>] [--max-conflict-rate <0..1>]\n"
        << "  [--min-vacuum-efficiency-p50 <0..1>] [--max-conflict-rate-p95 <0..1>]\n"
        << "  [--max-txn-begin-lock-conflict-delta <>=0] [--max-wal-compact-delta <>=0]\n"
        << "  [--max-vacuum-compact-failure-delta <>=0] [--min-vacuum-compact-reclaimed-bytes-delta <>=0]\n"
        << "  [--max-vacuum-queue-depth-peak <>=0] [--max-wal-recovery-last-elapsed-ms <>=0]\n"
        << "  [--max-lock-deadlock-detect-delta <>=0] [--max-lock-deadlock-victim-delta <>=0]\n"
        << "  [--max-lock-wait-max-ms-delta <>=0] [--max-scheduler-throttle-delta <>=0]\n"
        << "  [--min-wal-group-commit-batch-commits-delta <>=0]\n"
        << "  [--max-lsm-segment-count <>=0] [--min-lsm-memtable-flush-delta <>=0]\n"
        << "  [--max-lsm-read-segments-scanned-p95 <>=0] [--min-lsm-compaction-bytes-amp-efficiency <>=0]\n"
        << "  [--max-lazy-materialize-count-delta <>=0] [--max-lazy-materialize-rows-total-delta <>=0]\n"
        << "  [--max-table-storage-health-fragmentation-ratio <>=0]\n"
        << "  [--max-table-storage-health-dead-bytes <>=0]\n"
        << "  [--max-vacuum-health-bonus-last <>=0]\n"
        << "  [--max-compact-debt-bytes-peak <>=0]\n"
        << "  [--min-page-cache-hit-ratio <0..1>] [--max-where-scan-amplification <>=0]\n"
        << "  [--max-memory-budget-reject-delta <>=0]\n"
        << "  [--json] (emit only the summary JSON line on stdout)\n";
}

bool parse_double(const char* s, double& out) {
    try {
        out = std::stod(s);
        return true;
    } catch (...) {
        return false;
    }
}

bool parse_args(int argc, char** argv, Args& out) {
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        auto next = [&]() -> const char* {
            if (i + 1 >= argc) return nullptr;
            return argv[++i];
        };
        if (arg == "--help" || arg == "-h") {
            print_usage();
            return false;
        }
        if (arg == "--json") {
            out.json_only_stdout = true;
            continue;
        }
        if (arg == "--input") {
            const char* v = next();
            if (!v) return false;
            out.input_jsonl = v;
            continue;
        }
        if (arg == "--output") {
            const char* v = next();
            if (!v) return false;
            out.output_json = v;
            continue;
        }
        if (arg == "--min-vacuum-efficiency") {
            const char* v = next();
            if (!v || !parse_double(v, out.min_vacuum_efficiency)) return false;
            continue;
        }
        if (arg == "--max-conflict-rate") {
            const char* v = next();
            if (!v || !parse_double(v, out.max_conflict_rate)) return false;
            continue;
        }
        if (arg == "--min-vacuum-efficiency-p50") {
            const char* v = next();
            if (!v || !parse_double(v, out.min_vacuum_efficiency_p50)) return false;
            continue;
        }
        if (arg == "--max-conflict-rate-p95") {
            const char* v = next();
            if (!v || !parse_double(v, out.max_conflict_rate_p95)) return false;
            continue;
        }
        if (arg == "--max-txn-begin-lock-conflict-delta") {
            const char* v = next();
            if (!v || !parse_double(v, out.max_txn_begin_lock_conflict_delta)) return false;
            continue;
        }
        if (arg == "--max-wal-compact-delta") {
            const char* v = next();
            if (!v || !parse_double(v, out.max_wal_compact_delta)) return false;
            continue;
        }
        if (arg == "--max-vacuum-compact-failure-delta") {
            const char* v = next();
            if (!v || !parse_double(v, out.max_vacuum_compact_failure_delta)) return false;
            continue;
        }
        if (arg == "--min-vacuum-compact-reclaimed-bytes-delta") {
            const char* v = next();
            if (!v || !parse_double(v, out.min_vacuum_compact_reclaimed_bytes_delta)) return false;
            continue;
        }
        if (arg == "--max-vacuum-queue-depth-peak") {
            const char* v = next();
            if (!v || !parse_double(v, out.max_vacuum_queue_depth_peak)) return false;
            continue;
        }
        if (arg == "--max-wal-recovery-last-elapsed-ms") {
            const char* v = next();
            if (!v || !parse_double(v, out.max_wal_recovery_last_elapsed_ms)) return false;
            continue;
        }
        if (arg == "--max-lock-deadlock-detect-delta") {
            const char* v = next();
            if (!v || !parse_double(v, out.max_lock_deadlock_detect_delta)) return false;
            continue;
        }
        if (arg == "--max-lock-deadlock-victim-delta") {
            const char* v = next();
            if (!v || !parse_double(v, out.max_lock_deadlock_victim_delta)) return false;
            continue;
        }
        if (arg == "--max-lock-wait-max-ms-delta") {
            const char* v = next();
            if (!v || !parse_double(v, out.max_lock_wait_max_ms_delta)) return false;
            continue;
        }
        if (arg == "--max-scheduler-throttle-delta") {
            const char* v = next();
            if (!v || !parse_double(v, out.max_scheduler_throttle_delta)) return false;
            continue;
        }
        if (arg == "--min-wal-group-commit-batch-commits-delta") {
            const char* v = next();
            if (!v || !parse_double(v, out.min_wal_group_commit_batch_commits_delta)) return false;
            continue;
        }
        if (arg == "--max-lsm-segment-count") {
            const char* v = next();
            if (!v || !parse_double(v, out.max_lsm_segment_count)) return false;
            continue;
        }
        if (arg == "--min-lsm-memtable-flush-delta") {
            const char* v = next();
            if (!v || !parse_double(v, out.min_lsm_memtable_flush_delta)) return false;
            continue;
        }
        if (arg == "--max-lsm-read-segments-scanned-p95") {
            const char* v = next();
            if (!v || !parse_double(v, out.max_lsm_read_segments_scanned_p95)) return false;
            continue;
        }
        if (arg == "--min-lsm-compaction-bytes-amp-efficiency") {
            const char* v = next();
            if (!v || !parse_double(v, out.min_lsm_compaction_bytes_amp_efficiency)) return false;
            continue;
        }
        if (arg == "--max-lazy-materialize-count-delta") {
            const char* v = next();
            if (!v || !parse_double(v, out.max_lazy_materialize_count_delta)) return false;
            continue;
        }
        if (arg == "--max-lazy-materialize-rows-total-delta") {
            const char* v = next();
            if (!v || !parse_double(v, out.max_lazy_materialize_rows_total_delta)) return false;
            continue;
        }
        if (arg == "--max-table-storage-health-fragmentation-ratio") {
            const char* v = next();
            if (!v || !parse_double(v, out.max_table_storage_health_fragmentation_ratio)) return false;
            continue;
        }
        if (arg == "--max-table-storage-health-dead-bytes") {
            const char* v = next();
            if (!v || !parse_double(v, out.max_table_storage_health_dead_bytes)) return false;
            continue;
        }
        if (arg == "--max-vacuum-health-bonus-last") {
            const char* v = next();
            if (!v || !parse_double(v, out.max_vacuum_health_bonus_last)) return false;
            continue;
        }
        if (arg == "--max-compact-debt-bytes-peak") {
            const char* v = next();
            if (!v || !parse_double(v, out.max_compact_debt_bytes_peak)) return false;
            continue;
        }
        if (arg == "--min-page-cache-hit-ratio") {
            const char* v = next();
            if (!v || !parse_double(v, out.min_page_cache_hit_ratio)) return false;
            continue;
        }
        if (arg == "--max-where-scan-amplification") {
            const char* v = next();
            if (!v || !parse_double(v, out.max_where_scan_amplification)) return false;
            continue;
        }
        if (arg == "--max-memory-budget-reject-delta") {
            const char* v = next();
            if (!v || !parse_double(v, out.max_memory_budget_reject_delta)) return false;
            continue;
        }
        if (arg == "--label-prefix") {
            const char* v = next();
            if (!v) return false;
            out.label_prefix = v;
            continue;
        }
        if (arg == "--run-id") {
            const char* v = next();
            if (!v) return false;
            out.run_id = v;
            continue;
        }
        if (arg == "--last-n") {
            const char* v = next();
            if (!v) return false;
            try {
                out.last_n = std::stoi(v);
            } catch (...) {
                return false;
            }
            if (out.last_n < 2) return false;
            continue;
        }
        std::cerr << "unknown argument: " << arg << "\n";
        return false;
    }
    return !out.input_jsonl.empty();
}

bool extract_u64_field(const std::string& line, const std::string& key, std::uint64_t& out) {
    const std::string needle = "\"" + key + "\":";
    const std::size_t pos = line.find(needle);
    if (pos == std::string::npos) return false;
    std::size_t i = pos + needle.size();
    while (i < line.size() && (line[i] == ' ' || line[i] == '\t')) ++i;
    if (i >= line.size()) return false;
    std::size_t j = i;
    while (j < line.size() && line[j] >= '0' && line[j] <= '9') ++j;
    if (j == i) return false;
    try {
        out = static_cast<std::uint64_t>(std::stoull(line.substr(i, j - i)));
        return true;
    } catch (...) {
        return false;
    }
}

bool extract_u64_field_optional(const std::string& line, const std::string& key, std::uint64_t& out) {
    std::uint64_t v = 0;
    if (!extract_u64_field(line, key, v)) {
        return true;
    }
    out = v;
    return true;
}

bool extract_double_field(const std::string& line, const std::string& key, double& out) {
    const std::string needle = "\"" + key + "\":";
    const std::size_t pos = line.find(needle);
    if (pos == std::string::npos) {
        return false;
    }
    std::size_t i = pos + needle.size();
    while (i < line.size() && (line[i] == ' ' || line[i] == '\t')) {
        ++i;
    }
    std::size_t j = i;
    if (j < line.size() && line[j] == '-') {
        ++j;
    }
    while (j < line.size() && line[j] >= '0' && line[j] <= '9') {
        ++j;
    }
    if (j < line.size() && line[j] == '.') {
        ++j;
        while (j < line.size() && line[j] >= '0' && line[j] <= '9') {
            ++j;
        }
    }
    if (j == i) {
        return false;
    }
    try {
        out = std::stod(line.substr(i, j - i));
        return true;
    } catch (...) {
        return false;
    }
}

bool extract_string_field(const std::string& line, const std::string& key, std::string& out) {
    const std::string needle = "\"" + key + "\":\"";
    const std::size_t pos = line.find(needle);
    if (pos == std::string::npos) return false;
    std::size_t i = pos + needle.size();
    const std::size_t j = line.find('"', i);
    if (j == std::string::npos) return false;
    out = line.substr(i, j - i);
    return true;
}

Row parse_row(const std::string& line) {
    Row r{};
    bool ok = true;
    std::string label;
    std::string run_id;
    if (extract_string_field(line, "label", label)) r.label = label;
    if (extract_string_field(line, "run_id", run_id)) r.run_id = run_id;
    ok = ok && extract_u64_field(line, "vacuum_trigger_count", r.trigger_count);
    ok = ok && extract_u64_field(line, "vacuum_execute_count", r.execute_count);
    ok = ok && extract_u64_field(line, "vacuum_cooldown_skip_count", r.cooldown_skips);
    ok = ok && extract_u64_field_optional(line, "vacuum_compact_success_count", r.compact_success_count);
    ok = ok && extract_u64_field_optional(line, "vacuum_compact_failure_count", r.compact_failure_count);
    ok = ok && extract_u64_field_optional(line, "vacuum_compact_bytes_reclaimed", r.compact_bytes_reclaimed);
    ok = ok && extract_u64_field_optional(line, "vacuum_compact_last_elapsed_ms", r.compact_last_elapsed_ms);
    ok = ok && extract_u64_field_optional(line, "vacuum_queue_depth", r.vacuum_queue_depth);
    ok = ok && extract_u64_field_optional(line, "vacuum_queue_depth_peak", r.vacuum_queue_depth_peak);
    ok = ok && extract_u64_field(line, "write_conflicts", r.write_conflicts);
    ok = ok && extract_u64_field_optional(line, "lock_wait_ms_total", r.lock_wait_ms_total);
    ok = ok && extract_u64_field_optional(line, "lock_wait_max_ms", r.lock_wait_max_ms);
    ok = ok && extract_u64_field_optional(line, "lock_deadlock_detect_count", r.lock_deadlock_detect_count);
    ok = ok && extract_u64_field_optional(line, "lock_deadlock_victim_count", r.lock_deadlock_victim_count);
    ok = ok && extract_u64_field_optional(line, "txn_begin_lock_conflicts", r.txn_begin_lock_conflicts);
    ok = ok && extract_u64_field_optional(line, "wal_compact_count", r.wal_compact_count);
    ok = ok && extract_u64_field_optional(line, "wal_recovery_runs", r.wal_recovery_runs);
    ok = ok && extract_u64_field_optional(line, "wal_recovery_undo_ops", r.wal_recovery_undo_ops);
    ok = ok && extract_u64_field_optional(line, "wal_recovery_last_elapsed_ms", r.wal_recovery_last_elapsed_ms);
    ok = ok && extract_u64_field_optional(line, "wal_recovery_analyze_ms", r.wal_recovery_analyze_ms);
    ok = ok && extract_u64_field_optional(line, "wal_recovery_undo_ms", r.wal_recovery_undo_ms);
    ok = ok && extract_u64_field_optional(line, "wal_recovery_finalize_ms", r.wal_recovery_finalize_ms);
    ok = ok && extract_u64_field_optional(line, "wal_recovery_records_scanned", r.wal_recovery_records_scanned);
    ok = ok && extract_u64_field_optional(line, "wal_recovery_dangling_txns", r.wal_recovery_dangling_txns);
    ok = ok && extract_u64_field_optional(line, "wal_group_commit_count", r.wal_group_commit_count);
    ok = ok && extract_u64_field_optional(line, "wal_group_commit_batch_commits", r.wal_group_commit_batch_commits);
    ok = ok && extract_u64_field_optional(line, "scheduler_throttle_count", r.scheduler_throttle_count);
    ok = ok && extract_u64_field_optional(line, "lsm_memtable_flush_count", r.lsm_memtable_flush_count);
    ok = ok && extract_u64_field_optional(line, "lsm_compaction_count", r.lsm_compaction_count);
    ok = ok && extract_u64_field_optional(line, "lsm_segment_count", r.lsm_segment_count);
    ok = ok && extract_u64_field_optional(line, "lsm_memtable_bytes", r.lsm_memtable_bytes);
    ok = ok && extract_u64_field_optional(line, "lsm_read_segments_scanned", r.lsm_read_segments_scanned);
    ok = ok && extract_u64_field_optional(line, "lsm_read_segments_scanned_p95", r.lsm_read_segments_scanned_p95);
    ok = ok && extract_u64_field_optional(line, "lsm_compaction_bytes_in", r.lsm_compaction_bytes_in);
    ok = ok && extract_u64_field_optional(line, "lsm_compaction_bytes_out", r.lsm_compaction_bytes_out);
    ok = ok && extract_u64_field_optional(line, "lazy_materialize_count", r.lazy_materialize_count);
    ok = ok && extract_u64_field_optional(line, "lazy_materialize_rows_total", r.lazy_materialize_rows_total);
    ok = ok && extract_u64_field_optional(line, "lazy_materialize_max_rows", r.lazy_materialize_max_rows);
    ok = ok && extract_u64_field_optional(line, "lazy_materialize_elapsed_ms", r.lazy_materialize_elapsed_ms);
    if (extract_u64_field(line, "table_storage_health_logical_rows", r.table_storage_health_logical_rows)) {
        r.table_storage_health_sample_present = true;
        (void)extract_double_field(line, "table_storage_health_fragmentation_ratio",
                                   r.table_storage_health_fragmentation_ratio);
        (void)extract_u64_field_optional(line, "table_storage_health_dead_bytes", r.table_storage_health_dead_bytes);
    }
    (void)extract_u64_field_optional(line, "vacuum_health_bonus_last", r.vacuum_health_bonus_last);
    (void)extract_u64_field_optional(line, "compact_debt_bytes", r.compact_debt_bytes);
    (void)extract_u64_field_optional(line, "page_cache_hits", r.page_cache_hits);
    (void)extract_u64_field_optional(line, "page_cache_misses", r.page_cache_misses);
    (void)extract_u64_field_optional(line, "where_query_rows_scanned_total", r.where_query_rows_scanned_total);
    (void)extract_u64_field_optional(line, "where_query_rows_returned_total", r.where_query_rows_returned_total);
    (void)extract_u64_field_optional(line, "wal_recovery_redo_ms", r.wal_recovery_redo_ms);
    (void)extract_u64_field_optional(line, "memory_budget_reject_count", r.memory_budget_reject_count);
    r.ok = ok;
    return r;
}

double percentile_nearest_rank(std::vector<double> values, const double p) {
    if (values.empty()) return 0.0;
    std::sort(values.begin(), values.end());
    if (p <= 0.0) return values.front();
    if (p >= 1.0) return values.back();
    const std::size_t n = values.size();
    const std::size_t rank = static_cast<std::size_t>(std::ceil(p * static_cast<double>(n)));
    const std::size_t idx = (rank == 0) ? 0 : (rank - 1);
    return values[std::min(idx, n - 1)];
}

DistStats build_dist_stats(const std::vector<double>& values) {
    DistStats s{};
    if (values.empty()) return s;
    s.min = *std::min_element(values.begin(), values.end());
    s.max = *std::max_element(values.begin(), values.end());
    s.avg = std::accumulate(values.begin(), values.end(), 0.0) / static_cast<double>(values.size());
    s.p50 = percentile_nearest_rank(values, 0.50);
    s.p95 = percentile_nearest_rank(values, 0.95);
    return s;
}

std::string build_summary_json(std::size_t samples,
                               std::uint64_t trigger_delta,
                               std::uint64_t execute_delta,
                               std::uint64_t skip_delta,
                               std::uint64_t compact_success_delta,
                               std::uint64_t compact_failure_delta,
                               std::uint64_t compact_reclaimed_delta,
                               std::uint64_t compact_last_elapsed_ms,
                               std::uint64_t vacuum_queue_depth_peak_max,
                               std::uint64_t conflict_delta,
                               std::uint64_t lock_wait_ms_delta,
                               std::uint64_t lock_wait_max_ms_delta,
                               std::uint64_t lock_deadlock_delta,
                               std::uint64_t lock_deadlock_victim_delta,
                               std::uint64_t begin_lock_conflict_delta,
                               std::uint64_t wal_compact_delta,
                               std::uint64_t wal_recovery_runs_delta,
                               std::uint64_t wal_recovery_undo_ops_delta,
                               std::uint64_t wal_recovery_last_elapsed_ms_max,
                               std::uint64_t wal_recovery_analyze_ms_max,
                               std::uint64_t wal_recovery_undo_ms_max,
                               std::uint64_t wal_recovery_finalize_ms_max,
                               std::uint64_t wal_recovery_records_scanned_max,
                               std::uint64_t wal_recovery_dangling_txns_max,
                               std::uint64_t wal_group_commit_delta,
                               std::uint64_t wal_group_commit_batch_commits_delta,
                               std::uint64_t scheduler_throttle_delta,
                               std::uint64_t lsm_memtable_flush_delta,
                               std::uint64_t lsm_compaction_delta,
                               std::uint64_t lsm_segment_count_max,
                               std::uint64_t lsm_memtable_bytes_max,
                               std::uint64_t lsm_read_segments_scanned_delta,
                               std::uint64_t lsm_read_segments_scanned_p95_max,
                               std::uint64_t lsm_compaction_bytes_in_delta,
                               std::uint64_t lsm_compaction_bytes_out_delta,
                               double lsm_compaction_bytes_amp_efficiency,
                               std::uint64_t lazy_materialize_count_delta,
                               std::uint64_t lazy_materialize_rows_total_delta,
                               std::uint64_t lazy_materialize_max_rows_max,
                               std::uint64_t lazy_materialize_elapsed_ms_delta,
                               double vacuum_efficiency,
                               double conflict_rate,
                               const DistStats& vacuum_eff_stats,
                               const DistStats& conflict_rate_stats,
                               double table_storage_health_fragmentation_peak,
                               std::uint64_t table_storage_health_dead_bytes_peak,
                               bool table_storage_health_dead_peak_valid,
                               std::uint64_t vacuum_health_bonus_last_max,
                               std::uint64_t compact_debt_bytes_peak,
                               double page_cache_hit_ratio,
                               double where_scan_amplification,
                               double wal_recovery_redo_ratio,
                               std::uint64_t page_cache_hits_last,
                               std::uint64_t page_cache_misses_last,
                               std::uint64_t where_query_rows_scanned_delta,
                               std::uint64_t where_query_rows_returned_delta,
                               std::uint64_t memory_budget_reject_delta) {
    return std::string("{") +
           "\"samples\":" + std::to_string(samples) + "," +
           "\"vacuum_trigger_delta\":" + std::to_string(trigger_delta) + "," +
           "\"vacuum_execute_delta\":" + std::to_string(execute_delta) + "," +
           "\"vacuum_cooldown_skip_delta\":" + std::to_string(skip_delta) + "," +
           "\"vacuum_compact_success_delta\":" + std::to_string(compact_success_delta) + "," +
           "\"vacuum_compact_failure_delta\":" + std::to_string(compact_failure_delta) + "," +
           "\"vacuum_compact_reclaimed_bytes_delta\":" + std::to_string(compact_reclaimed_delta) + "," +
           "\"vacuum_compact_last_elapsed_ms\":" + std::to_string(compact_last_elapsed_ms) + "," +
           "\"vacuum_queue_depth_peak_max\":" + std::to_string(vacuum_queue_depth_peak_max) + "," +
           "\"write_conflict_delta\":" + std::to_string(conflict_delta) + "," +
           "\"lock_wait_ms_delta\":" + std::to_string(lock_wait_ms_delta) + "," +
           "\"lock_wait_max_ms_delta\":" + std::to_string(lock_wait_max_ms_delta) + "," +
           "\"lock_deadlock_detect_delta\":" + std::to_string(lock_deadlock_delta) + "," +
           "\"lock_deadlock_victim_delta\":" + std::to_string(lock_deadlock_victim_delta) + "," +
           "\"txn_begin_lock_conflict_delta\":" + std::to_string(begin_lock_conflict_delta) + "," +
           "\"wal_compact_delta\":" + std::to_string(wal_compact_delta) + "," +
           "\"wal_recovery_runs_delta\":" + std::to_string(wal_recovery_runs_delta) + "," +
           "\"wal_recovery_undo_ops_delta\":" + std::to_string(wal_recovery_undo_ops_delta) + "," +
           "\"wal_recovery_last_elapsed_ms_max\":" + std::to_string(wal_recovery_last_elapsed_ms_max) + "," +
           "\"wal_recovery_analyze_ms_max\":" + std::to_string(wal_recovery_analyze_ms_max) + "," +
           "\"wal_recovery_undo_ms_max\":" + std::to_string(wal_recovery_undo_ms_max) + "," +
           "\"wal_recovery_finalize_ms_max\":" + std::to_string(wal_recovery_finalize_ms_max) + "," +
           "\"wal_recovery_records_scanned_max\":" + std::to_string(wal_recovery_records_scanned_max) + "," +
           "\"wal_recovery_dangling_txns_max\":" + std::to_string(wal_recovery_dangling_txns_max) + "," +
           "\"wal_group_commit_delta\":" + std::to_string(wal_group_commit_delta) + "," +
           "\"wal_group_commit_batch_commits_delta\":" + std::to_string(wal_group_commit_batch_commits_delta) + "," +
           "\"scheduler_throttle_delta\":" + std::to_string(scheduler_throttle_delta) + "," +
           "\"lsm_memtable_flush_delta\":" + std::to_string(lsm_memtable_flush_delta) + "," +
           "\"lsm_compaction_delta\":" + std::to_string(lsm_compaction_delta) + "," +
           "\"lsm_segment_count_max\":" + std::to_string(lsm_segment_count_max) + "," +
           "\"lsm_memtable_bytes_max\":" + std::to_string(lsm_memtable_bytes_max) + "," +
           "\"lsm_read_segments_scanned_delta\":" + std::to_string(lsm_read_segments_scanned_delta) + "," +
           "\"lsm_read_segments_scanned_p95_max\":" + std::to_string(lsm_read_segments_scanned_p95_max) + "," +
           "\"lsm_compaction_bytes_in_delta\":" + std::to_string(lsm_compaction_bytes_in_delta) + "," +
           "\"lsm_compaction_bytes_out_delta\":" + std::to_string(lsm_compaction_bytes_out_delta) + "," +
           "\"lsm_compaction_bytes_amp_efficiency\":" + std::to_string(lsm_compaction_bytes_amp_efficiency) + "," +
           "\"lazy_materialize_count_delta\":" + std::to_string(lazy_materialize_count_delta) + "," +
           "\"lazy_materialize_rows_total_delta\":" + std::to_string(lazy_materialize_rows_total_delta) + "," +
           "\"lazy_materialize_max_rows_max\":" + std::to_string(lazy_materialize_max_rows_max) + "," +
           "\"lazy_materialize_elapsed_ms_delta\":" + std::to_string(lazy_materialize_elapsed_ms_delta) + "," +
           "\"vacuum_efficiency\":" + std::to_string(vacuum_efficiency) + "," +
           "\"conflict_rate\":" + std::to_string(conflict_rate) + "," +
           "\"vacuum_efficiency_min\":" + std::to_string(vacuum_eff_stats.min) + "," +
           "\"vacuum_efficiency_max\":" + std::to_string(vacuum_eff_stats.max) + "," +
           "\"vacuum_efficiency_avg\":" + std::to_string(vacuum_eff_stats.avg) + "," +
           "\"vacuum_efficiency_p50\":" + std::to_string(vacuum_eff_stats.p50) + "," +
           "\"vacuum_efficiency_p95\":" + std::to_string(vacuum_eff_stats.p95) + "," +
           "\"conflict_rate_min\":" + std::to_string(conflict_rate_stats.min) + "," +
           "\"conflict_rate_max\":" + std::to_string(conflict_rate_stats.max) + "," +
           "\"conflict_rate_avg\":" + std::to_string(conflict_rate_stats.avg) + "," +
           "\"conflict_rate_p50\":" + std::to_string(conflict_rate_stats.p50) + "," +
           "\"conflict_rate_p95\":" + std::to_string(conflict_rate_stats.p95) + "," +
           "\"table_storage_health_fragmentation_peak\":" +
           std::to_string(table_storage_health_fragmentation_peak) + "," +
           "\"table_storage_health_dead_bytes_peak\":" +
           std::to_string(table_storage_health_dead_bytes_peak) + "," +
           "\"table_storage_health_dead_peak_valid\":" +
           std::string(table_storage_health_dead_peak_valid ? "true" : "false") + "," +
           "\"vacuum_health_bonus_last_max\":" + std::to_string(vacuum_health_bonus_last_max) + "," +
           "\"compact_debt_bytes_peak\":" + std::to_string(compact_debt_bytes_peak) + "," +
           "\"page_cache_hit_ratio\":" + std::to_string(page_cache_hit_ratio) + "," +
           "\"where_scan_amplification\":" + std::to_string(where_scan_amplification) + "," +
           "\"wal_recovery_redo_ratio\":" + std::to_string(wal_recovery_redo_ratio) + "," +
           "\"page_cache_hits_last\":" + std::to_string(page_cache_hits_last) + "," +
           "\"page_cache_misses_last\":" + std::to_string(page_cache_misses_last) + "," +
           "\"where_query_rows_scanned_delta\":" + std::to_string(where_query_rows_scanned_delta) + "," +
           "\"where_query_rows_returned_delta\":" + std::to_string(where_query_rows_returned_delta) + "," +
           "\"memory_budget_reject_delta\":" + std::to_string(memory_budget_reject_delta) +
           "}";
}

} // namespace

int main(int argc, char** argv) {
    Args args;
    if (!parse_args(argc, argv, args)) {
        print_usage();
        return 2;
    }

    std::ifstream in(args.input_jsonl);
    if (!in.good()) {
        std::cerr << "failed to open input: " << args.input_jsonl << "\n";
        return 3;
    }

    std::vector<Row> rows;
    std::string line;
    std::string latest_run_id;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        Row r = parse_row(line);
        if (!r.ok) continue;
        if (!r.run_id.empty()) latest_run_id = r.run_id;
        if (!args.label_prefix.empty() && r.label.rfind(args.label_prefix, 0) != 0) continue;
        rows.push_back(r);
    }
    if (!args.run_id.empty()) {
        const std::string want = (args.run_id == "latest") ? latest_run_id : args.run_id;
        std::vector<Row> filtered;
        filtered.reserve(rows.size());
        for (const Row& r : rows) {
            if (r.run_id == want) filtered.push_back(r);
        }
        rows.swap(filtered);
    }
    if (args.last_n > 0 && static_cast<std::size_t>(args.last_n) < rows.size()) {
        rows.erase(rows.begin(), rows.end() - args.last_n);
    }
    if (rows.size() < 2) {
        std::cerr << "need at least 2 valid snapshot rows\n";
        return 4;
    }

    const Row& first = rows.front();
    const Row& last = rows.back();
    const std::uint64_t trigger_delta = (last.trigger_count >= first.trigger_count)
                                            ? (last.trigger_count - first.trigger_count)
                                            : 0;
    const std::uint64_t execute_delta = (last.execute_count >= first.execute_count)
                                            ? (last.execute_count - first.execute_count)
                                            : 0;
    const std::uint64_t skip_delta = (last.cooldown_skips >= first.cooldown_skips)
                                         ? (last.cooldown_skips - first.cooldown_skips)
                                         : 0;
    const std::uint64_t compact_success_delta =
        (last.compact_success_count >= first.compact_success_count)
            ? (last.compact_success_count - first.compact_success_count)
            : 0;
    const std::uint64_t compact_failure_delta =
        (last.compact_failure_count >= first.compact_failure_count)
            ? (last.compact_failure_count - first.compact_failure_count)
            : 0;
    const std::uint64_t compact_reclaimed_delta =
        (last.compact_bytes_reclaimed >= first.compact_bytes_reclaimed)
            ? (last.compact_bytes_reclaimed - first.compact_bytes_reclaimed)
            : 0;
    const std::uint64_t conflict_delta = (last.write_conflicts >= first.write_conflicts)
                                             ? (last.write_conflicts - first.write_conflicts)
                                             : 0;
    const std::uint64_t lock_wait_ms_delta = (last.lock_wait_ms_total >= first.lock_wait_ms_total)
                                                 ? (last.lock_wait_ms_total - first.lock_wait_ms_total)
                                                 : 0;
    const std::uint64_t lock_wait_max_ms_delta = (last.lock_wait_max_ms >= first.lock_wait_max_ms)
                                                     ? (last.lock_wait_max_ms - first.lock_wait_max_ms)
                                                     : 0;
    const std::uint64_t lock_deadlock_delta =
        (last.lock_deadlock_detect_count >= first.lock_deadlock_detect_count)
            ? (last.lock_deadlock_detect_count - first.lock_deadlock_detect_count)
            : 0;
    const std::uint64_t lock_deadlock_victim_delta =
        (last.lock_deadlock_victim_count >= first.lock_deadlock_victim_count)
            ? (last.lock_deadlock_victim_count - first.lock_deadlock_victim_count)
            : 0;
    const std::uint64_t begin_lock_conflict_delta =
        (last.txn_begin_lock_conflicts >= first.txn_begin_lock_conflicts)
            ? (last.txn_begin_lock_conflicts - first.txn_begin_lock_conflicts)
            : 0;
    const std::uint64_t wal_compact_delta = (last.wal_compact_count >= first.wal_compact_count)
                                                ? (last.wal_compact_count - first.wal_compact_count)
                                                : 0;
    const std::uint64_t wal_recovery_runs_delta =
        (last.wal_recovery_runs >= first.wal_recovery_runs)
            ? (last.wal_recovery_runs - first.wal_recovery_runs)
            : 0;
    const std::uint64_t wal_recovery_undo_ops_delta =
        (last.wal_recovery_undo_ops >= first.wal_recovery_undo_ops)
            ? (last.wal_recovery_undo_ops - first.wal_recovery_undo_ops)
            : 0;
    const std::uint64_t wal_group_commit_delta =
        (last.wal_group_commit_count >= first.wal_group_commit_count)
            ? (last.wal_group_commit_count - first.wal_group_commit_count)
            : 0;
    const std::uint64_t wal_group_commit_batch_commits_delta =
        (last.wal_group_commit_batch_commits >= first.wal_group_commit_batch_commits)
            ? (last.wal_group_commit_batch_commits - first.wal_group_commit_batch_commits)
            : 0;
    const std::uint64_t scheduler_throttle_delta =
        (last.scheduler_throttle_count >= first.scheduler_throttle_count)
            ? (last.scheduler_throttle_count - first.scheduler_throttle_count)
            : 0;
    const std::uint64_t lsm_memtable_flush_delta =
        (last.lsm_memtable_flush_count >= first.lsm_memtable_flush_count)
            ? (last.lsm_memtable_flush_count - first.lsm_memtable_flush_count)
            : 0;
    const std::uint64_t lsm_compaction_delta =
        (last.lsm_compaction_count >= first.lsm_compaction_count)
            ? (last.lsm_compaction_count - first.lsm_compaction_count)
            : 0;
    const std::uint64_t lsm_read_segments_scanned_delta =
        (last.lsm_read_segments_scanned >= first.lsm_read_segments_scanned)
            ? (last.lsm_read_segments_scanned - first.lsm_read_segments_scanned)
            : 0;
    const std::uint64_t lsm_compaction_bytes_in_delta =
        (last.lsm_compaction_bytes_in >= first.lsm_compaction_bytes_in)
            ? (last.lsm_compaction_bytes_in - first.lsm_compaction_bytes_in)
            : 0;
    const std::uint64_t lsm_compaction_bytes_out_delta =
        (last.lsm_compaction_bytes_out >= first.lsm_compaction_bytes_out)
            ? (last.lsm_compaction_bytes_out - first.lsm_compaction_bytes_out)
            : 0;
    const std::uint64_t lazy_materialize_count_delta =
        (last.lazy_materialize_count >= first.lazy_materialize_count)
            ? (last.lazy_materialize_count - first.lazy_materialize_count)
            : 0;
    const std::uint64_t lazy_materialize_rows_total_delta =
        (last.lazy_materialize_rows_total >= first.lazy_materialize_rows_total)
            ? (last.lazy_materialize_rows_total - first.lazy_materialize_rows_total)
            : 0;
    const std::uint64_t lazy_materialize_elapsed_ms_delta =
        (last.lazy_materialize_elapsed_ms >= first.lazy_materialize_elapsed_ms)
            ? (last.lazy_materialize_elapsed_ms - first.lazy_materialize_elapsed_ms)
            : 0;
    std::uint64_t vacuum_queue_depth_peak_max = 0;
    std::uint64_t wal_recovery_last_elapsed_ms_max = 0;
    std::uint64_t wal_recovery_analyze_ms_max = 0;
    std::uint64_t wal_recovery_undo_ms_max = 0;
    std::uint64_t wal_recovery_finalize_ms_max = 0;
    std::uint64_t wal_recovery_records_scanned_max = 0;
    std::uint64_t wal_recovery_dangling_txns_max = 0;
    std::uint64_t lsm_segment_count_max = 0;
    std::uint64_t lsm_memtable_bytes_max = 0;
    std::uint64_t lsm_read_segments_scanned_p95_max = 0;
    std::uint64_t lazy_materialize_max_rows_max = 0;
    double table_storage_health_fragmentation_peak = -1.0;
    std::uint64_t table_storage_health_dead_bytes_peak = 0;
    bool table_storage_health_dead_peak_valid = false;
    std::uint64_t vacuum_health_bonus_last_max = 0;
    std::uint64_t compact_debt_bytes_peak = 0;
    for (const Row& r : rows) {
        compact_debt_bytes_peak = std::max(compact_debt_bytes_peak, r.compact_debt_bytes);
        if (r.table_storage_health_sample_present && r.table_storage_health_logical_rows > 0) {
            table_storage_health_fragmentation_peak =
                std::max(table_storage_health_fragmentation_peak, r.table_storage_health_fragmentation_ratio);
            table_storage_health_dead_peak_valid = true;
            table_storage_health_dead_bytes_peak =
                std::max(table_storage_health_dead_bytes_peak, r.table_storage_health_dead_bytes);
        }
        vacuum_health_bonus_last_max = std::max(vacuum_health_bonus_last_max, r.vacuum_health_bonus_last);
        lazy_materialize_max_rows_max = std::max(lazy_materialize_max_rows_max, r.lazy_materialize_max_rows);
        vacuum_queue_depth_peak_max = std::max(vacuum_queue_depth_peak_max, r.vacuum_queue_depth_peak);
        wal_recovery_last_elapsed_ms_max =
            std::max(wal_recovery_last_elapsed_ms_max, r.wal_recovery_last_elapsed_ms);
        wal_recovery_analyze_ms_max = std::max(wal_recovery_analyze_ms_max, r.wal_recovery_analyze_ms);
        wal_recovery_undo_ms_max = std::max(wal_recovery_undo_ms_max, r.wal_recovery_undo_ms);
        wal_recovery_finalize_ms_max = std::max(wal_recovery_finalize_ms_max, r.wal_recovery_finalize_ms);
        wal_recovery_records_scanned_max =
            std::max(wal_recovery_records_scanned_max, r.wal_recovery_records_scanned);
        wal_recovery_dangling_txns_max =
            std::max(wal_recovery_dangling_txns_max, r.wal_recovery_dangling_txns);
        lsm_segment_count_max = std::max(lsm_segment_count_max, r.lsm_segment_count);
        lsm_memtable_bytes_max = std::max(lsm_memtable_bytes_max, r.lsm_memtable_bytes);
        lsm_read_segments_scanned_p95_max =
            std::max(lsm_read_segments_scanned_p95_max, r.lsm_read_segments_scanned_p95);
    }
    const double lsm_compaction_bytes_amp_efficiency =
        (lsm_compaction_bytes_in_delta == 0)
            ? 1.0
            : (static_cast<double>(lsm_compaction_bytes_out_delta) /
               static_cast<double>(lsm_compaction_bytes_in_delta));

    const double vacuum_efficiency =
        (trigger_delta == 0) ? 1.0 : (static_cast<double>(execute_delta) / static_cast<double>(trigger_delta));
    const double conflict_rate =
        static_cast<double>(conflict_delta) / static_cast<double>(rows.size() - 1);

    std::vector<double> vacuum_eff_series;
    std::vector<double> conflict_rate_series;
    vacuum_eff_series.reserve(rows.size() - 1);
    conflict_rate_series.reserve(rows.size() - 1);
    for (std::size_t i = 1; i < rows.size(); ++i) {
        const Row& prev = rows[i - 1];
        const Row& cur = rows[i];
        const std::uint64_t trig_d = (cur.trigger_count >= prev.trigger_count) ? (cur.trigger_count - prev.trigger_count) : 0;
        const std::uint64_t exec_d = (cur.execute_count >= prev.execute_count) ? (cur.execute_count - prev.execute_count) : 0;
        const std::uint64_t conf_d =
            (cur.write_conflicts >= prev.write_conflicts) ? (cur.write_conflicts - prev.write_conflicts) : 0;
        const double eff = (trig_d == 0) ? 1.0 : (static_cast<double>(exec_d) / static_cast<double>(trig_d));
        vacuum_eff_series.push_back(eff);
        conflict_rate_series.push_back(static_cast<double>(conf_d));
    }
    const DistStats vacuum_eff_stats = build_dist_stats(vacuum_eff_series);
    const DistStats conflict_rate_stats = build_dist_stats(conflict_rate_series);

    const std::uint64_t where_scanned_delta =
        (last.where_query_rows_scanned_total >= first.where_query_rows_scanned_total)
            ? (last.where_query_rows_scanned_total - first.where_query_rows_scanned_total)
            : 0;
    const std::uint64_t where_returned_delta =
        (last.where_query_rows_returned_total >= first.where_query_rows_returned_total)
            ? (last.where_query_rows_returned_total - first.where_query_rows_returned_total)
            : 0;
    const double where_scan_amplification =
        static_cast<double>(where_scanned_delta) /
        static_cast<double>(std::max<std::uint64_t>(where_returned_delta, 1));
    const std::uint64_t lookups_last = last.page_cache_hits + last.page_cache_misses;
    const double page_cache_hit_ratio =
        (lookups_last == 0)
            ? -1.0
            : (static_cast<double>(last.page_cache_hits) / static_cast<double>(lookups_last));
    const double wal_recovery_redo_ratio =
        (last.wal_recovery_last_elapsed_ms == 0)
            ? 0.0
            : (static_cast<double>(last.wal_recovery_redo_ms) /
               static_cast<double>(std::max<std::uint64_t>(last.wal_recovery_last_elapsed_ms, 1ULL)));
    const std::uint64_t memory_budget_reject_delta =
        (last.memory_budget_reject_count >= first.memory_budget_reject_count)
            ? (last.memory_budget_reject_count - first.memory_budget_reject_count)
            : 0;

    const std::string summary = build_summary_json(
        rows.size(),
        trigger_delta,
        execute_delta,
        skip_delta,
        compact_success_delta,
        compact_failure_delta,
        compact_reclaimed_delta,
        last.compact_last_elapsed_ms,
        vacuum_queue_depth_peak_max,
        conflict_delta,
        lock_wait_ms_delta,
        lock_wait_max_ms_delta,
        lock_deadlock_delta,
        lock_deadlock_victim_delta,
        begin_lock_conflict_delta,
        wal_compact_delta,
        wal_recovery_runs_delta,
        wal_recovery_undo_ops_delta,
        wal_recovery_last_elapsed_ms_max,
        wal_recovery_analyze_ms_max,
        wal_recovery_undo_ms_max,
        wal_recovery_finalize_ms_max,
        wal_recovery_records_scanned_max,
        wal_recovery_dangling_txns_max,
        wal_group_commit_delta,
        wal_group_commit_batch_commits_delta,
        scheduler_throttle_delta,
        lsm_memtable_flush_delta,
        lsm_compaction_delta,
        lsm_segment_count_max,
        lsm_memtable_bytes_max,
        lsm_read_segments_scanned_delta,
        lsm_read_segments_scanned_p95_max,
        lsm_compaction_bytes_in_delta,
        lsm_compaction_bytes_out_delta,
        lsm_compaction_bytes_amp_efficiency,
        lazy_materialize_count_delta,
        lazy_materialize_rows_total_delta,
        lazy_materialize_max_rows_max,
        lazy_materialize_elapsed_ms_delta,
        vacuum_efficiency,
        conflict_rate,
        vacuum_eff_stats,
        conflict_rate_stats,
        table_storage_health_fragmentation_peak,
        table_storage_health_dead_bytes_peak,
        table_storage_health_dead_peak_valid,
        vacuum_health_bonus_last_max,
        compact_debt_bytes_peak,
        page_cache_hit_ratio,
        where_scan_amplification,
        wal_recovery_redo_ratio,
        last.page_cache_hits,
        last.page_cache_misses,
        where_scanned_delta,
        where_returned_delta,
        memory_budget_reject_delta);

    std::cout << summary << "\n";
    if (!args.output_json.empty()) {
        std::ofstream out(args.output_json, std::ios::out | std::ios::trunc);
        if (!out.good()) {
            std::cerr << "failed to write output: " << args.output_json << "\n";
            return 5;
        }
        out << summary << "\n";
    }

    if (args.min_vacuum_efficiency >= 0.0 && vacuum_efficiency < args.min_vacuum_efficiency) {
        std::cerr << "gate failed: vacuum_efficiency(" << vacuum_efficiency
                  << ") < min_vacuum_efficiency(" << args.min_vacuum_efficiency << ")\n";
        return 10;
    }
    if (args.max_conflict_rate >= 0.0 && conflict_rate > args.max_conflict_rate) {
        std::cerr << "gate failed: conflict_rate(" << conflict_rate
                  << ") > max_conflict_rate(" << args.max_conflict_rate << ")\n";
        return 11;
    }
    if (args.min_vacuum_efficiency_p50 >= 0.0 && vacuum_eff_stats.p50 < args.min_vacuum_efficiency_p50) {
        std::cerr << "gate failed: vacuum_efficiency_p50(" << vacuum_eff_stats.p50
                  << ") < min_vacuum_efficiency_p50(" << args.min_vacuum_efficiency_p50 << ")\n";
        return 12;
    }
    if (args.max_conflict_rate_p95 >= 0.0 && conflict_rate_stats.p95 > args.max_conflict_rate_p95) {
        std::cerr << "gate failed: conflict_rate_p95(" << conflict_rate_stats.p95
                  << ") > max_conflict_rate_p95(" << args.max_conflict_rate_p95 << ")\n";
        return 13;
    }
    if (args.max_txn_begin_lock_conflict_delta >= 0.0 &&
        static_cast<double>(begin_lock_conflict_delta) > args.max_txn_begin_lock_conflict_delta) {
        std::cerr << "gate failed: txn_begin_lock_conflict_delta(" << begin_lock_conflict_delta
                  << ") > max_txn_begin_lock_conflict_delta(" << args.max_txn_begin_lock_conflict_delta << ")\n";
        return 14;
    }
    if (args.max_wal_compact_delta >= 0.0 && static_cast<double>(wal_compact_delta) > args.max_wal_compact_delta) {
        std::cerr << "gate failed: wal_compact_delta(" << wal_compact_delta
                  << ") > max_wal_compact_delta(" << args.max_wal_compact_delta << ")\n";
        return 15;
    }
    if (args.max_vacuum_compact_failure_delta >= 0.0 &&
        static_cast<double>(compact_failure_delta) > args.max_vacuum_compact_failure_delta) {
        std::cerr << "gate failed: vacuum_compact_failure_delta(" << compact_failure_delta
                  << ") > max_vacuum_compact_failure_delta(" << args.max_vacuum_compact_failure_delta << ")\n";
        return 16;
    }
    if (args.min_vacuum_compact_reclaimed_bytes_delta >= 0.0 &&
        static_cast<double>(compact_reclaimed_delta) < args.min_vacuum_compact_reclaimed_bytes_delta) {
        std::cerr << "gate failed: vacuum_compact_reclaimed_bytes_delta(" << compact_reclaimed_delta
                  << ") < min_vacuum_compact_reclaimed_bytes_delta("
                  << args.min_vacuum_compact_reclaimed_bytes_delta << ")\n";
        return 17;
    }
    if (args.max_vacuum_queue_depth_peak >= 0.0 &&
        static_cast<double>(vacuum_queue_depth_peak_max) > args.max_vacuum_queue_depth_peak) {
        std::cerr << "gate failed: vacuum_queue_depth_peak_max(" << vacuum_queue_depth_peak_max
                  << ") > max_vacuum_queue_depth_peak(" << args.max_vacuum_queue_depth_peak << ")\n";
        return 18;
    }
    if (args.max_wal_recovery_last_elapsed_ms >= 0.0 &&
        static_cast<double>(wal_recovery_last_elapsed_ms_max) > args.max_wal_recovery_last_elapsed_ms) {
        std::cerr << "gate failed: wal_recovery_last_elapsed_ms_max(" << wal_recovery_last_elapsed_ms_max
                  << ") > max_wal_recovery_last_elapsed_ms(" << args.max_wal_recovery_last_elapsed_ms << ")\n";
        return 19;
    }
    if (args.max_lock_deadlock_detect_delta >= 0.0 &&
        static_cast<double>(lock_deadlock_delta) > args.max_lock_deadlock_detect_delta) {
        std::cerr << "gate failed: lock_deadlock_detect_delta(" << lock_deadlock_delta
                  << ") > max_lock_deadlock_detect_delta(" << args.max_lock_deadlock_detect_delta << ")\n";
        return 20;
    }
    if (args.max_lock_deadlock_victim_delta >= 0.0 &&
        static_cast<double>(lock_deadlock_victim_delta) > args.max_lock_deadlock_victim_delta) {
        std::cerr << "gate failed: lock_deadlock_victim_delta(" << lock_deadlock_victim_delta
                  << ") > max_lock_deadlock_victim_delta(" << args.max_lock_deadlock_victim_delta << ")\n";
        return 23;
    }
    if (args.max_lock_wait_max_ms_delta >= 0.0 &&
        static_cast<double>(lock_wait_max_ms_delta) > args.max_lock_wait_max_ms_delta) {
        std::cerr << "gate failed: lock_wait_max_ms_delta(" << lock_wait_max_ms_delta
                  << ") > max_lock_wait_max_ms_delta(" << args.max_lock_wait_max_ms_delta << ")\n";
        return 24;
    }
    if (args.max_scheduler_throttle_delta >= 0.0 &&
        static_cast<double>(scheduler_throttle_delta) > args.max_scheduler_throttle_delta) {
        std::cerr << "gate failed: scheduler_throttle_delta(" << scheduler_throttle_delta
                  << ") > max_scheduler_throttle_delta(" << args.max_scheduler_throttle_delta << ")\n";
        return 21;
    }
    if (args.min_wal_group_commit_batch_commits_delta >= 0.0 &&
        static_cast<double>(wal_group_commit_batch_commits_delta) <
            args.min_wal_group_commit_batch_commits_delta) {
        std::cerr << "gate failed: wal_group_commit_batch_commits_delta("
                  << wal_group_commit_batch_commits_delta
                  << ") < min_wal_group_commit_batch_commits_delta("
                  << args.min_wal_group_commit_batch_commits_delta << ")\n";
        return 22;
    }
    if (args.max_lsm_segment_count >= 0.0 &&
        static_cast<double>(lsm_segment_count_max) > args.max_lsm_segment_count) {
        std::cerr << "gate failed: lsm_segment_count_max(" << lsm_segment_count_max
                  << ") > max_lsm_segment_count(" << args.max_lsm_segment_count << ")\n";
        return 25;
    }
    if (args.min_lsm_memtable_flush_delta >= 0.0 &&
        static_cast<double>(lsm_memtable_flush_delta) < args.min_lsm_memtable_flush_delta) {
        std::cerr << "gate failed: lsm_memtable_flush_delta(" << lsm_memtable_flush_delta
                  << ") < min_lsm_memtable_flush_delta(" << args.min_lsm_memtable_flush_delta << ")\n";
        return 26;
    }
    if (args.max_lsm_read_segments_scanned_p95 >= 0.0 &&
        static_cast<double>(lsm_read_segments_scanned_p95_max) > args.max_lsm_read_segments_scanned_p95) {
        std::cerr << "gate failed: lsm_read_segments_scanned_p95_max(" << lsm_read_segments_scanned_p95_max
                  << ") > max_lsm_read_segments_scanned_p95(" << args.max_lsm_read_segments_scanned_p95 << ")\n";
        return 27;
    }
    if (args.min_lsm_compaction_bytes_amp_efficiency >= 0.0 &&
        lsm_compaction_bytes_amp_efficiency < args.min_lsm_compaction_bytes_amp_efficiency) {
        std::cerr << "gate failed: lsm_compaction_bytes_amp_efficiency(" << lsm_compaction_bytes_amp_efficiency
                  << ") < min_lsm_compaction_bytes_amp_efficiency("
                  << args.min_lsm_compaction_bytes_amp_efficiency << ")\n";
        return 28;
    }
    if (args.max_lazy_materialize_count_delta >= 0.0 &&
        static_cast<double>(lazy_materialize_count_delta) > args.max_lazy_materialize_count_delta) {
        std::cerr << "gate failed: lazy_materialize_count_delta(" << lazy_materialize_count_delta
                  << ") > max_lazy_materialize_count_delta(" << args.max_lazy_materialize_count_delta << ")\n";
        return 29;
    }
    if (args.max_lazy_materialize_rows_total_delta >= 0.0 &&
        static_cast<double>(lazy_materialize_rows_total_delta) > args.max_lazy_materialize_rows_total_delta) {
        std::cerr << "gate failed: lazy_materialize_rows_total_delta(" << lazy_materialize_rows_total_delta
                  << ") > max_lazy_materialize_rows_total_delta("
                  << args.max_lazy_materialize_rows_total_delta << ")\n";
        return 30;
    }
    if (args.max_table_storage_health_fragmentation_ratio >= 0.0 && table_storage_health_fragmentation_peak >= 0.0 &&
        table_storage_health_fragmentation_peak > args.max_table_storage_health_fragmentation_ratio) {
        std::cerr << "gate failed: table_storage_health_fragmentation_peak("
                  << table_storage_health_fragmentation_peak
                  << ") > max_table_storage_health_fragmentation_ratio("
                  << args.max_table_storage_health_fragmentation_ratio << ")\n";
        return 31;
    }
    if (args.max_table_storage_health_dead_bytes >= 0.0 && table_storage_health_dead_peak_valid &&
        static_cast<double>(table_storage_health_dead_bytes_peak) > args.max_table_storage_health_dead_bytes) {
        std::cerr << "gate failed: table_storage_health_dead_bytes_peak("
                  << static_cast<unsigned long long>(table_storage_health_dead_bytes_peak)
                  << ") > max_table_storage_health_dead_bytes(" << args.max_table_storage_health_dead_bytes << ")\n";
        return 32;
    }
    if (args.max_vacuum_health_bonus_last >= 0.0 &&
        static_cast<double>(vacuum_health_bonus_last_max) > args.max_vacuum_health_bonus_last) {
        std::cerr << "gate failed: vacuum_health_bonus_last_max("
                  << static_cast<unsigned long long>(vacuum_health_bonus_last_max)
                  << ") > max_vacuum_health_bonus_last(" << args.max_vacuum_health_bonus_last << ")\n";
        return 33;
    }
    if (args.max_compact_debt_bytes_peak >= 0.0 &&
        static_cast<double>(compact_debt_bytes_peak) > args.max_compact_debt_bytes_peak) {
        std::cerr << "gate failed: compact_debt_bytes_peak("
                  << static_cast<unsigned long long>(compact_debt_bytes_peak)
                  << ") > max_compact_debt_bytes_peak(" << args.max_compact_debt_bytes_peak << ")\n";
        return 34;
    }
    if (args.min_page_cache_hit_ratio >= 0.0 && lookups_last > 0 &&
        page_cache_hit_ratio < args.min_page_cache_hit_ratio) {
        std::cerr << "gate failed: page_cache_hit_ratio(" << page_cache_hit_ratio
                  << ") < min_page_cache_hit_ratio(" << args.min_page_cache_hit_ratio << ")\n";
        return 35;
    }
    if (args.max_where_scan_amplification >= 0.0 &&
        (where_scanned_delta > 0 || where_returned_delta > 0) &&
        where_scan_amplification > args.max_where_scan_amplification) {
        std::cerr << "gate failed: where_scan_amplification(" << where_scan_amplification
                  << ") > max_where_scan_amplification(" << args.max_where_scan_amplification << ")\n";
        return 36;
    }
    if (args.max_memory_budget_reject_delta >= 0.0 &&
        static_cast<double>(memory_budget_reject_delta) > args.max_memory_budget_reject_delta) {
        std::cerr << "gate failed: memory_budget_reject_delta(" << memory_budget_reject_delta
                  << ") > max_memory_budget_reject_delta(" << args.max_memory_budget_reject_delta << ")\n";
        return 37;
    }
    return 0;
}

