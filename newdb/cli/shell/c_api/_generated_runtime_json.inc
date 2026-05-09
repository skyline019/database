static std::string build_runtime_stats_json(ShellStateFacade& f) {
    const auto stats = f.txn().runtimeStats();
    const auto isolation = f.txn().txnIsolationLevel();
    const char* isolation_s =
        (isolation == ::TxnIsolationLevel::ReadCommitted) ? "read_committed" : "snapshot";
    std::ostringstream oss;
    oss << "{"
        << "\"txn_isolation\":\"" << isolation_s << "\",";
    newdb::append_runtime_stats_snapshot_json_members_before_where(oss, stats);
    oss         << "\"where_query_cache_lookups\":" << f.where().cache_lookups.load(std::memory_order_relaxed) << ","
        << "\"where_query_cache_hits\":" << f.where().cache_hits.load(std::memory_order_relaxed) << ","
        << "\"where_policy_checks\":" << f.where().policy_checks.load(std::memory_order_relaxed) << ","
        << "\"where_policy_rejects\":" << f.where().policy_rejects.load(std::memory_order_relaxed) << ","
        << "\"where_heap_scan_budget_binding_events\":"
        << f.where().where_heap_scan_budget_binding_events.load(std::memory_order_relaxed) << ","
        << "\"where_fallback_scans\":" << f.where().fallback_scans.load(std::memory_order_relaxed) << ","
        << "\"where_plan_eq_sidecar_count\":" << f.where().plan_eq_sidecar_count.load(std::memory_order_relaxed)
        << ","
        << "\"where_plan_id_pk_count\":" << f.where().plan_id_pk_count.load(std::memory_order_relaxed) << ","
        << "\"where_plan_fallback_count\":" << f.where().plan_fallback_count.load(std::memory_order_relaxed)
        << ","
        << "\"where_query_count\":" << f.where().query_count.load(std::memory_order_relaxed) << ","
        << "\"where_query_rows_scanned_total\":" << f.where().query_rows_scanned_total.load(std::memory_order_relaxed)
        << ","
        << "\"where_query_rows_returned_total\":"
        << f.where().query_rows_returned_total.load(std::memory_order_relaxed) << ","
        << "\"where_eq_sidecar_disk_bytes_read_total\":"
        << f.where().where_eq_sidecar_disk_bytes_read_total.load(std::memory_order_relaxed) << ","
        << "\"where_eq_sidecar_disk_loads\":" << f.where().where_eq_sidecar_disk_loads.load(std::memory_order_relaxed)
        << ","
        << "\"heap_decode_slot_calls\":" << f.heap_decode_slot_calls() << ","
        << "\"heap_decode_slot_hits\":" << f.heap_decode_slot_hits() << ","
        << "\"heap_decode_slot_misses\":" << f.heap_decode_slot_misses() << ",";
    newdb::append_runtime_stats_snapshot_json_members_after_heap(oss, stats);
    oss << "\"wal_adaptive_enabled\":" << (f.txn().walAdaptiveEnabled() ? "true" : "false") << ","
        << "\"group_commit_window_ms\":" << f.txn().groupCommitWindowMs() << ","
        << "\"group_commit_max_batch_commits\":" << f.txn().groupCommitMaxBatchCommits() << ","
        << "\"vacuum_running\":" << (f.txn().vacuumRunning() ? "true" : "false") << ","
        << "\"vacuum_ops_threshold\":" << f.txn().vacuumOpsThreshold() << ","
        << "\"vacuum_min_interval_sec\":" << f.txn().vacuumMinIntervalSec()
        << "}";
    return oss.str();
}

static std::string build_runtime_snapshot_jsonl_line(ShellStateFacade& f, const std::string& label) {
    const auto now = std::chrono::system_clock::now();
    const auto ts_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    std::ostringstream oss;
    oss << "{"
        << "\"schema_version\":\"newdb.runtime_stats.v1\","
        << "\"ts_ms\":" << ts_ms << ","
        << "\"label\":\"" << newdb::json_escape(label) << "\","
        << "\"stats\":" << build_runtime_stats_json(f)
        << "}";
    return oss.str();
}
