export function isInsertOrUpdateCommand(text: string): boolean {
  const t = text.trim().toUpperCase();
  return t.startsWith("INSERT(") || t.startsWith("UPDATE(");
}

export function hasCommandError(raw: string, errorCode?: string | null): boolean {
  if (errorCode && errorCode.toLowerCase() !== "ok") return true;
  const t = raw || "";
  if (/\[CAPI_ERROR\]\s+code=/i.test(t)) return true;
  if (/\[ERROR\]/i.test(t)) return true;
  if (/expects .*?, got '/i.test(t)) return true;
  if (/\[SHOW PLAN\]\s+(blocked|invalid|usage)/i.test(t)) return true;
  if (/\b(fail|failed|invalid|duplicate|missing|usage)\b/i.test(t)) return true;
  return false;
}

export function shouldStopAndSkipHistory(
  command: string,
  rawOutput: string,
  errorCode?: string | null
): boolean {
  if (!isInsertOrUpdateCommand(command)) return false;
  return hasCommandError(rawOutput, errorCode);
}

/** Grouped keys for `SHOW TUNING JSON` / C API runtime mirror — sync with `scripts/validate/RUNTIME_STATS_SCHEMA.md` and required keys in `scripts/validate/contract/runtime_stats.v1.required.json`. */
export const RUNTIME_TUNING_DIAGNOSTIC_GROUPS: ReadonlyArray<{ title: string; keys: readonly string[] }> = [
  {
    title: "WHERE / 计划观测",
    keys: [
      "where_query_cache_lookups",
      "where_query_cache_hits",
      "where_heap_scan_budget_binding_events",
      "where_fallback_scans",
      "where_plan_eq_sidecar_count",
      "where_plan_id_pk_count",
      "where_plan_fallback_count",
      "where_eq_sidecar_disk_bytes_read_total",
      "where_eq_sidecar_disk_loads",
    ],
  },
  {
    title: "Snapshot / 读路径",
    keys: [
      "transaction_snapshot_lsn",
      "statement_snapshot_lsn",
      "txn_snapshot_refresh_count",
      "txn_snapshot_pinned_count",
      "txn_readpath_disabled_count",
      "last_snapshot_source",
      "lock_key_range_count",
      "lock_key_predicate_count",
    ],
  },
  {
    title: "PageCache / memory budget",
    keys: [
      "page_cache_hits",
      "page_cache_misses",
      "page_cache_bytes_in_cache",
      "memory_budget_max_bytes",
      "memory_budget_used_bytes",
      "memory_budget_reject_count",
      "memory_budget_bytes_evicted_total",
      "memory_budget_sidecar_load_skipped_total",
    ],
  },
  {
    title: "WAL 恢复摘要",
    keys: [
      "wal_recovery_runs",
      "wal_recovery_last_elapsed_ms",
      "wal_recovery_redo_ms",
      "wal_recovery_checkpoint_begin_count",
      "wal_recovery_checkpoint_end_count",
    ],
  },
  {
    title: "表存储健康 (SHOW TUNING JSON)",
    keys: [
      "table_storage_health_logical_rows",
      "table_storage_health_physical_rows",
      "table_storage_health_tombstone_rows",
      "table_storage_health_data_file_bytes",
      "table_storage_health_live_bytes",
      "table_storage_health_dead_bytes",
      "table_storage_health_fragmentation_ratio",
      "table_storage_health_last_vacuum_lsn",
      "table_storage_health_last_vacuum_elapsed_ms",
      "table_storage_health_tier",
      "vacuum_health_bonus_last",
    ],
  },
] as const;
