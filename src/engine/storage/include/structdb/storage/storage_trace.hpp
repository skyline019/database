#pragma once

namespace structdb::storage::trace {

/// `STRUCTDB_TRACE=1` 下存储引擎 span 统一前缀（`SpanGuard` 传入 `std::string`）。
inline constexpr const char kOpenRoot[] = "stdb.storage.open";
inline constexpr const char kOpenPhasePrefix[] = "stdb.storage.open.";
inline constexpr const char kFlushMemtable[] = "stdb.storage.flush_memtable";
inline constexpr const char kDrainL0Compactions[] = "stdb.storage.drain_l0_compactions";
inline constexpr const char kCompactionWorkerTask[] = "stdb.storage.compaction_worker_task";
inline constexpr const char kCompactMergeL0[] = "stdb.storage.compact_merge_l0";
inline constexpr const char kCompactMergeL1ToL2[] = "stdb.storage.compact_merge_l1_to_l2";
inline constexpr const char kCompactMergeL2ToL3[] = "stdb.storage.compact_merge_l2_to_l3";
inline constexpr const char kCompactMergeL3ToL4[] = "stdb.storage.compact_merge_l3_to_l4";

}  // namespace structdb::storage::trace
