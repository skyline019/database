#pragma once

#include <ostream>

/// POD snapshot (`txn_runtime_stats_snapshot.h`); global struct, aliased as coordinator `TxnRuntimeStats`.
struct TxnRuntimeStats;

namespace newdb {

/// Emits JSON fields from `TxnRuntimeStats` only (vacuum … lazy_materialize), including trailing comma.
void append_runtime_stats_snapshot_json_members_before_where(std::ostream& oss, const TxnRuntimeStats& stats);

/// Emits JSON fields from `TxnRuntimeStats` only (vacuum_priority … write_lsm_rotate_compact_max), including trailing comma.
void append_runtime_stats_snapshot_json_members_after_heap(std::ostream& oss, const TxnRuntimeStats& stats);

} // namespace newdb
