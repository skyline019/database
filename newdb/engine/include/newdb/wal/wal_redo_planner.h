#pragma once

#include <newdb/heap_table.h>
#include <newdb/row.h>
#include <newdb/schema.h>
#include <newdb/wal_codec.h>
#include <newdb/wal_manager.h>

#include <cstdint>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace newdb {
namespace wal_recovery {

/// Sequenced redo operation produced by `WalRedoPlanner` for committed transactions.
struct RedoOp {
    WalOp op{WalOp::INSERT};
    Row row{};
};

/// Full Planner output ready to be replayed by `WalRedoApplier` (commit-time order preserved).
struct RedoPlanSummary {
    /// Ops ordered by COMMIT/CHECKPOINT sequence; rows already filtered to target table.
    std::vector<RedoOp> committed_ops;
    /// Number of distinct txns whose pending ops were dropped (no COMMIT seen by EOF).
    std::uint64_t uncommitted_txn_discarded_count{0};
    /// Sum of dropped pending ops across all uncommitted txns (book-keeping, no apply).
    std::uint64_t recovery_uncommitted_records_ignored{0};
    /// Wall-time accumulated across `feed_record` (decode + bookkeeping; apply is excluded).
    std::uint64_t plan_ms{0};
};

/// Phase-2 closed-loop split: incremental redo planner that decodes WAL records and stages
/// commit-time ops without touching `HeapTable` (Applier writes the heap).
///
/// Design notes (see `newdb_v2_闭环路线`):
/// - `feed_record(...)` consumes one already-decoded WAL record (header + raw payload).
/// - Records targeting a table other than `target_table_name_` are dropped silently.
/// - Decode failures bump `out_decode_failures_` (mirrors existing `WalRecoveryStats::decode_failures`).
/// - On `COMMIT`, all pending ops for that txn are flushed into `committed_ops_` (preserving txn
///   commit order across calls) and the txn entry is erased.
/// - On `ROLLBACK`, the txn entry is erased without flushing.
/// - `finalize()` returns a fresh `RedoPlanSummary` and clears internal pending state; uncommitted
///   txn counts are accumulated into the summary (matches legacy `recover_replay_segments` accounting).
class WalRedoPlanner {
public:
    WalRedoPlanner(const TableSchema& schema, std::string target_table_name)
        : schema_(&schema), target_table_name_(std::move(target_table_name)) {}

    void feed_record(const WalRecordHeader& hdr, const std::vector<uint8_t>& payload);

    [[nodiscard]] RedoPlanSummary finalize();

    /// Decode failure counter accumulated during `feed_record` (mirrors `decode_failures` stat).
    [[nodiscard]] std::uint64_t decode_failures() const { return decode_failures_; }

    /// Hook for callers tracking checkpoint records separately (counts updated on relevant ops).
    [[nodiscard]] std::uint64_t checkpoint_begin_count() const { return checkpoint_begin_count_; }
    [[nodiscard]] std::uint64_t checkpoint_end_count() const { return checkpoint_end_count_; }

    /// Convenience: sum of records consumed by `feed_record` (any WalOp).
    [[nodiscard]] std::uint64_t records_seen() const { return records_seen_; }

private:
    struct PendingTxnOps {
        /// Latest WalOp + Row keyed by row id (last-writer-wins inside a txn, matches legacy behavior).
        std::unordered_map<int, RedoOp> by_row_id;
        /// Insertion order of row ids inside the txn (so commit order is stable across replay).
        std::vector<int> order;
    };

    const TableSchema* schema_{nullptr};
    std::string target_table_name_;
    std::unordered_map<std::uint64_t, PendingTxnOps> txn_ops_;
    std::vector<RedoOp> committed_ops_;
    std::uint64_t decode_failures_{0};
    std::uint64_t records_seen_{0};
    std::uint64_t checkpoint_begin_count_{0};
    std::uint64_t checkpoint_end_count_{0};
    std::uint64_t plan_ms_{0};
};

}  // namespace wal_recovery
}  // namespace newdb
