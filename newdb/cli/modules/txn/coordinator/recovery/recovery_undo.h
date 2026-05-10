#pragma once

#include "cli/modules/txn/coordinator/recovery/recovery_wal_op.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

struct RecoveryUndoResult {
    /// Number of dangling txns whose undo order fell back to global LSN sort (chain incomplete).
    std::uint64_t chain_fallback_txns{0};
};

/// Build undo apply order for ops of one txn: prefer `undo_prev_lsn` chain from max LSN;
/// on any break or unreachable op, fall back to `(record_lsn desc, op_seq desc)`.
/// `out_indices` are indices into `ops` in the order undo should be applied.
bool recovery_plan_undo_ops_order(const std::vector<TxnWalOp>& ops,
                                  std::vector<std::size_t>& out_indices,
                                  bool* used_chain_fallback);

/// Apply undo for all dangling txns (txn with max LSN first; within txn follow undo chain).
RecoveryUndoResult recovery_apply_dangling_undo(
    const std::unordered_map<std::uint64_t, std::vector<TxnWalOp>>& dangling_by_txn,
    const std::function<std::string(const std::string& table_name)>& resolve_data_file);
