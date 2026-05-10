#pragma once

#include "cli/modules/txn/coordinator/recovery/recovery_wal_op.h"

#include <newdb/wal_manager.h>

#include <cstdint>
#include <map>
#include <unordered_map>
#include <vector>

struct RecoveryAnalyzeOutput {
    std::unordered_map<std::uint64_t, std::vector<TxnWalOp>> committed_by_txn;
    std::unordered_map<std::uint64_t, std::vector<TxnWalOp>> dangling_by_txn;
    std::uint64_t checkpoint_begin{0};
    std::uint64_t checkpoint_end{0};
};

/// Classify decoded WAL into committed vs dangling txn op lists (no heap I/O).
/// `recover_target_lsn`: records with `lsn > recover_target_lsn` are ignored (0 = no cutoff).
void recovery_analyze_wal_records(const std::vector<newdb::WalDecodedRecord>& recs,
                                   std::uint64_t recover_target_lsn,
                                   RecoveryAnalyzeOutput& out);

std::size_t recovery_count_dangling_ops(const RecoveryAnalyzeOutput& out);
