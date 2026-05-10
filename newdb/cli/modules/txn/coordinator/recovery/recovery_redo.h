#pragma once

#include "cli/modules/txn/coordinator/recovery/recovery_wal_op.h"

#include <cstdint>
#include <functional>
#include <map>
#include <string>
#include <vector>

struct RecoveryRedoResult {
    std::uint64_t apply_conflicts{0};
};

bool recovery_strict_redo_from_env();

/// Replay committed txn ops to heap files (LSN ascending per table).
RecoveryRedoResult recovery_apply_committed_redo(
    const std::map<std::string, std::vector<TxnWalOp>>& redo_by_table,
    const std::function<std::string(const std::string& table_name)>& resolve_data_file,
    bool strict_redo);
