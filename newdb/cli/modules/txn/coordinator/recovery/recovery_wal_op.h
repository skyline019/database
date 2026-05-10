#pragma once

#include "cli/modules/txn/coordinator/txn_coordinator_types.h"

#include <newdb/row.h>
#include <newdb/wal_manager.h>

#include <cstdint>

/// One heap DML operation as seen during WAL recovery (CLI coordinator).
struct TxnWalOp {
    TxnRecord rec;
    std::uint64_t op_seq{0};
    std::uint64_t record_lsn{0};
    std::uint64_t undo_prev_lsn{0};
    bool has_undo_prev_lsn{false};
    std::uint64_t db_object_id{0};
    bool has_before{false};
    newdb::Row before_row;
    bool has_after{false};
    newdb::Row after_row;
    newdb::WalOp wal_op{newdb::WalOp::UPDATE};
};
