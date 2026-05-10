#pragma once

#include "cli/modules/txn/coordinator/txn_coordinator_types.h"

#include <newdb/row.h>

#include <map>
#include <string>

namespace cli_txn_heap_undo {

/// Parse `k=v;k2=v2` packed attrs used by in-memory txn records and legacy WAL recovery.
std::map<std::string, std::string> parse_packed_attrs(const std::string& packed);

struct TxnWalUndoView {
    const TxnRecord* rec{nullptr};
    bool has_before{false};
    const newdb::Row* before_row{nullptr};
};

/// Undo one WAL-visible op onto heap (append_row). Used by crash recovery and mirrors runtime rollback.
bool append_undo_row_to_heap(const char* data_file_path, const TxnWalUndoView& op);

}  // namespace cli_txn_heap_undo
