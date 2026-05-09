#pragma once

#include <newdb/heap_table.h>

#include "cli/modules/txn/coordinator/txn_manager.h"
#include "cli/shell/state/shell_state.h"

/// Ensures MVCC heap read snapshot is synced for WHERE/query paths (RAII).
struct HeapReadViewGuard {
    ShellState& st;
    newdb::HeapTable& tbl;
    HeapReadViewGuard(ShellState& s, newdb::HeapTable& t) : st(s), tbl(t) {
        st.txn().syncHeapReadSnapshotForQuery(tbl);
    }
    ~HeapReadViewGuard() { tbl.clear_snapshot(); }
};
