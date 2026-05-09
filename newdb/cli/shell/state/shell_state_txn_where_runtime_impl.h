#pragma once

#include "cli/modules/txn/coordinator/txn_manager.h"
#include "cli/modules/where/executor/where.h"

/// Opaque (to shell_state.h) txn + WHERE bundle for one interactive session.
/// Option B storage + option A-style nested grouping of the two sub-objects.
struct ShellTxnWhereRuntime {
    struct TxnWhereMembers {
        TxnCoordinator txn;
        WhereQueryContext where_ctx;
    } bundle;
};
