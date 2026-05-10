#include "cli/modules/txn/coordinator/recovery/recovery_finalize.h"

void recovery_finalize_dangling_txns(const std::vector<std::uint64_t>& dangling_txn_ids,
                                     const RecoveryFinalizeCallbacks& cb) {
    for (std::uint64_t txn : dangling_txn_ids) {
        if (cb.set_active_txn_id_for_wal) {
            cb.set_active_txn_id_for_wal(txn);
        }
        if (cb.write_txn_rollback_recovered) {
            cb.write_txn_rollback_recovered();
        }
    }
    if (cb.write_recovery_done) {
        cb.write_recovery_done();
    }
    if (cb.flush_wal) {
        cb.flush_wal();
    }
}
