#pragma once

#include <cstdint>
#include <functional>
#include <vector>

/// Callbacks for post-undo WAL finalize (mock-friendly).
struct RecoveryFinalizeCallbacks {
    /// Set coordinator txn id before each synthetic `TXN_ROLLBACK` append.
    std::function<void(std::uint64_t txn_id)> set_active_txn_id_for_wal;
    std::function<void()> write_txn_rollback_recovered;
    std::function<void()> write_recovery_done;
    std::function<void()> flush_wal;
};

/// Append `TXN_ROLLBACK` per dangling txn, then `RECOVERY_DONE` + flush.
void recovery_finalize_dangling_txns(const std::vector<std::uint64_t>& dangling_txn_ids,
                                     const RecoveryFinalizeCallbacks& cb);
