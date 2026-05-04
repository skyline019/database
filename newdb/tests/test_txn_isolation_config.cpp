#include <gtest/gtest.h>

#include "cli/modules/txn/coordinator/txn_manager.h"

// Guards isolation/file-lock API semantics documented in docs/txn/TXN_ISOLATION_AND_LOCKING.md
TEST(TxnIsolationConfig, DefaultIsolationIsSnapshot) {
    TxnCoordinator c;
    EXPECT_EQ(c.txnIsolationLevel(), TxnIsolationLevel::Snapshot);
}

TEST(TxnIsolationConfig, SetIsolationRoundTrip) {
    TxnCoordinator c;
    c.setTxnIsolationLevel(TxnIsolationLevel::ReadCommitted);
    EXPECT_EQ(c.txnIsolationLevel(), TxnIsolationLevel::ReadCommitted);
    c.setTxnIsolationLevel(TxnIsolationLevel::Snapshot);
    EXPECT_EQ(c.txnIsolationLevel(), TxnIsolationLevel::Snapshot);
}

TEST(TxnIsolationConfig, IsLockedFalseWhenNotHeld) {
    TxnCoordinator c;
    EXPECT_FALSE(c.isLocked("/no/such/table.bin"));
}
