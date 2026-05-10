#include "cli/modules/txn/coordinator/recovery/recovery_undo.h"
#include "cli/modules/txn/coordinator/recovery/recovery_wal_op.h"

#include <gtest/gtest.h>

#include <vector>

namespace {

TxnWalOp make_op(std::uint64_t lsn, std::uint64_t op_seq, bool has_prev, std::uint64_t prev_lsn) {
    TxnWalOp o;
    o.record_lsn = lsn;
    o.op_seq = op_seq;
    o.has_undo_prev_lsn = has_prev;
    o.undo_prev_lsn = prev_lsn;
    o.rec.operation = "UPDATE";
    o.rec.key = "1";
    return o;
}

}  // namespace

TEST(RecoveryUndoChain, LinearChainUsesUndoPrevOrder) {
    std::vector<TxnWalOp> ops;
    ops.push_back(make_op(10, 1, false, 0));
    ops.push_back(make_op(20, 2, true, 10));
    ops.push_back(make_op(30, 3, true, 20));
    std::vector<std::size_t> ord;
    bool fb = true;
    ASSERT_TRUE(recovery_plan_undo_ops_order(ops, ord, &fb));
    EXPECT_FALSE(fb);
    ASSERT_EQ(ord.size(), 3u);
    EXPECT_EQ(ord[0], 2u);
    EXPECT_EQ(ord[1], 1u);
    EXPECT_EQ(ord[2], 0u);
}

TEST(RecoveryUndoChain, BrokenPrevFallsBackToLsnDescOrder) {
    std::vector<TxnWalOp> ops;
    ops.push_back(make_op(10, 1, false, 0));
    ops.push_back(make_op(20, 2, true, 10));
    // Missing LSN 15: chain from max (20) cannot reach 10
    ops.push_back(make_op(25, 3, true, 99));
    std::vector<std::size_t> ord;
    bool fb = false;
    ASSERT_TRUE(recovery_plan_undo_ops_order(ops, ord, &fb));
    EXPECT_TRUE(fb);
    ASSERT_EQ(ord.size(), 3u);
    EXPECT_EQ(ord[0], 2u);
    EXPECT_EQ(ord[1], 1u);
    EXPECT_EQ(ord[2], 0u);
}

TEST(RecoveryUndoChain, DuplicateRecordLsnUsesOpSeqToFollowPrev) {
    std::vector<TxnWalOp> ops;
    ops.push_back(make_op(10, 1, false, 0));
    ops.push_back(make_op(10, 2, true, 10));
    ops.push_back(make_op(30, 3, true, 10));
    std::vector<std::size_t> ord;
    bool fb = true;
    ASSERT_TRUE(recovery_plan_undo_ops_order(ops, ord, &fb));
    EXPECT_FALSE(fb);
    ASSERT_EQ(ord.size(), 3u);
    EXPECT_EQ(ord[0], 2u);
    EXPECT_EQ(ord[1], 1u);
    EXPECT_EQ(ord[2], 0u);
}

TEST(RecoveryUndoChain, CycleOnChainFallsBack) {
    std::vector<TxnWalOp> ops;
    ops.push_back(make_op(10, 1, true, 20));
    ops.push_back(make_op(20, 2, true, 10));
    std::vector<std::size_t> ord;
    bool fb = false;
    ASSERT_TRUE(recovery_plan_undo_ops_order(ops, ord, &fb));
    EXPECT_TRUE(fb);
    ASSERT_EQ(ord.size(), 2u);
    EXPECT_EQ(ord[0], 1u);
    EXPECT_EQ(ord[1], 0u);
}
