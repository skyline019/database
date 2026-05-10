#include "cli/modules/txn/coordinator/txn_manager.h"

#include <newdb/page_io.h>

#include "test_util.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <string>

namespace fs = std::filesystem;

/// Strict coordinator API contracts (no CLI): illegal state transitions must fail predictably.
TEST(TxnChainStrict, CommitWithoutActiveTransactionFails) {
    TxnCoordinator tx;
    const auto dir = newdb::test::unique_temp_subdir("txn_strict_commit");
    fs::create_directories(dir);
    tx.set_workspace_root(dir.string());
    const auto r = tx.commit();
    ASSERT_TRUE(r.isErr());
    EXPECT_NE(r.error().find("no active transaction"), std::string::npos);
    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST(TxnChainStrict, RollbackWithoutActiveTransactionFails) {
    TxnCoordinator tx;
    const auto dir = newdb::test::unique_temp_subdir("txn_strict_rb");
    fs::create_directories(dir);
    tx.set_workspace_root(dir.string());
    const auto r = tx.rollback();
    ASSERT_TRUE(r.isErr());
    EXPECT_NE(r.error().find("no active transaction"), std::string::npos);
    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST(TxnChainStrict, NestedBeginRejected) {
    const auto dir = newdb::test::unique_temp_subdir("txn_strict_nested");
    fs::create_directories(dir);
    const std::string bin = (dir / "t.bin").string();
    ASSERT_TRUE(newdb::io::create_heap_file(bin.c_str(), {newdb::Row{1, {}, {}}}).ok);

    TxnCoordinator tx;
    tx.set_workspace_root(dir.string());
    ASSERT_TRUE(tx.begin("t").isOk());
    const auto r2 = tx.begin("t");
    ASSERT_TRUE(r2.isErr());
    EXPECT_NE(r2.error().find("nested"), std::string::npos);
    ASSERT_TRUE(tx.rollback().isOk());
    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST(TxnChainStrict, SavepointRequiresActiveTxn) {
    TxnCoordinator tx;
    const auto dir = newdb::test::unique_temp_subdir("txn_strict_sp");
    fs::create_directories(dir);
    tx.set_workspace_root(dir.string());
    const auto r = tx.savepoint("x");
    ASSERT_TRUE(r.isErr());
    EXPECT_NE(r.error().find("no active transaction"), std::string::npos);
    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST(TxnChainStrict, RollbackToSavepointRequiresActiveTxn) {
    TxnCoordinator tx;
    const auto dir = newdb::test::unique_temp_subdir("txn_strict_rbt");
    fs::create_directories(dir);
    tx.set_workspace_root(dir.string());
    const auto r = tx.rollbackToSavepoint("sp");
    ASSERT_TRUE(r.isErr());
    EXPECT_NE(r.error().find("no active transaction"), std::string::npos);
    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST(TxnChainStrict, NewTxnAfterRollbackSucceeds) {
    const auto dir = newdb::test::unique_temp_subdir("txn_strict_after_rb");
    fs::create_directories(dir);
    const std::string bin = (dir / "u.bin").string();
    ASSERT_TRUE(newdb::io::create_heap_file(bin.c_str(), {newdb::Row{1, {}, {}}}).ok);

    TxnCoordinator tx;
    tx.set_workspace_root(dir.string());
    ASSERT_TRUE(tx.begin("u").isOk());
    ASSERT_TRUE(tx.rollback().isOk());
    ASSERT_EQ(tx.getState(), TxnState::RolledBack);
    ASSERT_TRUE(tx.begin("u").isOk());
    ASSERT_EQ(tx.getState(), TxnState::Active);
    ASSERT_TRUE(tx.rollback().isOk());
    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST(TxnChainStrict, NewTxnAfterCommitSucceeds) {
    const auto dir = newdb::test::unique_temp_subdir("txn_strict_after_cmt");
    fs::create_directories(dir);
    const std::string bin = (dir / "v.bin").string();
    ASSERT_TRUE(newdb::io::create_heap_file(bin.c_str(), {newdb::Row{1, {}, {}}}).ok);

    TxnCoordinator tx;
    tx.set_workspace_root(dir.string());
    ASSERT_TRUE(tx.begin("v").isOk());
    ASSERT_TRUE(tx.commit().isOk());
    ASSERT_EQ(tx.getState(), TxnState::Committed);
    ASSERT_TRUE(tx.begin("v").isOk());
    ASSERT_TRUE(tx.rollback().isOk());
    std::error_code ec;
    fs::remove_all(dir, ec);
}
