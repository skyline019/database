#include "cli/modules/txn/coordinator/txn_manager.h"

#include <newdb/page_io.h>

#include "test_util.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <string>

namespace fs = std::filesystem;

/// rollbackToSavepoint applies heap undo via append_undo_row_to_heap; on failure it increments
/// undo_chain_fallback_count but still returns Ok (see TXN_ISOLATION_AND_LOCKING.md §4.2).
TEST(TxnUndoMetrics, InvalidRecordKeyIncrementsUndoChainFallbackCount) {
    const auto dir = newdb::test::unique_temp_subdir("txn_undo_fallback");
    fs::create_directories(dir);
    const std::string bin = (dir / "users.bin").string();
    ASSERT_TRUE(newdb::io::create_heap_file(bin.c_str(), {newdb::Row{1, {{"n", "a"}}, {}}}).ok);

    TxnCoordinator tx;
    tx.set_workspace_root(dir.string());
    ASSERT_TRUE(tx.begin("users").isOk());
    ASSERT_EQ(tx.runtimeStats().undo_chain_fallback_count, 0u);

    ASSERT_TRUE(tx.savepoint("sp").isOk());
    // Non-integer key causes std::stoi to throw inside append_undo_row_to_heap -> false -> fallback.
    tx.recordOperation("UPDATE", "users", "not_an_int", "n=a", "n=b");

    ASSERT_TRUE(tx.rollbackToSavepoint("sp").isOk());
    EXPECT_GE(tx.runtimeStats().undo_chain_fallback_count, 1u);
    EXPECT_EQ(tx.runtimeStats().rollback_savepoint_count, 1u);

    ASSERT_TRUE(tx.rollback().isOk());
}
