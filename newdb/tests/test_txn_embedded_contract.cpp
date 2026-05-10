#include "cli/modules/txn/coordinator/txn_manager.h"

#include <newdb/page_io.h>
#include <newdb/schema_io.h>
#include <newdb/session.h>

#include "test_util.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <string>

namespace fs = std::filesystem;

namespace {

std::string attr_for(const std::string& bin) {
    return newdb::schema_sidecar_path_for_data_file(bin);
}

} // namespace

/// Embedded callers that invoke TxnCoordinator::rollbackToSavepoint without going through
/// process_command_line must refresh Session / HeapTable; see txn_manager.h on rollbackToSavepoint.
TEST(TxnEmbeddedContract, RollbackToSavepointWithoutSessionInvalidateLeavesStaleHeap) {
    const auto dir = newdb::test::unique_temp_subdir("txn_emb_stale");
    fs::create_directories(dir);
    const std::string bin = (dir / "users.bin").string();

    newdb::TableSchema sch;
    sch.primary_key = "id";
    newdb::AttrMeta n{};
    n.name = "n";
    n.type = newdb::AttrType::String;
    sch.attrs.push_back(n);
    ASSERT_TRUE(newdb::save_schema_text(attr_for(bin), sch).ok);
    ASSERT_TRUE(newdb::io::create_heap_file(bin.c_str(), {newdb::Row{1, {{"n", "a"}}, {}}}).ok);

    newdb::Session session;
    session.data_path = bin;
    session.table_name = "users";
    session.schema = sch;
    ASSERT_TRUE(session.ensure_loaded().ok);
    ASSERT_EQ(session.table.rows.size(), 1u);
    EXPECT_EQ(session.table.rows[0].attrs["n"], "a");

    TxnCoordinator tx;
    tx.set_workspace_root(dir.string());
    ASSERT_TRUE(tx.begin("users").isOk());
    ASSERT_TRUE(tx.savepoint("sp").isOk());

    // Simulate embedded path: in-memory row advanced while coordinator + disk follow WAL protocol.
    session.table.rows[0].attrs["n"] = "b";
    ASSERT_TRUE(newdb::io::append_row(bin.c_str(), newdb::Row{1, {{"n", "b"}}, {}}).ok);
    tx.recordOperation("UPDATE", "users", "1", "n=a", "n=b");

    ASSERT_TRUE(tx.rollbackToSavepoint("sp").isOk());
    EXPECT_EQ(tx.runtimeStats().rollback_savepoint_count, 1u);

    newdb::HeapTable disk_after;
    ASSERT_TRUE(newdb::io::load_heap_file(bin.c_str(), "users", sch, disk_after).ok);
    const newdb::Row* dr = disk_after.find_by_id(1);
    ASSERT_NE(dr, nullptr);
    EXPECT_EQ(dr->attrs.at("n"), "a") << "disk should reflect undo append";

    // Stale: session still shows optimistic "b" while disk has "a".
    ASSERT_EQ(session.table.rows.size(), 1u);
    EXPECT_EQ(session.table.rows[0].attrs["n"], "b");
    EXPECT_NE(session.table.rows[0].attrs["n"], dr->attrs.at("n"));

    ASSERT_TRUE(tx.rollback().isOk());
}

TEST(TxnEmbeddedContract, InvalidateAndReloadAlignsHeapWithDiskAfterSavepointRollback) {
    const auto dir = newdb::test::unique_temp_subdir("txn_emb_reload");
    fs::create_directories(dir);
    const std::string bin = (dir / "users.bin").string();

    newdb::TableSchema sch;
    sch.primary_key = "id";
    newdb::AttrMeta n{};
    n.name = "n";
    n.type = newdb::AttrType::String;
    sch.attrs.push_back(n);
    ASSERT_TRUE(newdb::save_schema_text(attr_for(bin), sch).ok);
    ASSERT_TRUE(newdb::io::create_heap_file(bin.c_str(), {newdb::Row{1, {{"n", "a"}}, {}}}).ok);

    newdb::Session session;
    session.data_path = bin;
    session.table_name = "users";
    session.schema = sch;
    ASSERT_TRUE(session.ensure_loaded().ok);

    TxnCoordinator tx;
    tx.set_workspace_root(dir.string());
    ASSERT_TRUE(tx.begin("users").isOk());
    ASSERT_TRUE(tx.savepoint("sp").isOk());

    session.table.rows[0].attrs["n"] = "b";
    ASSERT_TRUE(newdb::io::append_row(bin.c_str(), newdb::Row{1, {{"n", "b"}}, {}}).ok);
    tx.recordOperation("UPDATE", "users", "1", "n=a", "n=b");

    ASSERT_TRUE(tx.rollbackToSavepoint("sp").isOk());

    session.invalidate();
    ASSERT_TRUE(session.ensure_loaded().ok);
    ASSERT_EQ(session.table.rows.size(), 1u);
    EXPECT_EQ(session.table.rows[0].attrs["n"], "a");

    newdb::HeapTable disk_after;
    ASSERT_TRUE(newdb::io::load_heap_file(bin.c_str(), "users", sch, disk_after).ok);
    const newdb::Row* dr = disk_after.find_by_id(1);
    ASSERT_NE(dr, nullptr);
    EXPECT_EQ(session.table.rows[0].attrs["n"], dr->attrs.at("n"));

    ASSERT_TRUE(tx.rollback().isOk());
}
