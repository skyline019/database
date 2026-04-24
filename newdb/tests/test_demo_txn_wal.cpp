#include "txn_manager.h"

#include <newdb/page_io.h>
#include <newdb/wal_manager.h>

#include "test_util.h"

#include <gtest/gtest.h>

TEST(DemoTxnWal, RecoveryIsIdempotentAcrossRestarts) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_txn_wal");
    std::filesystem::create_directories(dir);
    const std::string bin = (dir / "users.bin").string();
    ASSERT_TRUE(newdb::io::create_heap_file(bin.c_str(), {newdb::Row{1, {{"n", "a"}}, {}}}).ok);

    TxnCoordinator tx;
    tx.set_workspace_root(dir.string());
    ASSERT_TRUE(tx.begin("users").isOk());
    tx.recordOperation("INSERT", "users", "2", "", "");
    ASSERT_TRUE(newdb::io::append_row(bin.c_str(), newdb::Row{2, {{"n", "b"}}, {}}).ok);
    tx.flushWAL();

    ASSERT_TRUE(tx.recoverFromWAL());
    {
        newdb::HeapTable tbl;
        newdb::TableSchema sch;
        sch.primary_key = "id";
        ASSERT_TRUE(newdb::io::load_heap_file(bin.c_str(), "users", sch, tbl).ok);
        EXPECT_EQ(tbl.find_by_id(2), nullptr);
    }

    newdb::WalManager wm("demodb", dir.string());
    ASSERT_TRUE(wm.open().ok);
    std::vector<newdb::WalDecodedRecord> recs1;
    ASSERT_TRUE(wm.read_all_records(newdb::TableSchema{}, recs1).ok);
    std::size_t r1 = 0;
    for (const auto& r : recs1) {
        if (r.op == newdb::WalOp::ROLLBACK) {
            ++r1;
        }
    }

    ASSERT_TRUE(tx.recoverFromWAL());
    std::vector<newdb::WalDecodedRecord> recs2;
    ASSERT_TRUE(wm.read_all_records(newdb::TableSchema{}, recs2).ok);
    std::size_t r2 = 0;
    for (const auto& r : recs2) {
        if (r.op == newdb::WalOp::ROLLBACK) {
            ++r2;
        }
    }

    EXPECT_EQ(r2, r1);
}

