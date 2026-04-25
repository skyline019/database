#include <gtest/gtest.h>

#include "txn_manager.h"

#include <filesystem>
#include <chrono>

namespace {
std::filesystem::path unique_temp_subdir(const char* tag) {
    namespace fs = std::filesystem;
    std::error_code ec;
    fs::path base = fs::temp_directory_path(ec);
    if (ec) base = fs::current_path(ec);
    const auto now = std::to_string(
        static_cast<long long>(std::chrono::steady_clock::now().time_since_epoch().count()));
    fs::path dir = base / (std::string("newdb_") + tag + "_" + now);
    fs::create_directories(dir, ec);
    return dir;
}
} // namespace

TEST(TxnWriteConflict, SameTableSameIdRejectedAcrossActiveTransactions) {
    namespace fs = std::filesystem;
    const fs::path ws = unique_temp_subdir("txn_conflict");

    TxnCoordinator a;
    a.set_workspace_root(ws.string());
    TxnCoordinator b;
    b.set_workspace_root(ws.string());

    ASSERT_TRUE(a.begin("ta").isOk());
    ASSERT_TRUE(b.begin("tb").isOk());

    std::string reason;
    EXPECT_TRUE(a.tryReserveWriteKey("users", 7, &reason));
    EXPECT_FALSE(b.tryReserveWriteKey("users", 7, &reason));
    EXPECT_NE(reason.find("write conflict"), std::string::npos);
    EXPECT_GE(b.writeConflictCount(), 1u);

    ASSERT_TRUE(a.commit().isOk());
    EXPECT_TRUE(b.tryReserveWriteKey("users", 7, &reason));

    ASSERT_TRUE(b.commit().isOk());

    std::error_code ec;
    fs::remove_all(ws, ec);
}

TEST(TxnWriteConflict, SameTableConcurrentBeginRejectedByFileLock) {
    namespace fs = std::filesystem;
    const fs::path ws = unique_temp_subdir("txn_conflict_same_table_lock");

    TxnCoordinator a;
    a.set_workspace_root(ws.string());
    TxnCoordinator b;
    b.set_workspace_root(ws.string());

    ASSERT_TRUE(a.begin("users").isOk());
    EXPECT_FALSE(b.begin("users").isOk());

    ASSERT_TRUE(a.commit().isOk());

    std::error_code ec;
    fs::remove_all(ws, ec);
}

TEST(TxnWriteConflict, DifferentTableSameIdAllowed) {
    namespace fs = std::filesystem;
    const fs::path ws = unique_temp_subdir("txn_conflict_diff_table");

    TxnCoordinator a;
    a.set_workspace_root(ws.string());
    TxnCoordinator b;
    b.set_workspace_root(ws.string());

    ASSERT_TRUE(a.begin("users").isOk());
    ASSERT_TRUE(b.begin("orders").isOk());

    std::string reason;
    EXPECT_TRUE(a.tryReserveWriteKey("users", 7, &reason));
    EXPECT_TRUE(b.tryReserveWriteKey("orders", 7, &reason));
    EXPECT_EQ(b.writeConflictCount(), 0u);

    ASSERT_TRUE(a.commit().isOk());
    ASSERT_TRUE(b.commit().isOk());

    std::error_code ec;
    fs::remove_all(ws, ec);
}

TEST(TxnWriteConflict, RollbackReleasesWriteIntent) {
    namespace fs = std::filesystem;
    const fs::path ws = unique_temp_subdir("txn_conflict_rollback_release");

    TxnCoordinator a;
    a.set_workspace_root(ws.string());

    ASSERT_TRUE(a.begin("users").isOk());
    std::string reason;
    EXPECT_TRUE(a.tryReserveWriteKey("users", 42, &reason));
    ASSERT_TRUE(a.rollback().isOk());

    TxnCoordinator b2;
    b2.set_workspace_root(ws.string());
    ASSERT_TRUE(b2.begin("users").isOk());
    EXPECT_TRUE(b2.tryReserveWriteKey("users", 42, &reason));
    ASSERT_TRUE(b2.commit().isOk());

    std::error_code ec;
    fs::remove_all(ws, ec);
}

