#include <gtest/gtest.h>

#include "cli/modules/txn/coordinator/txn_manager.h"

#include <newdb/page_io.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

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

TEST(TxnAutoVacuum, TriggersAfterCommitNotMidTransaction) {
    namespace fs = std::filesystem;
    const fs::path ws = unique_temp_subdir("txn_autovac");

    TxnCoordinator txn;
    txn.set_workspace_root(ws.string());
    txn.setVacuumOpsThreshold(2);

    std::mutex mu;
    std::condition_variable cv;
    std::atomic<int> callback_hits{0};
    txn.setVacuumCallback([&](const std::string&) {
        callback_hits.fetch_add(1, std::memory_order_relaxed);
        cv.notify_all();
    });
    txn.startVacuumThread();

    ASSERT_TRUE(txn.begin("users").isOk());
    txn.recordOperation("INSERT", "users", "1", "", "name=a");
    txn.recordOperation("UPDATE", "users", "1", "name=a", "name=b");

    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    EXPECT_EQ(callback_hits.load(std::memory_order_relaxed), 0);

    ASSERT_TRUE(txn.commit().isOk());

    {
        std::unique_lock<std::mutex> lk(mu);
        const bool fired = cv.wait_for(lk, std::chrono::seconds(2), [&]() {
            return callback_hits.load(std::memory_order_relaxed) >= 1;
        });
        EXPECT_TRUE(fired);
    }

    txn.stopVacuumThread();
    std::error_code ec;
    fs::remove_all(ws, ec);
}

TEST(TxnAutoVacuum, CooldownSuppressesFrequentTriggersSameTable) {
    namespace fs = std::filesystem;
    const fs::path ws = unique_temp_subdir("txn_autovac_cooldown");

    TxnCoordinator txn;
    txn.set_workspace_root(ws.string());
    txn.setVacuumOpsThreshold(1);
    txn.setVacuumMinIntervalSec(2);

    std::atomic<int> callback_hits{0};
    txn.setVacuumCallback([&](const std::string&) {
        callback_hits.fetch_add(1, std::memory_order_relaxed);
    });
    txn.startVacuumThread();

    ASSERT_TRUE(txn.begin("users").isOk());
    txn.recordOperation("INSERT", "users", "1", "", "name=a");
    ASSERT_TRUE(txn.commit().isOk());

    ASSERT_TRUE(txn.begin("users").isOk());
    txn.recordOperation("UPDATE", "users", "1", "name=a", "name=b");
    ASSERT_TRUE(txn.commit().isOk());

    std::this_thread::sleep_for(std::chrono::milliseconds(400));
    EXPECT_EQ(callback_hits.load(std::memory_order_relaxed), 1);
    EXPECT_EQ(txn.vacuumTriggerCount(), 1u);
    EXPECT_EQ(txn.vacuumExecuteCount(), 1u);
    EXPECT_GE(txn.vacuumCooldownSkipCount(), 1u);
    const TxnRuntimeStats s = txn.runtimeStats();
    EXPECT_EQ(s.vacuum_trigger_count, 1u);
    EXPECT_EQ(s.vacuum_execute_count, 1u);
    EXPECT_GE(s.vacuum_cooldown_skip_count, 1u);

    txn.stopVacuumThread();
    std::error_code ec;
    fs::remove_all(ws, ec);
}

TEST(TxnAutoVacuum, CooldownIsScopedPerTable) {
    namespace fs = std::filesystem;
    const fs::path ws = unique_temp_subdir("txn_autovac_table_scope");

    TxnCoordinator txn;
    txn.set_workspace_root(ws.string());
    txn.setVacuumOpsThreshold(1);
    txn.setVacuumMinIntervalSec(5);

    std::atomic<int> callback_hits{0};
    txn.setVacuumCallback([&](const std::string&) {
        callback_hits.fetch_add(1, std::memory_order_relaxed);
    });
    txn.startVacuumThread();

    ASSERT_TRUE(txn.begin("users").isOk());
    txn.recordOperation("INSERT", "users", "1", "", "name=a");
    ASSERT_TRUE(txn.commit().isOk());

    ASSERT_TRUE(txn.begin("orders").isOk());
    txn.recordOperation("INSERT", "orders", "1", "", "name=o");
    ASSERT_TRUE(txn.commit().isOk());

    std::this_thread::sleep_for(std::chrono::milliseconds(450));
    EXPECT_EQ(callback_hits.load(std::memory_order_relaxed), 2);
    EXPECT_EQ(txn.vacuumTriggerCount(), 2u);
    EXPECT_EQ(txn.vacuumExecuteCount(), 2u);
    EXPECT_EQ(txn.vacuumCooldownSkipCount(), 0u);

    txn.stopVacuumThread();
    std::error_code ec;
    fs::remove_all(ws, ec);
}

TEST(TxnAutoVacuum, DefaultVacuumCompactsHeapFileWithoutCallback) {
    namespace fs = std::filesystem;
    const fs::path ws = unique_temp_subdir("txn_autovac_default_compact");
    const fs::path data_file = ws / "users.bin";

    std::vector<newdb::Row> seed;
    seed.reserve(320);
    const std::string payload(512, 'x');
    for (int i = 0; i < 300; ++i) {
        seed.push_back(newdb::Row{1, {{"name", "v" + std::to_string(i) + payload}}, {}});
    }
    seed.push_back(newdb::Row{2, {{"name", "will_delete"}}, {}});
    seed.push_back(newdb::Row{2, {{"__deleted", "1"}, {"name", "will_delete"}}, {}});
    ASSERT_TRUE(newdb::io::create_heap_file(data_file.string().c_str(), seed).ok);

    const auto before_bytes = fs::file_size(data_file);

    TxnCoordinator txn;
    txn.set_workspace_root(ws.string());
    txn.setVacuumOpsThreshold(1);
    txn.setVacuumMinIntervalSec(0);
    txn.startVacuumThread();

    ASSERT_TRUE(txn.begin("users").isOk());
    txn.recordOperation("INSERT", "users", "999", "", "name=trigger");
    ASSERT_TRUE(txn.commit().isOk());

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (txn.vacuumExecuteCount() < 1u && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    EXPECT_GE(txn.vacuumExecuteCount(), 1u);
    EXPECT_GE(txn.vacuumCompactSuccessCount(), 1u);

    txn.stopVacuumThread();

    newdb::HeapTable tbl;
    newdb::TableSchema schema;
    schema.primary_key = "id";
    ASSERT_TRUE(newdb::io::load_heap_file(data_file.string().c_str(), "users", schema, tbl).ok);
    ASSERT_EQ(tbl.rows.size(), 1u);
    EXPECT_EQ(tbl.rows[0].id, 1);

    const auto after_bytes = fs::file_size(data_file);
    EXPECT_LT(after_bytes, before_bytes);
    EXPECT_GT(txn.vacuumCompactBytesReclaimed(), 0u);
    EXPECT_EQ(txn.vacuumCompactFailureCount(), 0u);
    EXPECT_GE(txn.vacuumCompactLastElapsedMs(), 0u);

    std::error_code ec;
    fs::remove_all(ws, ec);
}

