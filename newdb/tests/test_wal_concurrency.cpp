#include <newdb/wal_manager.h>

#include <atomic>
#include <filesystem>
#include <gtest/gtest.h>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

TEST(WalConcurrency, ParallelAppendRecordsRemainReadable) {
    const fs::path base = fs::temp_directory_path() / ("wal_conc_" + std::to_string(std::rand()));
    fs::create_directories(base);

    newdb::WalManager wal("concurrency", base.string());
    ASSERT_TRUE(wal.open().ok);

    constexpr int kThreads = 12;
    constexpr int kOpsPerThread = 120;
    std::atomic<bool> ok{true};
    std::vector<std::thread> workers;
    workers.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        workers.emplace_back([&, t]() {
            for (int i = 0; i < kOpsPerThread; ++i) {
                const std::uint64_t txn = static_cast<std::uint64_t>(t * 100000 + i + 1);
                newdb::Row r;
                r.id = t * 10000 + i + 1;
                r.attrs["v"] = std::to_string(i);
                if (!wal.append_record(txn, newdb::WalOp::INSERT, "users", &r).ok) {
                    ok.store(false, std::memory_order_relaxed);
                    return;
                }
                if (!wal.commit_transaction(txn).ok) {
                    ok.store(false, std::memory_order_relaxed);
                    return;
                }
            }
        });
    }
    for (auto& th : workers) th.join();
    ASSERT_TRUE(ok.load(std::memory_order_relaxed));
    ASSERT_TRUE(wal.flush().ok);

    std::vector<newdb::WalDecodedRecord> records;
    newdb::TableSchema schema;
    ASSERT_TRUE(wal.read_all_records(schema, records).ok);

    std::size_t insert_count = 0;
    std::size_t commit_count = 0;
    for (const auto& rec : records) {
        if (rec.op == newdb::WalOp::INSERT) ++insert_count;
        if (rec.op == newdb::WalOp::COMMIT) ++commit_count;
    }
    EXPECT_EQ(insert_count, static_cast<std::size_t>(kThreads * kOpsPerThread));
    EXPECT_EQ(commit_count, static_cast<std::size_t>(kThreads * kOpsPerThread));

    wal.close();
    fs::remove_all(base);
}
