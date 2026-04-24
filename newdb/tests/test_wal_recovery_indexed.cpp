#include <gtest/gtest.h>

#include <newdb/wal_manager.h>
#include <newdb/heap_table.h>

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
}  // namespace

TEST(WalRecoveryIndexed, CollectsSegmentIndexStats) {
    namespace fs = std::filesystem;
    const fs::path dir = unique_temp_subdir("wal_recovery_idx");
    const std::string db = "recovery_idx";

    newdb::WalManager wal(db, dir.string());
    wal.set_segment_max_bytes(512);
    ASSERT_TRUE(wal.open().ok);

    newdb::Row r;
    r.id = 1;
    r.attrs["name"] = "u1";
    r.attrs["dept"] = "ENG";
    r.attrs["age"] = "30";
    r.attrs["salary"] = "100";
    for (int i = 0; i < 40; ++i) {
        r.id = i + 1;
        ASSERT_TRUE(wal.begin_transaction(static_cast<std::uint64_t>(i + 1)).ok);
        ASSERT_TRUE(wal.append_record(static_cast<std::uint64_t>(i + 1), newdb::WalOp::INSERT, "users", &r).ok);
        ASSERT_TRUE(wal.commit_transaction(static_cast<std::uint64_t>(i + 1)).ok);
    }
    ASSERT_TRUE(wal.flush().ok);

    newdb::HeapTable table;
    table.name = "users";
    newdb::TableSchema schema;
    newdb::WalRecoveryStats stats;
    ASSERT_TRUE(wal.recover(&table, schema, &stats).ok);
    EXPECT_GT(stats.indexed_segments, 0u);
    EXPECT_GT(stats.indexed_records, 0u);
    EXPECT_GT(stats.indexed_offsets, 0u);
    EXPECT_GT(stats.scanned_segments, 0u);

    const auto last = wal.last_recovery_stats();
    ASSERT_TRUE(last.has_value());
    EXPECT_EQ(last->indexed_segments, stats.indexed_segments);

    wal.close();
    std::error_code ec;
    fs::remove_all(dir, ec);
}
