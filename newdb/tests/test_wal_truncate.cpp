#include <newdb/wal_manager.h>

#include <cstdlib>
#include <filesystem>
#include <gtest/gtest.h>

namespace fs = std::filesystem;

TEST(WalManager, CheckpointTruncatesFileKeepsLsnMonotonic) {
    const fs::path base = fs::temp_directory_path() / ("newdb_wal_truncate_" + std::to_string(std::rand()));
    fs::create_directories(base);
    const std::string dir = base.string();
    newdb::WalManager w("truncate_t", dir);
    ASSERT_TRUE(w.open().ok);
    for (int i = 0; i < 4; ++i) {
        ASSERT_TRUE(w.append_record(1, newdb::WalOp::COMMIT, "", nullptr).ok);
    }
    const std::uint64_t before_sz = w.wal_file_size_bytes();
    const std::uint64_t lsn_before = w.current_lsn();
    ASSERT_GT(before_sz, 0u);
    ASSERT_GT(lsn_before, 0u);
    ASSERT_TRUE(w.checkpoint_and_truncate(lsn_before).ok);
    EXPECT_EQ(w.wal_file_size_bytes(), 0u);
    EXPECT_EQ(w.current_lsn(), lsn_before);
    ASSERT_TRUE(w.append_record(1, newdb::WalOp::COMMIT, "", nullptr).ok);
    EXPECT_GT(w.current_lsn(), lsn_before);
    w.close();
    fs::remove_all(base);
}

TEST(WalManager, OpenResyncsLsnFromDisk) {
    const fs::path base = fs::temp_directory_path() / ("newdb_wal_resync_" + std::to_string(std::rand()));
    fs::create_directories(base);
    const std::string dir = base.string();
    {
        newdb::WalManager w("resync_t", dir);
        ASSERT_TRUE(w.open().ok);
        ASSERT_TRUE(w.append_record(7, newdb::WalOp::COMMIT, "", nullptr).ok);
        ASSERT_GE(w.current_lsn(), 1u);
        w.close();
    }
    {
        newdb::WalManager w2("resync_t", dir);
        ASSERT_TRUE(w2.open().ok);
        EXPECT_GE(w2.current_lsn(), 1u);
        const std::uint64_t prev = w2.current_lsn();
        ASSERT_TRUE(w2.append_record(1, newdb::WalOp::COMMIT, "", nullptr).ok);
        EXPECT_GT(w2.current_lsn(), prev);
    }
    fs::remove_all(base);
}

TEST(WalManager, CheckpointWritesBeginAndEndMarkers) {
    const fs::path base = fs::temp_directory_path() / ("newdb_wal_checkpoint_markers_" + std::to_string(std::rand()));
    fs::create_directories(base);
    const std::string dir = base.string();
    newdb::WalManager w("checkpoint_markers_t", dir);
    ASSERT_TRUE(w.open().ok);
    ASSERT_TRUE(w.checkpoint(0).ok);
    ASSERT_TRUE(w.flush().ok);

    std::vector<newdb::WalDecodedRecord> recs;
    ASSERT_TRUE(w.read_all_records(newdb::TableSchema{}, recs).ok);
    std::size_t begin_count = 0;
    std::size_t end_count = 0;
    for (const auto& r : recs) {
        if (r.op == newdb::WalOp::CHECKPOINT_BEGIN) ++begin_count;
        if (r.op == newdb::WalOp::CHECKPOINT_END) ++end_count;
    }
    EXPECT_EQ(begin_count, 1u);
    EXPECT_EQ(end_count, 1u);
    w.close();
    fs::remove_all(base);
}
