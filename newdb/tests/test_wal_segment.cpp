#include <newdb/wal_manager.h>

#include <filesystem>
#include <chrono>
#include <gtest/gtest.h>

namespace fs = std::filesystem;

TEST(WalSegment, RotateAndRecoverAcrossSegments) {
    const auto uniq = static_cast<long long>(std::chrono::steady_clock::now().time_since_epoch().count());
    const fs::path base = fs::temp_directory_path() / ("wal_seg_" + std::to_string(uniq));
    std::error_code ec;
    fs::remove_all(base, ec);
    fs::create_directories(base);

    newdb::WalManager wal("segdb", base.string());
    wal.set_segment_max_bytes(700);
    ASSERT_TRUE(wal.open().ok);

    for (int i = 1; i <= 120; ++i) {
        newdb::Row r;
        r.id = i;
        r.attrs["name"] = "u" + std::to_string(i);
        ASSERT_TRUE(wal.append_record(static_cast<std::uint64_t>(i), newdb::WalOp::INSERT, "users", &r).ok);
        ASSERT_TRUE(wal.commit_transaction(static_cast<std::uint64_t>(i)).ok);
    }
    ASSERT_TRUE(wal.flush().ok);
    wal.close();

    std::size_t seg_count = 0;
    for (const auto& ent : fs::directory_iterator(base)) {
        if (!ent.is_regular_file()) continue;
        const std::string fn = ent.path().filename().string();
        if (fn == "segdb.wal" || fn.rfind("segdb.wal.", 0) == 0) {
            ++seg_count;
        }
    }
    EXPECT_GT(seg_count, 1u);

    newdb::HeapTable t;
    t.name = "users";
    newdb::TableSchema schema;
    schema.primary_key = "id";
    ASSERT_TRUE(wal.recover(&t, schema).ok);
    EXPECT_EQ(t.rows.size(), 120u);
    EXPECT_NE(t.find_by_id(1), nullptr);
    EXPECT_NE(t.find_by_id(120), nullptr);

    fs::remove_all(base, ec);
}
