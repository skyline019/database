#include <newdb/wal/wal_segment_scanner.h>

#include <filesystem>
#include <fstream>

#include <gtest/gtest.h>

namespace fs = std::filesystem;

TEST(WalSegmentScanner, InventoryIncludesSizes) {
    const fs::path dir = fs::temp_directory_path() / "newdb_wal_seg_inventory_test";
    fs::remove_all(dir);
    fs::create_directories(dir);
    {
        std::ofstream a(dir / "x.wal");
        a << "hello";
    }
    const auto inv = newdb::list_wal_segment_inventory(dir.string());
    ASSERT_EQ(inv.size(), 1u);
    EXPECT_EQ(inv[0].size_bytes, 5u);
    fs::remove_all(dir);
}

TEST(WalSegmentScanner, ListsSortedWalFiles) {
    const fs::path dir = fs::temp_directory_path() / "newdb_wal_seg_scanner_test";
    fs::remove_all(dir);
    fs::create_directories(dir);
    {
        std::ofstream a(dir / "b.wal");
        a << 'b';
    }
    {
        std::ofstream z(dir / "a.wal");
        z << 'a';
    }
    const std::vector<std::string> paths = newdb::list_wal_segment_paths(dir.string());
    ASSERT_EQ(paths.size(), 2u);
    EXPECT_LT(paths[0], paths[1]);
    fs::remove_all(dir);
}

TEST(WalSegmentScanner, EmptyDirectoryReturnsEmptyLists) {
    const fs::path dir = fs::temp_directory_path() / "newdb_wal_seg_empty";
    fs::remove_all(dir);
    fs::create_directories(dir);
    EXPECT_TRUE(newdb::list_wal_segment_paths(dir.string()).empty());
    EXPECT_TRUE(newdb::list_wal_segment_inventory(dir.string()).empty());
    fs::remove_all(dir);
}

TEST(WalSegmentScanner, NonWalFilesIgnored) {
    const fs::path dir = fs::temp_directory_path() / "newdb_wal_seg_nonwal";
    fs::remove_all(dir);
    fs::create_directories(dir);
    {
        std::ofstream junk(dir / "notes.txt");
        junk << "not a wal";
    }
    {
        std::ofstream w(dir / "seg.wal");
        w << "x";
    }
    const auto paths = newdb::list_wal_segment_paths(dir.string());
    ASSERT_EQ(paths.size(), 1u);
    EXPECT_NE(paths[0].find("seg.wal"), std::string::npos);
    const auto inv = newdb::list_wal_segment_inventory(dir.string());
    ASSERT_EQ(inv.size(), 1u);
    EXPECT_EQ(inv[0].size_bytes, 1u);
    fs::remove_all(dir);
}

TEST(WalSegmentScanner, LexicographicOrderIsNotNumericOrder) {
    const fs::path dir = fs::temp_directory_path() / "newdb_wal_seg_lex";
    fs::remove_all(dir);
    fs::create_directories(dir);
    {
        std::ofstream a(dir / "1.wal");
        a << '1';
    }
    {
        std::ofstream b(dir / "10.wal");
        b << "xx";
    }
    {
        std::ofstream c(dir / "2.wal");
        c << '2';
    }
    const auto paths = newdb::list_wal_segment_paths(dir.string());
    ASSERT_EQ(paths.size(), 3u);
    EXPECT_LT(paths[0], paths[1]);
    EXPECT_LT(paths[1], paths[2]);
    EXPECT_NE(paths[0].find("1.wal"), std::string::npos);
    EXPECT_NE(paths[1].find("10.wal"), std::string::npos);
    EXPECT_NE(paths[2].find("2.wal"), std::string::npos);
    fs::remove_all(dir);
}

TEST(WalSegmentScanner, ManyWalFilesAreSortedDeterministically) {
    const fs::path dir = fs::temp_directory_path() / "newdb_wal_seg_many";
    fs::remove_all(dir);
    fs::create_directories(dir);
    for (int i = 0; i < 12; ++i) {
        const std::string name = std::string(1, static_cast<char>('a' + i)) + ".wal";
        std::ofstream f(dir / name);
        f << static_cast<char>('0' + (i % 10));
    }
    const auto paths = newdb::list_wal_segment_paths(dir.string());
    ASSERT_EQ(paths.size(), 12u);
    for (std::size_t i = 1; i < paths.size(); ++i) {
        EXPECT_LT(paths[i - 1], paths[i]) << paths[i - 1] << " vs " << paths[i];
    }
    const auto inv = newdb::list_wal_segment_inventory(dir.string());
    ASSERT_EQ(inv.size(), paths.size());
    for (std::size_t i = 0; i < paths.size(); ++i) {
        EXPECT_EQ(inv[i].path, paths[i]);
    }
    fs::remove_all(dir);
}

TEST(WalSegmentScanner, InventoryOrderMatchesPathOrder) {
    const fs::path dir = fs::temp_directory_path() / "newdb_wal_seg_inv_order";
    fs::remove_all(dir);
    fs::create_directories(dir);
    {
        std::ofstream a(dir / "m.wal");
        a << "mm";
    }
    {
        std::ofstream b(dir / "n.wal");
        b << "n";
    }
    const auto paths = newdb::list_wal_segment_paths(dir.string());
    const auto inv = newdb::list_wal_segment_inventory(dir.string());
    ASSERT_EQ(paths.size(), inv.size());
    for (std::size_t i = 0; i < paths.size(); ++i) {
        EXPECT_EQ(paths[i], inv[i].path);
    }
    fs::remove_all(dir);
}
