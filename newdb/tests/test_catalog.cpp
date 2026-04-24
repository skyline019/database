#include <newdb/catalog.h>

#include "test_util.h"

#include <gtest/gtest.h>

#include <algorithm>

namespace fs = std::filesystem;

TEST(Catalog, CreateListUseDrop) {
    const auto root = newdb::test::unique_temp_subdir("newdb_cat");
    fs::create_directories(root);

    newdb::Catalog cat(root.string());
    ASSERT_TRUE(cat.create_database("db1").ok);
    EXPECT_TRUE(cat.has_database("db1"));
    const auto dbs = cat.list_databases();
    EXPECT_NE(std::find(dbs.begin(), dbs.end(), "db1"), dbs.end());

    ASSERT_TRUE(cat.use_database("db1").ok);
    EXPECT_EQ(cat.current_database(), "db1");
    EXPECT_NE(cat.current_database_path().find("db1"), std::string::npos);

    EXPECT_FALSE(cat.drop_database("db1").ok);
    cat.clear_selection();
    ASSERT_TRUE(cat.drop_database("db1").ok);
    EXPECT_FALSE(cat.has_database("db1"));
}

TEST(Catalog, CreateEmptyNameFails) {
    const auto root = newdb::test::unique_temp_subdir("newdb_cat_empty");
    fs::create_directories(root);
    newdb::Catalog cat(root.string());
    EXPECT_FALSE(cat.create_database("").ok);
}

TEST(Catalog, UseAndDropMissingFails) {
    const auto root = newdb::test::unique_temp_subdir("newdb_cat_miss");
    fs::create_directories(root);
    newdb::Catalog cat(root.string());
    EXPECT_FALSE(cat.use_database("nope").ok);
    EXPECT_FALSE(cat.drop_database("nope").ok);
}

TEST(Catalog, CreateDuplicateFails) {
    const auto root = newdb::test::unique_temp_subdir("newdb_cat_dup");
    fs::create_directories(root);
    newdb::Catalog cat(root.string());
    ASSERT_TRUE(cat.create_database("x").ok);
    EXPECT_FALSE(cat.create_database("x").ok);
}
