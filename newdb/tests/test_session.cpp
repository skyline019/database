#include <newdb/page_io.h>
#include <newdb/schema_io.h>
#include <newdb/session.h>

#include "test_util.h"

#include <gtest/gtest.h>

using newdb::Row;
using newdb::TableSchema;

TEST(Session, ReloadLoadsSidecarAndHeap) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_session");
    std::filesystem::create_directories(dir);
    const std::string bin = (dir / "users.bin").string();
    const std::string attr = newdb::schema_sidecar_path_for_data_file(bin);

    TableSchema sch;
    sch.primary_key = "id";
    ASSERT_TRUE(newdb::save_schema_text(attr, sch).ok);
    ASSERT_TRUE(newdb::io::create_heap_file(bin.c_str(), {Row{5, {{"n", "a"}}, {}}}).ok);

    newdb::Session s;
    s.data_path = bin;
    s.table_name = "users";
    s.schema = sch;
    ASSERT_TRUE(s.reload().ok);
    EXPECT_TRUE(s.cache_valid);
    ASSERT_EQ(s.table.rows.size(), 1u);
    EXPECT_EQ(s.table.rows[0].id, 5);

    s.invalidate();
    EXPECT_FALSE(s.cache_valid);
    ASSERT_TRUE(s.ensure_loaded().ok);
    EXPECT_TRUE(s.cache_valid);
}

TEST(Session, ReloadFailsWhenBinMissing) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_sess_missbin");
    std::filesystem::create_directories(dir);
    const std::string bin = (dir / "ghost.bin").string();
    const std::string attr = newdb::schema_sidecar_path_for_data_file(bin);
    newdb::TableSchema sch;
    sch.primary_key = "id";
    ASSERT_TRUE(newdb::save_schema_text(attr, sch).ok);

    newdb::Session s;
    s.data_path = bin;
    s.table_name = "ghost";
    s.schema = sch;
    const auto st = s.reload();
    EXPECT_FALSE(st.ok);
    EXPECT_FALSE(s.cache_valid);
}

TEST(Session, EnsureLoadedUsesCache) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_sess_cache");
    std::filesystem::create_directories(dir);
    const std::string bin = (dir / "t.bin").string();
    const std::string attr = newdb::schema_sidecar_path_for_data_file(bin);
    newdb::TableSchema sch;
    sch.primary_key = "id";
    ASSERT_TRUE(newdb::save_schema_text(attr, sch).ok);
    ASSERT_TRUE(newdb::io::create_heap_file(bin.c_str(), {newdb::Row{1, {}, {}}}).ok);

    newdb::Session s;
    s.data_path = bin;
    s.table_name = "t";
    s.schema = sch;
    ASSERT_TRUE(s.ensure_loaded().ok);
    ASSERT_TRUE(s.cache_valid);
    ASSERT_TRUE(s.ensure_loaded().ok);
    EXPECT_TRUE(s.cache_valid);
}

TEST(Session, EnsureLoadedFailsWithoutPath) {
    newdb::Session s;
    const auto st = s.ensure_loaded();
    EXPECT_FALSE(st.ok);
}

TEST(Session, MutableHeapNullWhenLoadFails) {
    newdb::Session s;
    s.data_path.clear();
    EXPECT_EQ(s.mutable_heap(nullptr), nullptr);
    EXPECT_EQ(s.mutable_heap("any.log"), nullptr);
}

TEST(Session, LockHeapKeepsTableStableUntilReleased) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_sess_lockheap");
    std::filesystem::create_directories(dir);
    const std::string bin = (dir / "lh.bin").string();
    const std::string attr = newdb::schema_sidecar_path_for_data_file(bin);
    newdb::TableSchema sch;
    sch.primary_key = "id";
    ASSERT_TRUE(newdb::save_schema_text(attr, sch).ok);
    ASSERT_TRUE(newdb::io::create_heap_file(bin.c_str(), {newdb::Row{9, {}, {}}}).ok);

    newdb::Session s;
    s.data_path = bin;
    s.table_name = "lh";
    s.schema = sch;

    {
        newdb::Session::HeapAccess acc = s.lock_heap(nullptr);
        ASSERT_TRUE(static_cast<bool>(acc));
        newdb::HeapTable* tp = acc.table();
        ASSERT_NE(tp, nullptr);
        EXPECT_EQ(tp->rows.size(), 1u);
        EXPECT_EQ(tp->rows[0].id, 9);
    }
    ASSERT_TRUE(s.reload().ok);
    EXPECT_EQ(s.table.rows.size(), 1u);
}

TEST(Session, MutableHeapReturnsTableWhenLoaded) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_sess_mutable");
    std::filesystem::create_directories(dir);
    const std::string bin = (dir / "m.bin").string();
    const std::string attr = newdb::schema_sidecar_path_for_data_file(bin);
    newdb::TableSchema sch;
    sch.primary_key = "id";
    ASSERT_TRUE(newdb::save_schema_text(attr, sch).ok);
    ASSERT_TRUE(newdb::io::create_heap_file(bin.c_str(), {newdb::Row{3, {}, {}}}).ok);

    newdb::Session s;
    s.data_path = bin;
    s.table_name = "m";
    s.schema = sch;
    newdb::HeapTable* hp = s.mutable_heap(nullptr);
    ASSERT_NE(hp, nullptr);
    EXPECT_EQ(hp->rows.size(), 1u);
}
