#include <newdb/heap_table.h>
#include <newdb/mvcc.h>
#include <newdb/page_io.h>
#include <newdb/schema.h>

#include "test_util.h"

#include <gtest/gtest.h>

using newdb::AttrMeta;
using newdb::AttrType;
using newdb::HeapTable;
using newdb::MVCCSnapshot;
using newdb::RecordMetadata;
using newdb::Row;
using newdb::SortDir;
using newdb::TableSchema;

static TableSchema mvcc_schema() {
    TableSchema s;
    s.primary_key = "id";
    s.attrs = {AttrMeta{"v", AttrType::String}};
    return s;
}

TEST(MVCC, HeapTableSnapshotFiltersInvisibleRows) {
    HeapTable t;
    const TableSchema sch = mvcc_schema();
    t.rows = {
        Row{1, {{"v", "a"}}, {}},
        Row{2, {{"v", "b"}}, {}},
        Row{3, {{"v", "c"}, {"__deleted", "1"}}, {}},
    };
    t.row_meta = {
        RecordMetadata{1, 0, 11, false},
        RecordMetadata{2, 0, 22, false},
        RecordMetadata{3, 0, 33, true},
    };

    MVCCSnapshot snap;
    snap.snapshot_lsn = 10;
    snap.active_txns.insert(22); // row id=2 invisible

    t.set_snapshot(snap);
    t.rebuild_indexes(sch);

    ASSERT_NE(t.find_by_id(1), nullptr);
    EXPECT_EQ(t.find_by_id(2), nullptr);
    EXPECT_EQ(t.find_by_id(3), nullptr);

    const auto& sorted = t.sorted_indices(sch, "id", SortDir::Asc);
    ASSERT_EQ(sorted.size(), 1u);
    EXPECT_EQ(t.rows[sorted[0]].id, 1);
}

TEST(MVCC, PageIoPopulatesRowMetadataShape) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_mvcc_meta");
    std::filesystem::create_directories(dir);
    const std::string path = (dir / "m.bin").string();
    std::vector<Row> seed = {
        Row{1, {{"v", "x"}}, {}},
        Row{2, {{"v", "y"}}, {}},
    };
    ASSERT_TRUE(newdb::io::create_heap_file(path.c_str(), seed).ok);

    HeapTable t;
    ASSERT_TRUE(newdb::io::load_heap_file(path.c_str(), "m", mvcc_schema(), t).ok);
    ASSERT_EQ(t.row_meta.size(), t.rows.size());
    ASSERT_EQ(t.row_meta.size(), 2u);
    EXPECT_GT(t.row_meta[0].created_lsn, 0u);
    EXPECT_GT(t.row_meta[1].created_lsn, 0u);
}

