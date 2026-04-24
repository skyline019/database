#include <newdb/heap_table.h>
#include <newdb/schema.h>

#include <gtest/gtest.h>

using newdb::AttrMeta;
using newdb::AttrType;
using newdb::HeapTable;
using newdb::Row;
using newdb::SortDir;
using newdb::TableSchema;

static TableSchema sample_schema() {
    TableSchema s;
    s.primary_key = "id";
    s.attrs = {AttrMeta{"balance", AttrType::Int}, AttrMeta{"name", AttrType::String}};
    return s;
}

TEST(HeapTable, RebuildIndexesAndFindById) {
    TableSchema schema = sample_schema();
    HeapTable t;
    t.rows.push_back(Row{10, {{"balance", "100"}, {"name", "a"}}, {}});
    t.rows.push_back(Row{2, {{"balance", "50"}, {"name", "b"}}, {}});
    t.rebuild_indexes(schema);

    const Row* r = t.find_by_id(10);
    ASSERT_NE(r, nullptr);
    EXPECT_EQ(r->attrs.at("name"), "a");

    std::string pkv;
    ASSERT_TRUE(HeapTable::row_get_pk_value(*r, "id", pkv));
    EXPECT_EQ(pkv, "10");
}

TEST(HeapTable, AlternatePrimaryKeyIndex) {
    TableSchema schema;
    schema.primary_key = "email";
    schema.attrs = {AttrMeta{"email", AttrType::String}, AttrMeta{"age", AttrType::Int}};

    HeapTable t;
    t.rows.push_back(Row{1, {{"email", "a@x"}, {"age", "20"}}, {}});
    t.rows.push_back(Row{2, {{"email", "b@x"}, {"age", "30"}}, {}});
    t.rebuild_indexes(schema);

    EXPECT_TRUE(t.primary_key_value_exists(schema, "email", "b@x", 0));
    EXPECT_FALSE(t.primary_key_value_exists(schema, "email", "b@x", 2));
}

TEST(HeapTable, SortedIndicesByIdAndAttr) {
    TableSchema schema = sample_schema();
    HeapTable t;
    t.rows = {
        Row{3, {{"balance", "3"}, {"name", "c"}}, {}},
        Row{1, {{"balance", "10"}, {"name", "a"}}, {}},
        Row{2, {{"balance", "5"}, {"name", "b"}}, {}},
    };
    t.rebuild_indexes(schema);

    const auto& by_id = t.sorted_indices(schema, "id", SortDir::Asc);
    ASSERT_EQ(by_id.size(), 3u);
    EXPECT_EQ(t.rows[by_id[0]].id, 1);
    EXPECT_EQ(t.rows[by_id[1]].id, 2);
    EXPECT_EQ(t.rows[by_id[2]].id, 3);

    const auto& by_bal = t.sorted_indices(schema, "balance", SortDir::Desc);
    ASSERT_EQ(by_bal.size(), 3u);
    EXPECT_EQ(t.rows[by_bal[0]].id, 1);
}

TEST(HeapTable, FindByIdReturnsNullWhenMissing) {
    TableSchema schema = sample_schema();
    HeapTable t;
    t.rows.push_back(Row{5, {{"balance", "1"}, {"name", "n"}}, {}});
    t.rebuild_indexes(schema);
    EXPECT_EQ(t.find_by_id(99), nullptr);
}

TEST(HeapTable, RowGetPkValueMissingAttr) {
    Row r{1, {}, {}};
    std::string v;
    EXPECT_FALSE(HeapTable::row_get_pk_value(r, "missing", v));
}

TEST(HeapTable, SortedIndicesCacheReuse) {
    TableSchema schema = sample_schema();
    HeapTable t;
    t.rows = {
        Row{1, {{"balance", "2"}, {"name", "a"}}, {}},
        Row{2, {{"balance", "1"}, {"name", "b"}}, {}},
    };
    t.rebuild_indexes(schema);
    const auto& a1 = t.sorted_indices(schema, "balance", SortDir::Asc);
    const auto& a2 = t.sorted_indices(schema, "balance", SortDir::Asc);
    EXPECT_EQ(&a1, &a2);
}

TEST(HeapTable, ClearData) {
    HeapTable t;
    t.rows.push_back(Row{1, {}, {}});
    t.index_by_id[1] = 0;
    t.clear_data();
    EXPECT_TRUE(t.rows.empty());
    EXPECT_TRUE(t.index_by_id.empty());
}

TEST(HeapTable, SortedIndicesCacheEvictsOldKeys) {
    TableSchema schema;
    schema.primary_key = "id";
    for (int i = 0; i < 22; ++i) {
        schema.attrs.push_back(AttrMeta{"c" + std::to_string(i), AttrType::Int});
    }
    HeapTable t;
    Row r{1, {}, {}};
    for (int i = 0; i < 22; ++i) {
        r.attrs["c" + std::to_string(i)] = std::to_string(30 - i);
    }
    t.rows.push_back(std::move(r));
    t.rebuild_indexes(schema);
    for (int i = 0; i < 22; ++i) {
        (void)t.sorted_indices(schema, "c" + std::to_string(i), SortDir::Asc);
    }
    const auto& again = t.sorted_indices(schema, "c0", SortDir::Asc);
    ASSERT_EQ(again.size(), 1u);
    EXPECT_EQ(t.rows[again[0]].id, 1);
}

TEST(HeapTable, FindByIdLinearScanWhenIndexEmpty) {
    HeapTable t;
    t.rows.push_back(Row{42, {{"x", "y"}}, {}});
    const Row* p = t.find_by_id(42);
    ASSERT_NE(p, nullptr);
    EXPECT_EQ(p->id, 42);
}

TEST(HeapTable, PrimaryKeyExistsIdBranchWithoutIndex) {
    TableSchema schema;
    schema.primary_key = "id";
    HeapTable t;
    t.rows.push_back(Row{9, {}, {}});
    EXPECT_TRUE(t.primary_key_value_exists(schema, "id", "9", 0));
    EXPECT_FALSE(t.primary_key_value_exists(schema, "id", "9", 9));
    EXPECT_FALSE(t.primary_key_value_exists(schema, "id", "not_a_number", 0));
}

TEST(HeapTable, SortedIndicesOrderKeyNotInSchemaAttrs) {
    TableSchema schema = sample_schema();
    HeapTable t;
    t.rows = {
        Row{1, {{"balance", "2"}, {"name", "a"}, {"extra", "z"}}, {}},
        Row{2, {{"balance", "1"}, {"name", "b"}, {"extra", "m"}}, {}},
    };
    t.rebuild_indexes(schema);
    const auto& ord = t.sorted_indices(schema, "extra", SortDir::Asc);
    ASSERT_EQ(ord.size(), 2u);
    EXPECT_EQ(t.rows[ord[0]].id, 2);
    EXPECT_EQ(t.rows[ord[1]].id, 1);
}
