#include <newdb/heap_table.h>
#include <newdb/schema.h>

#include <chrono>
#include <gtest/gtest.h>

using newdb::AttrMeta;
using newdb::AttrType;
using newdb::HeapTable;
using newdb::Row;
using newdb::SortDir;
using newdb::TableSchema;

// CI gate: cheap regression on non-id sort without full lazy materialize (heap path not used here;
// still guards sort path cost on in-memory tables).
TEST(CiMicrobench, SortByNonIdColumnUnderBudget) {
    TableSchema schema;
    schema.primary_key = "id";
    schema.attrs = {AttrMeta{"score", AttrType::Int}, AttrMeta{"name", AttrType::String}};
    HeapTable t;
    t.rows.reserve(4000);
    for (int i = 0; i < 4000; ++i) {
        Row r;
        r.id = i + 1;
        r.attrs["score"] = std::to_string((i * 17) % 10000);
        r.attrs["name"] = "n" + std::to_string(i);
        t.rows.push_back(std::move(r));
    }
    t.rebuild_indexes(schema);
    const auto t0 = std::chrono::steady_clock::now();
    const auto& ord = t.sorted_indices(schema, "score", SortDir::Asc);
    const auto t1 = std::chrono::steady_clock::now();
    ASSERT_EQ(ord.size(), 4000u);
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
    EXPECT_LT(ms, 2000) << "sort regression: " << ms << "ms";
}
