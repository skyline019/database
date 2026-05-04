#include "cli/modules/where/executor/stats/table_stats.h"

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

TEST(CiMicrobench, HighCardinalityColumnYieldsLowEqSelectivity) {
    TableSchema schema;
    schema.primary_key = "id";
    schema.attrs = {AttrMeta{"sku", AttrType::String}};
    HeapTable t;
    constexpr int n = 2000;
    t.rows.reserve(n);
    for (int i = 0; i < n; ++i) {
        Row r;
        r.id = i + 1;
        r.attrs["sku"] = "k" + std::to_string(i);
        t.rows.push_back(std::move(r));
    }
    t.rebuild_indexes(schema);
    TableStats st{};
    ASSERT_TRUE(build_table_stats_from_heap(t, schema, &st));
    EXPECT_EQ(st.columns["sku"].distinct_count, static_cast<std::uint64_t>(n));
    const double sel = eq_selectivity_from_stats(&st, "sku", t.logical_row_count());
    EXPECT_GT(sel, 0.0);
    EXPECT_LT(sel, 0.01) << "near-unique values should produce low eq selectivity estimate";
}
