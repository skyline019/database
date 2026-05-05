#include "cli/modules/where/executor/stats/table_stats.h"
#include "cli/modules/where/executor/where.h"

#include <newdb/heap_table.h>
#include <newdb/schema.h>

#include <filesystem>
#include <fstream>
#include <random>

#include <gtest/gtest.h>

using newdb::AttrMeta;
using newdb::AttrType;
using newdb::HeapTable;
using newdb::Row;
using newdb::TableSchema;

TEST(QueryTableStats, HistogramEquiDepthEightBuckets) {
    TableSchema schema;
    schema.attrs = {AttrMeta{"n", AttrType::String}};
    HeapTable tbl;
    for (unsigned i = 0; i < 16; ++i) {
        const int rid = static_cast<int>(i) + 1;
        tbl.rows.push_back(Row{rid, {{"n", std::to_string(static_cast<int>(i))}}});
    }
    tbl.rebuild_indexes(schema);
    TableStats st{};
    ASSERT_TRUE(build_table_stats_from_heap(tbl, schema, &st));
    const ColumnStats& cs = st.columns.at("n");
    ASSERT_EQ(cs.histogram_buckets.size(), 8u);
    std::uint64_t sum = 0;
    for (auto v : cs.histogram_buckets) {
        sum += v;
    }
    EXPECT_EQ(sum, cs.non_null_count);
}

TEST(QueryTableStats, BuildTracksMinMaxTopK) {
    TableSchema schema;
    schema.attrs = {AttrMeta{"color", AttrType::String}};
    HeapTable tbl;
    tbl.rows = {
        Row{1, {{"color", "red"}}, {}},
        Row{2, {{"color", "blue"}}, {}},
        Row{3, {{"color", "red"}}, {}},
    };
    tbl.rebuild_indexes(schema);
    TableStats st{};
    ASSERT_TRUE(build_table_stats_from_heap(tbl, schema, &st));
    const ColumnStats& cs = st.columns.at("color");
    EXPECT_EQ(cs.min_value, "blue");
    EXPECT_EQ(cs.max_value, "red");
    ASSERT_GE(cs.top_k.size(), 1u);
    EXPECT_EQ(cs.top_k[0], "red");
}

TEST(QueryTableStats, PlanCandidatesPreferIdLookup) {
    TableSchema schema;
    schema.primary_key = "id";
    schema.attrs = {AttrMeta{"color", AttrType::String}};
    HeapTable tbl;
    tbl.rows = {
        Row{10, {{"color", "x"}}, {}},
        Row{20, {{"color", "y"}}, {}},
    };
    tbl.rebuild_indexes(schema);
    WhereCond wc;
    wc.attr = "id";
    wc.op = CondOp::Eq;
    wc.value = "10";
    const std::vector<WhereCond> conds = {wc};
    const std::vector<PlanCandidate> pc = where_build_plan_candidates(tbl, schema, conds, nullptr);
    ASSERT_FALSE(pc.empty());
    EXPECT_EQ(pc.front().id, "id_lookup");
}

TEST(QueryTableStats, BuildAndEqSelectivity) {
    TableSchema schema;
    schema.attrs = {AttrMeta{"color", AttrType::String}};
    HeapTable tbl;
    tbl.rows = {
        Row{1, {{"color", "red"}}, {}},
        Row{2, {{"color", "blue"}}, {}},
        Row{3, {{"color", "red"}}, {}},
    };
    tbl.rebuild_indexes(schema);
    TableStats st{};
    ASSERT_TRUE(build_table_stats_from_heap(tbl, schema, &st));
    EXPECT_EQ(st.row_count, 3u);
    EXPECT_EQ(st.stats_schema_fp, table_stats_schema_fingerprint(schema));
    EXPECT_TRUE(table_stats_matches_schema(st, schema));
    const double sel = eq_selectivity_from_stats(&st, "color", 3);
    EXPECT_GT(sel, 0.0);
    EXPECT_LE(sel, 1.0);
    const double rsel = range_selectivity_from_stats(&st, "color", 3);
    EXPECT_GE(rsel, sel);
    EXPECT_LE(rsel, 0.5);
}

TEST(QueryTableStats, StaleStatsWhenSchemaMismatchMatchesQueryHandlerSemantics) {
    namespace fs = std::filesystem;
    std::error_code ec;
    fs::path base = fs::temp_directory_path(ec);
    if (ec) {
        base = fs::current_path(ec);
    }
    const auto tag = static_cast<unsigned long long>(std::random_device{}());
    const fs::path dir = base / (std::string("newdb_ts_stale_") + std::to_string(tag));
    fs::create_directories(dir, ec);
    const std::string data_file = (dir / "t.bin").string();

    TableSchema schema_v1;
    schema_v1.attrs = {AttrMeta{"c", AttrType::String}, AttrMeta{"d", AttrType::String}};
    TableStats st{};
    st.row_count = 2;
    st.columns["c"] = ColumnStats{2, 1};
    st.columns["d"] = ColumnStats{2, 2};
    ASSERT_TRUE(save_table_stats_file(data_file, schema_v1, st));

    TableSchema schema_v2;
    schema_v2.attrs = {AttrMeta{"c", AttrType::String}};
    TableStats loaded{};
    EXPECT_FALSE(load_table_stats_file(data_file, schema_v2, &loaded));
    ASSERT_TRUE(fs::exists(table_stats_file_path_for_data_file(data_file), ec));

    invalidate_table_stats_for_data_file(data_file);
    fs::remove_all(dir, ec);
}

TEST(QueryTableStats, PersistSaveLoadRoundTrip) {
    namespace fs = std::filesystem;
    std::error_code ec;
    fs::path base = fs::temp_directory_path(ec);
    if (ec) {
        base = fs::current_path(ec);
    }
    const auto tag = static_cast<unsigned long long>(std::random_device{}());
    const fs::path dir = base / (std::string("newdb_ts_persist_") + std::to_string(tag));
    fs::create_directories(dir, ec);
    const std::string data_file = (dir / "t.bin").string();

    TableSchema schema;
    schema.attrs = {AttrMeta{"c", AttrType::String}, AttrMeta{"d", AttrType::String}};
    TableStats st{};
    st.row_count = 4;
    st.columns["c"] = ColumnStats{4, 2};
    st.columns["d"] = ColumnStats{3, 3};

    ASSERT_TRUE(save_table_stats_file(data_file, schema, st));
    {
        const std::string stats_path = table_stats_file_path_for_data_file(data_file);
        ASSERT_TRUE(fs::exists(stats_path)) << stats_path;
        std::ifstream check(stats_path, std::ios::binary);
        std::string hdr;
        ASSERT_TRUE(static_cast<bool>(std::getline(check, hdr)));
        if (!hdr.empty() && hdr.back() == '\r') {
            hdr.pop_back();
        }
        EXPECT_EQ(hdr, "NEWDB_TABLESTATS_V3");
    }
    TableStats loaded{};
    ASSERT_TRUE(load_table_stats_file(data_file, schema, &loaded));
    EXPECT_TRUE(table_stats_matches_schema(loaded, schema));
    EXPECT_EQ(loaded.row_count, 4u);
    EXPECT_EQ(loaded.columns["c"].non_null_count, 4u);
    EXPECT_EQ(loaded.columns["c"].distinct_count, 2u);
    EXPECT_EQ(loaded.columns["d"].non_null_count, 3u);

    schema.attrs = {AttrMeta{"c", AttrType::String}};
    EXPECT_FALSE(load_table_stats_file(data_file, schema, &loaded));

    invalidate_table_stats_for_data_file(data_file);
    EXPECT_FALSE(fs::exists(table_stats_file_path_for_data_file(data_file), ec));

    fs::remove_all(dir, ec);
}
