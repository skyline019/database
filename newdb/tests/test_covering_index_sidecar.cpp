#include "covering_index_sidecar.h"

#include <newdb/page_io.h>
#include <newdb/schema_io.h>

#include <cstdlib>
#include <filesystem>

#include <gtest/gtest.h>

TEST(CoveringSidecar, EqCountAndSumLookup) {
    namespace fs = std::filesystem;
    const fs::path base = fs::temp_directory_path() / ("cov_" + std::to_string(std::rand()));
    std::error_code ec;
    fs::create_directories(base, ec);
    ASSERT_FALSE(ec);

    const std::string data_file = (base / "t.bin").string();
    newdb::TableSchema schema;
    schema.primary_key = "id";
    schema.attrs = {
        newdb::AttrMeta{"dept", newdb::AttrType::String},
        newdb::AttrMeta{"salary", newdb::AttrType::Int},
    };
    ASSERT_TRUE(newdb::io::create_heap_file(data_file.c_str(), {}));
    ASSERT_TRUE(newdb::save_schema_text(newdb::schema_sidecar_path_for_data_file(data_file), schema));

    newdb::HeapTable table;
    table.rows.push_back(newdb::Row{1, {{"dept", "ENG"}, {"salary", "100"}}, {}});
    table.rows.push_back(newdb::Row{2, {{"dept", "ENG"}, {"salary", "200"}}, {}});
    table.rows.push_back(newdb::Row{3, {{"dept", "HR"}, {"salary", "90"}}, {}});
    table.rebuild_indexes(schema);

    const auto c = lookup_or_build_covering_agg_sidecar(data_file, "dept", "__count__", "ENG", schema, table);
    ASSERT_TRUE(c.used);
    EXPECT_EQ(c.count, 2u);

    const auto s = lookup_or_build_covering_agg_sidecar(data_file, "dept", "salary", "ENG", schema, table);
    ASSERT_TRUE(s.used);
    EXPECT_EQ(s.count, 2u);
    EXPECT_DOUBLE_EQ(static_cast<double>(s.sum), 300.0);

    fs::remove_all(base, ec);
}
