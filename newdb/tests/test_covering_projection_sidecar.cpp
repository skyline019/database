#include "covering_index_sidecar.h"

#include <newdb/schema_io.h>

#include <cstdlib>
#include <filesystem>

#include <gtest/gtest.h>

TEST(CoveringProjSidecar, WhereEqProjectionHitPath) {
    namespace fs = std::filesystem;
    const fs::path base = fs::temp_directory_path() / ("covp_" + std::to_string(std::rand()));
    std::error_code ec;
    fs::create_directories(base, ec);
    ASSERT_FALSE(ec);

    const std::string data_file = (base / "t.bin").string();
    newdb::TableSchema schema;
    schema.primary_key = "id";
    schema.attrs.push_back(newdb::AttrMeta{"dept", newdb::AttrType::String});
    schema.attrs.push_back(newdb::AttrMeta{"name", newdb::AttrType::String});
    ASSERT_TRUE(newdb::save_schema_text(newdb::schema_sidecar_path_for_data_file(data_file), schema));

    newdb::HeapTable table;
    table.rows.push_back(newdb::Row{1, {{"dept", "ENG"}, {"name", "alice"}}, {}});
    table.rows.push_back(newdb::Row{2, {{"dept", "ENG"}, {"name", "bob"}}, {}});
    table.rows.push_back(newdb::Row{3, {{"dept", "HR"}, {"name", "carl"}}, {}});
    table.rebuild_indexes(schema);

    const auto rows = lookup_or_build_covering_proj_sidecar(data_file,
                                                            "dept",
                                                            "name",
                                                            "ENG",
                                                            50,
                                                            schema,
                                                            table);
    ASSERT_EQ(rows.size(), 2u);
    EXPECT_EQ(rows[0].id, 1);
    EXPECT_EQ(rows[0].value, "alice");
    EXPECT_EQ(rows[1].id, 2);
    EXPECT_EQ(rows[1].value, "bob");

    // Ensure the sidecar is on disk and a re-lookup returns same data.
    ASSERT_TRUE(fs::exists(data_file + ".covp.dept.name", ec));
    const auto rows2 = lookup_or_build_covering_proj_sidecar(data_file,
                                                             "dept",
                                                             "name",
                                                             "ENG",
                                                             50,
                                                             schema,
                                                             table);
    ASSERT_EQ(rows2.size(), 2u);
    EXPECT_EQ(rows2[0].id, 1);
    EXPECT_EQ(rows2[0].value, "alice");

    fs::remove_all(base, ec);
}

