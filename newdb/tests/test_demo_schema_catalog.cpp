#include "schema_catalog.h"

#include <newdb/schema_io.h>

#include "test_util.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <fstream>

namespace fs = std::filesystem;

TEST(SchemaCatalog, CreateListDelete) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_schcat");
    fs::create_directories(dir);
    newdb::test::ScopedCwd cwd(dir);

    EXPECT_TRUE(create_schema("", "demo_schema_a"));
    EXPECT_FALSE(create_schema("", "demo_schema_a"));

    std::vector<std::string> schemas;
    list_schemas("", schemas);
    ASSERT_NE(std::find(schemas.begin(), schemas.end(), "demo_schema_a"), schemas.end());

    EXPECT_TRUE(delete_schema("", "demo_schema_a"));
    list_schemas("", schemas);
    EXPECT_EQ(std::find(schemas.begin(), schemas.end(), "demo_schema_a"), schemas.end());
}

TEST(SchemaCatalog, GetTablesInSchemaUsesSidecarLabel) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_schcat_tbl");
    fs::create_directories(dir);
    newdb::test::ScopedCwd cwd(dir);

    const fs::path bin_path = dir / "accounts.bin";
    const std::string attr = newdb::schema_sidecar_path_for_data_file(bin_path.string());

    newdb::TableSchema sch;
    sch.table_label = "catalog_label_z";
    sch.primary_key = "id";
    sch.attrs = {newdb::AttrMeta{"id", newdb::AttrType::Int}};
    ASSERT_TRUE(newdb::save_schema_text(attr, sch).ok);

    std::ofstream(bin_path, std::ios::binary | std::ios::trunc).close();

    std::vector<std::string> tables;
    get_tables_in_schema("", "catalog_label_z", tables);
    ASSERT_NE(std::find(tables.begin(), tables.end(), "accounts"), tables.end());
}
