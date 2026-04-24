#include "visibility_checkpoint_sidecar.h"

#include <newdb/page_io.h>
#include <newdb/schema_io.h>

#include <cstdlib>
#include <filesystem>

#include <gtest/gtest.h>

TEST(VisibilityCheckpoint, BuildReadAndInvalidate) {
    namespace fs = std::filesystem;
    const fs::path base = fs::temp_directory_path() / ("vischk_" + std::to_string(std::rand()));
    std::error_code ec;
    fs::create_directories(base, ec);
    ASSERT_FALSE(ec);

    const std::string data_file = (base / "t.bin").string();
    newdb::TableSchema schema;
    schema.primary_key = "id";
    schema.attrs.push_back(newdb::AttrMeta{"name", newdb::AttrType::String});
    ASSERT_TRUE(newdb::io::create_heap_file(data_file.c_str(), {}));
    ASSERT_TRUE(newdb::save_schema_text(newdb::schema_sidecar_path_for_data_file(data_file), schema));

    newdb::HeapTable table;
    table.name = (base / "t").string();
    table.rows.push_back(newdb::Row{1, {{"name", "a"}}, {}});
    table.rows.push_back(newdb::Row{2, {{"name", "b"}}, {}});
    table.rebuild_indexes(schema);

    const auto first = load_or_build_visibility_checkpoint_sidecar(data_file, schema, table);
    ASSERT_EQ(first.size(), 2u);
    EXPECT_EQ(first[0], 0u);
    EXPECT_EQ(first[1], 1u);

    const auto second = load_or_build_visibility_checkpoint_sidecar(data_file, schema, table);
    ASSERT_EQ(second, first);

    const fs::path sidecar = data_file + ".vischk";
    ASSERT_TRUE(fs::exists(sidecar, ec));
    invalidate_visibility_checkpoint_sidecars_for_data_file(data_file);
    EXPECT_FALSE(fs::exists(sidecar, ec));

    fs::remove_all(base, ec);
}

TEST(VisibilityCheckpoint, EnvOffFallsBackWithoutFile) {
    namespace fs = std::filesystem;
    const fs::path base = fs::temp_directory_path() / ("vischk_off_" + std::to_string(std::rand()));
    std::error_code ec;
    fs::create_directories(base, ec);
    ASSERT_FALSE(ec);
    const std::string data_file = (base / "x.bin").string();

    newdb::TableSchema schema;
    schema.primary_key = "id";
    newdb::HeapTable table;
    table.rows.push_back(newdb::Row{1, {}, {}});
    table.rebuild_indexes(schema);

#ifdef _WIN32
    _putenv_s("NEWDB_VISCHK", "off");
#else
    setenv("NEWDB_VISCHK", "off", 1);
#endif
    const auto slots = load_or_build_visibility_checkpoint_sidecar(data_file, schema, table);
    ASSERT_EQ(slots.size(), 1u);
    EXPECT_EQ(slots[0], 0u);
    EXPECT_FALSE(fs::exists(data_file + ".vischk", ec));
#ifdef _WIN32
    _putenv_s("NEWDB_VISCHK", "");
#else
    unsetenv("NEWDB_VISCHK");
#endif
    fs::remove_all(base, ec);
}
