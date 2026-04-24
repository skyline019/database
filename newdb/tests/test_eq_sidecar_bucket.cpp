#include "equality_index_sidecar.h"

#include <newdb/page_io.h>
#include <newdb/schema_io.h>

#include <cstdlib>
#include <filesystem>

#include <gtest/gtest.h>

TEST(EqSidecar, BucketedLayoutLookup) {
    namespace fs = std::filesystem;
    const fs::path base = fs::temp_directory_path() / ("eqbucket_" + std::to_string(std::rand()));
    std::error_code ec;
    fs::create_directories(base, ec);
    ASSERT_FALSE(ec);

    const std::string data_file = (base / "t.bin").string();
    newdb::TableSchema schema;
    schema.primary_key = "id";
    schema.attrs.push_back(newdb::AttrMeta{"dept", newdb::AttrType::String});
    ASSERT_TRUE(newdb::io::create_heap_file(data_file.c_str(), {}));
    ASSERT_TRUE(newdb::save_schema_text(newdb::schema_sidecar_path_for_data_file(data_file), schema));

    newdb::HeapTable table;
    table.rows.push_back(newdb::Row{1, {{"dept", "ENG"}}, {}});
    table.rows.push_back(newdb::Row{2, {{"dept", "HR"}}, {}});
    table.rebuild_indexes(schema);

#ifdef _WIN32
    _putenv_s("NEWDB_EQ_BUCKETS", "8");
#else
    setenv("NEWDB_EQ_BUCKETS", "8", 1);
#endif
    const auto r = lookup_or_build_eq_index_sidecar(
        EqIndexRequest{.data_file = data_file, .attr_name = "dept"},
        schema,
        table,
        "ENG");
    ASSERT_TRUE(r.used_index);
    ASSERT_EQ(r.slots.size(), 1u);
    EXPECT_EQ(r.slots[0], 0u);
#ifdef _WIN32
    _putenv_s("NEWDB_EQ_BUCKETS", "");
#else
    unsetenv("NEWDB_EQ_BUCKETS");
#endif

    fs::remove_all(base, ec);
}
