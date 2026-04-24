#include "equality_index_sidecar.h"

#include <newdb/page_io.h>
#include <newdb/schema_io.h>

#include <cstdlib>
#include <filesystem>

#include <gtest/gtest.h>

TEST(EqBloom, DefiniteMissSkipsSidecarParse) {
    namespace fs = std::filesystem;
    const fs::path base = fs::temp_directory_path() / ("eqbloom_" + std::to_string(std::rand()));
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
    _putenv_s("NEWDB_EQ_BLOOM_BITS", "4096");
    _putenv_s("NEWDB_EQ_BLOOM_K", "5");
#else
    setenv("NEWDB_EQ_BLOOM_BITS", "4096", 1);
    setenv("NEWDB_EQ_BLOOM_K", "5", 1);
#endif

    // Build sidecar (also writes bloom).
    (void)lookup_or_build_eq_index_sidecar(EqIndexRequest{.data_file = data_file, .attr_name = "dept"},
                                           schema,
                                           table,
                                           "ENG");
    ASSERT_TRUE(fs::exists(data_file + ".eqbloom.dept", ec));

    // Lookup a value not present: should hit bloom definite-miss and return used_index=true with empty.
    const auto miss = lookup_or_build_eq_index_sidecar(EqIndexRequest{.data_file = data_file, .attr_name = "dept"},
                                                       schema,
                                                       table,
                                                       "NO_SUCH");
    ASSERT_TRUE(miss.used_index);
    EXPECT_TRUE(miss.slots.empty());

#ifdef _WIN32
    _putenv_s("NEWDB_EQ_BLOOM_BITS", "");
    _putenv_s("NEWDB_EQ_BLOOM_K", "");
#else
    unsetenv("NEWDB_EQ_BLOOM_BITS");
    unsetenv("NEWDB_EQ_BLOOM_K");
#endif

    fs::remove_all(base, ec);
}

