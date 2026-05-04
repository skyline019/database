#include "cli/modules/sidecar/eq/equality_index_sidecar.h"

#include <newdb/page_io.h>
#include <newdb/schema_io.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#if !defined(_WIN32)
#include <stdlib.h>
#endif

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

TEST(EqSidecar, MemoryBudgetSkipsOversizedDiskSidecar) {
    namespace fs = std::filesystem;
    const fs::path base = fs::temp_directory_path() / ("eqbudget_" + std::to_string(std::rand()));
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
    table.rebuild_indexes(schema);

    const auto r0 = lookup_or_build_eq_index_sidecar(
        EqIndexRequest{.data_file = data_file, .attr_name = "dept"}, schema, table, "ENG", nullptr);
    ASSERT_TRUE(r0.used_index);

    invalidate_eq_index_sidecars_for_data_file(data_file);
    const std::string sidecar_path = data_file + ".eqidx.dept";
    {
        std::ofstream junk(sidecar_path, std::ios::binary | std::ios::trunc);
        for (int i = 0; i < 12000; ++i) {
            junk.put('x');
        }
    }

#if defined(_WIN32)
    ASSERT_EQ(0, _putenv_s("NEWDB_MEMORY_BUDGET_MAX_BYTES", "4096"));
#else
    ASSERT_EQ(0, setenv("NEWDB_MEMORY_BUDGET_MAX_BYTES", "4096", 1));
#endif
    const std::uint64_t skips_before = eq_sidecar_memory_budget_skip_count();
    (void)lookup_or_build_eq_index_sidecar(
        EqIndexRequest{.data_file = data_file, .attr_name = "dept"}, schema, table, "ENG", nullptr);
    EXPECT_GE(eq_sidecar_memory_budget_skip_count(), skips_before + 1u);

#if defined(_WIN32)
    (void)_putenv_s("NEWDB_MEMORY_BUDGET_MAX_BYTES", "");
#else
    unsetenv("NEWDB_MEMORY_BUDGET_MAX_BYTES");
#endif
    fs::remove_all(base, ec);
}
