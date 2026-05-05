#include "cli/modules/sidecar/eq/equality_index_sidecar.h"

#include <newdb/memory_registry.h>
#include <newdb/page_io.h>
#include <newdb/schema_io.h>

#include <cstdlib>
#include <filesystem>
#if defined(_WIN32)
#include <stdlib.h>
#endif

#include <gtest/gtest.h>

namespace {

void set_env(const char* k, const char* v) {
#if defined(_WIN32)
    if (v == nullptr || v[0] == '\0') {
        (void)_putenv_s(k, "");
    } else {
        (void)_putenv_s(k, v);
    }
#else
    if (v == nullptr || v[0] == '\0') {
        unsetenv(k);
    } else {
        setenv(k, v, 1);
    }
#endif
}

}  // namespace

TEST(EqSidecarAdmit, TinySidecarCapRejectsCacheInstall) {
    namespace fs = std::filesystem;
    set_env("NEWDB_SIDECAR_CACHE_MAX_BYTES", "8");
    const fs::path base = fs::temp_directory_path() / ("eqadm_" + std::to_string(std::rand()));
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

    const std::uint64_t before = newdb::memory_registry_totals().sidecar_admit_rejects;
    const auto r = lookup_or_build_eq_index_sidecar(
        EqIndexRequest{.data_file = data_file, .attr_name = "dept"}, schema, table, "ENG", nullptr);
    ASSERT_TRUE(r.used_index);
    EXPECT_EQ(r.slots.size(), 1u);
    const std::uint64_t after = newdb::memory_registry_totals().sidecar_admit_rejects;
    EXPECT_GE(after, before + 1u);

    set_env("NEWDB_SIDECAR_CACHE_MAX_BYTES", "");
    fs::remove_all(base, ec);
}
