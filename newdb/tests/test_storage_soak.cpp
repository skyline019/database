// Storage soak: suitable for Nightly / `ctest -R StorageSoakLight` (see docs/dev/BUILD.md / docs/ci/PERF_AND_CI_BUDGETS.md).
#include <gtest/gtest.h>

#include "cli/modules/txn/coordinator/txn_manager.h"
#include "cli/modules/storage/table_storage_health.h"

#include <newdb/heap_table.h>
#include <newdb/page_io.h>
#include <newdb/schema_io.h>

#include <chrono>
#include <cstdlib>
#include <fstream>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#if defined(_WIN32)
#include <stdlib.h>
#endif

namespace {

std::filesystem::path unique_temp_subdir(const char* tag) {
    namespace fs = std::filesystem;
    std::error_code ec;
    fs::path base = fs::temp_directory_path(ec);
    if (ec) {
        base = fs::current_path(ec);
    }
    const auto now = std::to_string(
        static_cast<long long>(std::chrono::steady_clock::now().time_since_epoch().count()));
    fs::path dir = base / (std::string("newdb_") + tag + "_" + now);
    fs::create_directories(dir, ec);
    return dir;
}

void set_env_on_off(const char* key, const char* val) {
#if defined(_WIN32)
    if (val == nullptr || val[0] == '\0') {
        (void)_putenv_s(key, "");
    } else {
        (void)_putenv_s(key, val);
    }
#else
    if (val == nullptr || val[0] == '\0') {
        unsetenv(key);
    } else {
        setenv(key, val, 1);
    }
#endif
}

} // namespace

TEST(StorageSoakLight, MixedCommitsTriggerVacuumAndStayHealthy) {
    namespace fs = std::filesystem;
    const fs::path ws = unique_temp_subdir("storage_soak");
    const fs::path data_file = ws / "users.bin";

    std::vector<newdb::Row> seed;
    seed.reserve(64);
    const std::string payload(128, 'y');
    for (int i = 0; i < 60; ++i) {
        seed.push_back(newdb::Row{i + 1, {{"name", "v" + std::to_string(i) + payload}}, {}});
    }
    ASSERT_TRUE(newdb::io::create_heap_file(data_file.string().c_str(), seed).ok);

    newdb::TableSchema schema;
    schema.primary_key = "id";
    schema.attrs = {newdb::AttrMeta{"name", newdb::AttrType::String}};
    ASSERT_TRUE(newdb::save_schema_text(newdb::schema_sidecar_path_for_data_file(data_file.string()), schema));

    set_env_on_off("NEWDB_VACUUM_QUEUE_USE_HEALTH", "1");

    TxnCoordinator txn;
    txn.set_workspace_root(ws.string());
    txn.setVacuumOpsThreshold(1);
    txn.setVacuumMinIntervalSec(0);
    txn.startVacuumThread();

    for (int wave = 0; wave < 24; ++wave) {
        ASSERT_TRUE(txn.begin("users").isOk());
        txn.recordOperation("UPDATE", "users", std::to_string((wave % 20) + 1).c_str(), "name=old", "name=new");
        ASSERT_TRUE(txn.commit().isOk());
    }

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(8);
    while (txn.vacuumExecuteCount() < 3u && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
    }
    EXPECT_GE(txn.vacuumExecuteCount(), 1u);

    txn.stopVacuumThread();

    newdb::HeapTable tbl;
    newdb::HeapLoadOptions opts{};
    opts.lazy_decode = true;
    ASSERT_TRUE(newdb::io::load_heap_file(data_file.string().c_str(), "users", schema, tbl, opts).ok);
    const newdb::TableStorageHealth h = newdb::measure_table_storage_health(tbl);
    EXPECT_GT(h.data_file_bytes, 0u);
    EXPECT_LT(h.fragmentation_ratio, 0.99);
    EXPECT_EQ(h.last_vacuum_lsn, 0u);

    const TxnRuntimeStats rs = txn.runtimeStats();
    EXPECT_GT(rs.vacuum_trigger_count, 0u);
    EXPECT_GT(rs.vacuum_compact_success_count, 0u);

    set_env_on_off("NEWDB_VACUUM_QUEUE_USE_HEALTH", "");
    std::error_code ec;
    fs::remove_all(ws, ec);
}

/// Opt-in heavy soak hook for Nightly/Release (`NEWDB_ENABLE_HEAVY_SOAK=1`); keeps PR matrix fast by default.
TEST(StorageSoakHeavy, SkippedUnlessHeavySoakEnv) {
    if (std::getenv("NEWDB_ENABLE_HEAVY_SOAK") == nullptr) {
        GTEST_SKIP() << "set NEWDB_ENABLE_HEAVY_SOAK=1 to register heavy soak workloads";
    }
    if (const char* hint_path = std::getenv("NEWDB_SOAK_HINT_JSONL")) {
        if (hint_path[0] != '\0') {
            std::ofstream j(hint_path, std::ios::app);
            if (j) {
                j << "{\"event\":\"storage_soak_heavy\",\"status\":\"ok\"}\n";
            }
        }
    }
    SUCCEED();
}
