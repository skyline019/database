#include "cli/shell/dispatch/router/dispatch.h"
#include "cli/modules/where/executor/stats/table_stats.h"
#include "cli/shell/state/shell_state_facade.h"
#include "cli/shell/state/shell_state_owner.h"

#include <newdb/schema.h>
#include "cli/modules/txn/coordinator/txn_manager.h"
#include "test_util.h"
#include "shell_state_test_support.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>

namespace fs = std::filesystem;

namespace {

#if defined(_WIN32)
void set_test_env(const char* key, const char* val) {
    (void)_putenv_s(key, val);
}
void unset_test_env(const char* key) {
    (void)_putenv_s(key, "");
}
#else
void set_test_env(const char* key, const char* val) {
    (void)setenv(key, val, 1);
}
void unset_test_env(const char* key) {
    (void)unsetenv(key);
}
#endif

struct ScopedEnv {
    const char* key;
    ScopedEnv(const char* k, const char* val) : key(k) {
        set_test_env(k, val);
    }
    ScopedEnv(const ScopedEnv&) = delete;
    ScopedEnv& operator=(const ScopedEnv&) = delete;
    ~ScopedEnv() {
        unset_test_env(key);
    }
};

struct DemoHarness {
    fs::path dir;
    ShellStateOwner own;
    ShellStateFacade f;

    explicit DemoHarness(const std::string& prefix)
        : own(make_shell_state_for_test()), f(own.shell()) {
        dir = newdb::test::unique_temp_subdir(prefix);
        fs::create_directories(dir);
        f.data_dir() = dir.string();
        f.log_file_path() = (dir / "demo_log.bin").string();
        f.table_name().clear();
        f.data_path().clear();
        f.schema() = newdb::TableSchema{};
        f.bind_logging();
        f.txn().set_workspace_root(f.data_dir());
    }

    std::string run(const std::string& cmd) {
        std::error_code ec;
        const std::uintmax_t before = fs::file_size(f.log_file_path(), ec);
        const std::uintmax_t start = ec ? 0 : before;

        const bool ok = process_command_line(own.shell(), cmd.c_str());
        EXPECT_TRUE(ok) << "cmd=" << cmd;

        std::ifstream in(f.log_file_path(), std::ios::binary);
        if (!in.good()) {
            return {};
        }
        in.seekg(0, std::ios::end);
        const std::streamoff end = in.tellg();
        if (end <= 0 || start >= static_cast<std::uintmax_t>(end)) {
            return {};
        }
        in.seekg(static_cast<std::streamoff>(start), std::ios::beg);
        std::string out;
        out.assign(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
        return out;
    }
};

} // namespace

TEST(ShowPlanTableStatsStaleShell, ShowPlanJsonMarksStaleWhenPersistedStatsFileMismatch) {
    ScopedEnv e_use("NEWDB_QUERY_USE_TABLE_STATS", "1");
    ScopedEnv e_persist("NEWDB_QUERY_PERSIST_TABLE_STATS", "1");

    DemoHarness h("showplan_stats_stale");
    h.run("CREATE TABLE(sps)");
    h.run("USE(sps)");
    h.run("DEFATTR(name:string,age:int)");
    h.run("INSERT(1,Alice,20)");
    const std::string first = h.run("SHOW PLAN(name, =, Alice)");
    EXPECT_NE(first.find("\"table_stats_stale\":false"), std::string::npos) << first;

    const std::string data_file = h.f.resolve_table_file("sps.bin");
    const std::string stats_path = table_stats_file_path_for_data_file(data_file);
    {
        std::ofstream wreck(stats_path, std::ios::out | std::ios::trunc);
        ASSERT_TRUE(wreck.good());
        wreck << "NEWDB_TABLESTATS_V2\n";
        wreck << "fp=1\n";
        wreck << "row_count=1\n";
    }

    const std::string second = h.run("SHOW PLAN(name, =, Alice)");
    EXPECT_NE(second.find("\"table_stats_stale\":true"), std::string::npos) << second;
}
