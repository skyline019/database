#include "cli/shell/dispatch/router/dispatch.h"
#include "cli/shell/state/shell_state_facade.h"
#include "cli/shell/state/shell_state_owner.h"

#include <newdb/schema.h>
#include "cli/modules/txn/coordinator/txn_manager.h"
#include "test_util.h"
#include "shell_state_test_support.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>

namespace fs = std::filesystem;

namespace {

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

std::optional<std::uint64_t> json_u64_field(const std::string& s, const std::string& key) {
    const std::string needle = "\"" + key + "\":";
    const auto pos = s.find(needle);
    if (pos == std::string::npos) {
        return std::nullopt;
    }
    std::size_t i = pos + needle.size();
    while (i < s.size() && (s[i] == ' ' || s[i] == '\t')) {
        ++i;
    }
    const std::size_t start = i;
    while (i < s.size() && s[i] >= '0' && s[i] <= '9') {
        ++i;
    }
    if (i == start) {
        return std::nullopt;
    }
    try {
        return static_cast<std::uint64_t>(std::stoull(s.substr(start, i - start)));
    } catch (...) {
        return std::nullopt;
    }
}

} // namespace

TEST(TxnShellMultiEntrySnapshot, TransactionSnapshotLsnStableAcrossCountPageWhereFind) {
    DemoHarness h("txn_shell_multi_snap");
    h.run("CREATE TABLE(mqs)");
    h.run("USE(mqs)");
    h.run("DEFATTR(name:string,age:int)");
    h.run("INSERT(1,Alice,10)");
    h.run("INSERT(2,Bob,20)");
    h.run("TXNISOLATION snapshot");
    h.run("BEGIN");
    h.run("COUNT");

    const std::string baseline = h.run("SHOW TUNING JSON");
    const auto base_txn = json_u64_field(baseline, "transaction_snapshot_lsn");
    const auto base_stmt = json_u64_field(baseline, "statement_snapshot_lsn");
    ASSERT_TRUE(base_txn.has_value()) << "tail=" << baseline;
    ASSERT_TRUE(base_stmt.has_value()) << "tail=" << baseline;
    ASSERT_GT(*base_txn, 0u) << "snapshot txn read view should pin non-zero transaction LSN";

    const char* cmds[] = {
        "PAGE(1,10,id,desc)",
        "WHERE(id, =, 1)",
        "COUNT",
        "FIND(1)",
    };
    for (int i = 0; i < 4; ++i) {
        h.run(cmds[i]);
        const std::string tail = h.run("SHOW TUNING JSON");
        const auto txn = json_u64_field(tail, "transaction_snapshot_lsn");
        const auto stmt = json_u64_field(tail, "statement_snapshot_lsn");
        ASSERT_TRUE(txn.has_value()) << "tail=" << tail;
        ASSERT_TRUE(stmt.has_value()) << "tail=" << tail;
        EXPECT_EQ(*txn, *base_txn) << "cmd=" << cmds[i];
        EXPECT_EQ(*stmt, *base_stmt) << "cmd=" << cmds[i];
    }

    h.run("COMMIT");
}
