#include "cli/shell/dispatch/router/dispatch.h"
#include "cli/shell/state/shell_state_facade.h"
#include "cli/shell/state/shell_state_owner.h"

#include <newdb/page_io.h>
#include <newdb/schema.h>
#include "cli/modules/txn/coordinator/txn_manager.h"
#include "test_util.h"
#include "shell_state_test_support.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
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

void init_shell_shared_dir(ShellStateOwner& own, ShellStateFacade& f, const fs::path& dir, const fs::path& log_name) {
    f.data_dir() = dir.string();
    f.log_file_path() = (dir / log_name).string();
    f.table_name().clear();
    f.data_path().clear();
    f.schema() = newdb::TableSchema{};
    f.bind_logging();
    f.txn().set_workspace_root(f.data_dir());
}

} // namespace

TEST(DemoWhereBatchDml, UpdateWhereInTxnCommitsAndDeletesWhere) {
    DemoHarness h("demo_uw_dw");
    h.run("CREATE TABLE(wbd)");
    h.run("USE(wbd)");
    h.run("DEFATTR(name:string,age:int)");
    h.run("INSERT(1,Alice,10)");
    h.run("INSERT(2,Bob,10)");
    h.run("BEGIN");
    const std::string uw = h.run("UPDATEWHERE(age, 77, WHERE, id, =, 1)");
    EXPECT_NE(uw.find("[UPDATEWHERE] ok:"), std::string::npos) << uw;
    const std::string cmt = h.run("COMMIT");
    EXPECT_NE(cmt.find("COMMIT"), std::string::npos) << cmt;

    const std::string chk = h.run("WHERE(age, =, 77)");
    EXPECT_NE(chk.find("matched"), std::string::npos) << chk;

    h.run("BEGIN");
    const std::string dw = h.run("DELETEWHERE(age, =, 77)");
    EXPECT_NE(dw.find("[DELETEWHERE] ok:"), std::string::npos) << dw;
    h.run("COMMIT");

    const std::string cnt = h.run("COUNT(age, =, 77)");
    EXPECT_NE(cnt.find("0 /"), std::string::npos) << cnt;
}

TEST(DemoWhereBatchDml, SetattrMultiInTxnRollbackRestores) {
    DemoHarness h("demo_setattr_multi_rb");
    h.run("CREATE TABLE(smr)");
    h.run("USE(smr)");
    h.run("DEFATTR(name:string,age:int)");
    h.run("INSERT(1,Alice,10)");
    h.run("INSERT(2,Bob,20)");
    h.run("BEGIN");
    const std::string sm = h.run("SETATTRMULTI(age, 99, 1, 2)");
    EXPECT_NE(sm.find("[SETATTRMULTI] ok:"), std::string::npos) << sm;
    const std::string rb = h.run("ROLLBACK");
    EXPECT_NE(rb.find("rolled back"), std::string::npos) << rb;

    {
        newdb::HeapTable disk;
        const newdb::TableSchema sch = h.f.schema();
        const std::string bin = (h.dir / "smr.bin").string();
        ASSERT_TRUE(newdb::io::load_heap_file(bin.c_str(), "smr", sch, disk).ok);
        const newdb::Row* ra = disk.find_by_id(1);
        const newdb::Row* rbrow = disk.find_by_id(2);
        ASSERT_NE(ra, nullptr);
        ASSERT_NE(rbrow, nullptr);
        const auto ia = ra->attrs.find("age");
        const auto ib = rbrow->attrs.find("age");
        ASSERT_NE(ia, ra->attrs.end()) << "id=1 missing age after ROLLBACK";
        ASSERT_NE(ib, rbrow->attrs.end()) << "id=2 missing age after ROLLBACK";
        EXPECT_EQ(ia->second, "10");
        EXPECT_EQ(ib->second, "20");
    }

    const std::string c10 = h.run("COUNT(age, =, 10)");
    EXPECT_NE(c10.find("1 /"), std::string::npos) << c10;
    const std::string c20 = h.run("COUNT(age, =, 20)");
    EXPECT_NE(c20.find("1 /"), std::string::npos) << c20;
    const std::string c99 = h.run("COUNT(age, =, 99)");
    EXPECT_NE(c99.find("0 /"), std::string::npos) << c99;
}

#if !defined(_WIN32)
// Two concurrent ShellState sessions on one workspace need overlapping txn BEGIN; Windows file-lock
// semantics often reject the second `BEGIN` on the same table path (see test_txn_write_conflict).
TEST(DemoWhereBatchDml, UpdateWhereWriteConflictSecondShell) {
    const fs::path dir = newdb::test::unique_temp_subdir("demo_uw_conflict");
    fs::create_directories(dir);

    ShellStateOwner own_a(make_shell_state_for_test());
    ShellStateFacade fa(own_a.shell());
    init_shell_shared_dir(own_a, fa, dir, "a.log");

    ASSERT_TRUE(process_command_line(own_a.shell(), "CREATE TABLE(wbx)"));
    ASSERT_TRUE(process_command_line(own_a.shell(), "USE(wbx)"));
    ASSERT_TRUE(process_command_line(own_a.shell(), "DEFATTR(name:string,age:int)"));
    ASSERT_TRUE(process_command_line(own_a.shell(), "INSERT(1,Alice,10)"));
    ASSERT_TRUE(process_command_line(own_a.shell(), "BEGIN"));
    ASSERT_TRUE(process_command_line(own_a.shell(), "UPDATEWHERE(age, 99, WHERE, id, =, 1)"));

    ShellStateOwner own_b(make_shell_state_for_test());
    ShellStateFacade fb(own_b.shell());
    init_shell_shared_dir(own_b, fb, dir, "b.log");
    ASSERT_TRUE(process_command_line(own_b.shell(), "USE(wbx)"));
    ASSERT_TRUE(process_command_line(own_b.shell(), "BEGIN"));

    const std::uintmax_t log_b_start = [&]() -> std::uintmax_t {
        std::error_code ec;
        const std::uintmax_t sz = fs::file_size(fb.log_file_path(), ec);
        return ec ? 0 : sz;
    }();
    ASSERT_TRUE(process_command_line(own_b.shell(), "UPDATEWHERE(age, 55, WHERE, id, =, 1)"));

    std::string tail_b;
    {
        std::ifstream in(fb.log_file_path(), std::ios::binary);
        ASSERT_TRUE(in.good());
        in.seekg(0, std::ios::end);
        const auto end = in.tellg();
        ASSERT_GT(end, 0);
        in.seekg(static_cast<std::streamoff>(log_b_start), std::ios::beg);
        tail_b.assign(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
    }
    EXPECT_NE(tail_b.find("[UPDATEWHERE]"), std::string::npos) << tail_b;
    EXPECT_NE(tail_b.find("write conflict"), std::string::npos) << tail_b;

    ASSERT_TRUE(process_command_line(own_a.shell(), "COMMIT"));

    const std::uintmax_t log_b2 = [&]() -> std::uintmax_t {
        std::error_code ec;
        const std::uintmax_t sz = fs::file_size(fb.log_file_path(), ec);
        return ec ? 0 : sz;
    }();
    ASSERT_TRUE(process_command_line(own_b.shell(), "UPDATEWHERE(age, 55, WHERE, id, =, 1)"));
    std::string tail_b2;
    {
        std::ifstream in(fb.log_file_path(), std::ios::binary);
        ASSERT_TRUE(in.good());
        in.seekg(static_cast<std::streamoff>(log_b2), std::ios::beg);
        tail_b2.assign(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
    }
    EXPECT_NE(tail_b2.find("[UPDATEWHERE] ok:"), std::string::npos) << tail_b2;

    ASSERT_TRUE(process_command_line(own_b.shell(), "COMMIT"));
}
#endif
