#include "demo_cli.h"
#include "csv_export.h"

#include <filesystem>
#include <gtest/gtest.h>

namespace fs = std::filesystem;

TEST(DemoCli, ResolveLogRelativeUnderDataDir) {
    const std::string data_dir = (fs::temp_directory_path() / "newdb_workspace_cli").string();
    const std::string got = demo_resolve_log_path(data_dir, "session.bin");
    const fs::path want = (fs::path(data_dir) / "session.bin").lexically_normal();
    EXPECT_EQ(fs::path(got), want);
}

TEST(DemoCli, ResolveLogEmptyMeansDefaultFileName) {
    const std::string got = demo_resolve_log_path("", "");
    EXPECT_TRUE(fs::path(got).filename() == "demo_log.bin");
}

TEST(DemoCli, ParseArgvSetsWorkspaceAndFlags) {
    char a0[] = "newdb_demo";
    char f1[] = "--data-dir";
    char v1[] = "/w";
    char f2[] = "--table";
    char v2[] = "t1";
    char f3[] = "--log-file";
    char v3[] = "logs/x.bin";
    char f4[] = "--verbose";
    char* argv[] = {a0, f1, v1, f2, v2, f3, v3, f4, nullptr};
    DemoCliInvocation inv;
    demo_parse_invocation(8, argv, inv);
    EXPECT_TRUE(inv.error.empty());
    EXPECT_EQ(inv.ws.data_dir, "/w");
    EXPECT_EQ(inv.ws.table_name, "t1");
    EXPECT_EQ(inv.ws.log_file, "logs/x.bin");
    EXPECT_TRUE(inv.verbose);
    EXPECT_TRUE(std::holds_alternative<CliInteractive>(inv.primary));
}

TEST(DemoCli, ParseExecLineBecomesExec) {
    char a0[] = "newdb_demo";
    char f1[] = "--exec-line";
    char v1[] = "HELP";
    char* argv[] = {a0, f1, v1, nullptr};
    DemoCliInvocation inv;
    demo_parse_invocation(3, argv, inv);
    ASSERT_TRUE(inv.error.empty());
    const auto* ex = std::get_if<CliExec>(&inv.primary);
    ASSERT_NE(ex, nullptr);
    EXPECT_EQ(ex->command_line, "HELP");
}

TEST(DemoCli, ParseErrorWhenLogFileWithoutOperand) {
    char a0[] = "newdb_demo";
    char f[] = "--log-file";
    char* argv[] = {a0, f, nullptr};
    DemoCliInvocation inv;
    demo_parse_invocation(2, argv, inv);
    ASSERT_FALSE(inv.error.empty());
    EXPECT_NE(inv.error.find("[ERR] domain=cli code=arg_missing"), std::string::npos);
    EXPECT_NE(inv.error.find("--log-file requires PATH"), std::string::npos);
}

TEST(DemoCli, ParseErrorWhenDataDirWithoutOperand) {
    char a0[] = "x";
    char f[] = "--data-dir";
    char* argv[] = {a0, f, nullptr};
    DemoCliInvocation inv;
    demo_parse_invocation(2, argv, inv);
    ASSERT_FALSE(inv.error.empty());
}

TEST(DemoCli, ParseErrorWhenTableWithoutOperand) {
    char a0[] = "x";
    char f[] = "--table";
    char* argv[] = {a0, f, nullptr};
    DemoCliInvocation inv;
    demo_parse_invocation(2, argv, inv);
    ASSERT_FALSE(inv.error.empty());
}

TEST(DemoCli, ParseErrorWhenExecLineWithoutOperand) {
    char a0[] = "x";
    char f[] = "--exec-line";
    char* argv[] = {a0, f, nullptr};
    DemoCliInvocation inv;
    demo_parse_invocation(2, argv, inv);
    ASSERT_FALSE(inv.error.empty());
}

TEST(DemoCli, ParseRunMdbAndImportDir) {
    char a0[] = "x";
    char f1[] = "--run-mdb";
    char v1[] = "/scripts/a.mdb";
    char f2[] = "--import-dir";
    char v2[] = "/in";
    {
        char* argv[] = {a0, f1, v1, nullptr};
        DemoCliInvocation inv;
        demo_parse_invocation(3, argv, inv);
        ASSERT_TRUE(inv.error.empty());
        const auto* p = std::get_if<CliRunMdb>(&inv.primary);
        ASSERT_NE(p, nullptr);
        EXPECT_EQ(p->script_path, "/scripts/a.mdb");
    }
    {
        char* argv[] = {a0, f2, v2, nullptr};
        DemoCliInvocation inv;
        demo_parse_invocation(3, argv, inv);
        ASSERT_TRUE(inv.error.empty());
        const auto* p = std::get_if<CliImportDir>(&inv.primary);
        ASSERT_NE(p, nullptr);
        EXPECT_EQ(p->folder_path, "/in");
    }
}

TEST(DemoCli, ParseErrorRunMdbWithoutOperand) {
    char a0[] = "x";
    char f[] = "--run-mdb";
    char* argv[] = {a0, f, nullptr};
    DemoCliInvocation inv;
    demo_parse_invocation(2, argv, inv);
    ASSERT_FALSE(inv.error.empty());
}

TEST(DemoCli, ParseErrorImportDirWithoutOperand) {
    char a0[] = "x";
    char f[] = "--import-dir";
    char* argv[] = {a0, f, nullptr};
    DemoCliInvocation inv;
    demo_parse_invocation(2, argv, inv);
    ASSERT_FALSE(inv.error.empty());
}

TEST(DemoCli, ParseErrorRunMdbAndImportDirTogether) {
    char a0[] = "x";
    char f1[] = "--run-mdb";
    char v1[] = "/a.mdb";
    char f2[] = "--import-dir";
    char v2[] = "/in";
    char* argv[] = {a0, f1, v1, f2, v2, nullptr};
    DemoCliInvocation inv;
    demo_parse_invocation(5, argv, inv);
    ASSERT_FALSE(inv.error.empty());
    EXPECT_NE(inv.error.find("[ERR] domain=cli code=arg_conflict"), std::string::npos);
}

TEST(DemoCli, ParseDumpLogPrimary) {
    char a0[] = "x";
    char d[] = "--dump-log";
    char* argv[] = {a0, d, nullptr};
    DemoCliInvocation inv;
    demo_parse_invocation(2, argv, inv);
    ASSERT_TRUE(inv.error.empty());
    const auto* p = std::get_if<CliDumpLog>(&inv.primary);
    ASSERT_NE(p, nullptr);
    EXPECT_TRUE(p->user_path_arg.empty());
}

TEST(DemoCli, ParseBatchQueryBalance) {
    char a0[] = "x";
    char q[] = "--query-balance";
    char v[] = "5";
    char* argv[] = {a0, q, v, nullptr};
    DemoCliInvocation inv;
    demo_parse_invocation(3, argv, inv);
    ASSERT_TRUE(inv.error.empty());
    const auto* p = std::get_if<CliBatchQueryBalance>(&inv.primary);
    ASSERT_NE(p, nullptr);
    EXPECT_EQ(p->min_balance, 5);
}

TEST(DemoCli, ParseBatchPageOrderDesc) {
    char a0[] = "x";
    char p[] = "--page";
    char p1[] = "0";
    char p2[] = "10";
    char o[] = "--order";
    char ok[] = "balance";
    char d[] = "--desc";
    char* argv[] = {a0, p, p1, p2, o, ok, d, nullptr};
    DemoCliInvocation inv;
    demo_parse_invocation(7, argv, inv);
    ASSERT_TRUE(inv.error.empty());
    const auto* pg = std::get_if<CliBatchPage>(&inv.primary);
    ASSERT_NE(pg, nullptr);
    EXPECT_EQ(pg->page_no, 0u);
    EXPECT_EQ(pg->page_size, 10u);
    EXPECT_EQ(pg->order_key, "balance");
    EXPECT_TRUE(pg->descending);
}

TEST(DemoCli, WeaklyCanonicalCurrentDirNoThrow) {
    const std::string r = demo_weakly_canonical_or_fallback(".");
    EXPECT_FALSE(r.empty());
}

TEST(DemoCli, CountBatchSubcommands) {
    char a0[] = "x";
    char q[] = "--query-balance";
    char qb[] = "10";
    char p[] = "--page";
    char p1[] = "0";
    char p2[] = "5";
    {
        char* argv[] = {a0, q, qb, nullptr};
        EXPECT_EQ(demo_count_batch_subcommands(3, argv), 1);
    }
    {
        char f[] = "--find-id";
        char id[] = "3";
        char* argv[] = {a0, q, qb, f, id, nullptr};
        EXPECT_EQ(demo_count_batch_subcommands(5, argv), 2);
    }
    {
        char* argv[] = {a0, p, p1, p2, nullptr};
        EXPECT_EQ(demo_count_batch_subcommands(4, argv), 1);
    }
}

TEST(CsvExport, EscapePlain) {
    EXPECT_EQ(csv_escape_cell("abc"), "abc");
}

TEST(CsvExport, EscapeCommaQuotesNewline) {
    EXPECT_EQ(csv_escape_cell("a,b"), "\"a,b\"");
    EXPECT_EQ(csv_escape_cell("say \"x\""), "\"say \"\"x\"\"\"");
    EXPECT_EQ(csv_escape_cell("a\nb"), "\"a\nb\"");
}
