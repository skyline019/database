#pragma once

#include <cstddef>
#include <string>
#include <variant>

// Workspace + default table stem + session log path (argv first; env fills empties after parse).
struct DemoCliWorkspace {
    std::string data_dir;
    std::string table_name;
    std::string log_file;
    // True only when user provided `--table` explicitly (not from env NEWDB_TABLE).
    bool table_from_argv{false};
};

struct CliInteractive {};

// Optional path token after --dump-log; empty => use default log spec from workspace.
struct CliDumpLog {
    std::string user_path_arg;
};

struct CliRunMdb {
    std::string script_path;
};

struct CliImportDir {
    std::string folder_path;
};

struct CliExec {
    std::string command_line;
};

struct CliBatchQueryBalance {
    int min_balance{0};
};

struct CliBatchFindId {
    int id{0};
};

struct CliBatchPage {
    std::size_t page_no{0};
    std::size_t page_size{0};
    std::string order_key{"id"};
    bool descending{false};
    bool json_output{false};
};

using DemoPrimaryAction = std::variant<CliInteractive,
                                       CliDumpLog,
                                       CliRunMdb,
                                       CliImportDir,
                                       CliExec,
                                       CliBatchQueryBalance,
                                       CliBatchFindId,
                                       CliBatchPage>;

// Single parse result for newdb_demo: workspace + switches + exactly one primary action.
struct DemoCliInvocation {
    DemoCliWorkspace ws;
    bool encrypt_log{false};
    bool verbose{false};
    DemoPrimaryAction primary{CliInteractive{}};
    std::string error;
};

void demo_parse_invocation(int argc, char** argv, DemoCliInvocation& out);

// Default session log file stem/spec from workspace (before absolute resolve).
std::string demo_default_log_spec(const DemoCliWorkspace& ws);

// Resolved filesystem path for `newdb_demo --dump-log [PATH]`.
std::string demo_resolve_dump_log_path(const DemoCliWorkspace& ws, const std::string& dump_user_arg);

std::string demo_resolve_log_path(const std::string& data_dir, const std::string& log_path);

std::string demo_weakly_canonical_or_fallback(const std::string& path_str);

int demo_count_batch_subcommands(int argc, char** argv);

// Shared by demo_main (e.g. --help) and demo_cli.cpp.
bool demo_argv_contains(int argc, char** argv, const char* flag);
