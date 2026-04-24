#include "demo_cli.h"

#include <newdb/error_format.h>

#include <cstdlib>
#include <cstring>
#include <filesystem>

namespace fs = std::filesystem;

bool demo_argv_contains(int argc, char** argv, const char* flag) {
    for (int i = 1; i < argc; ++i) {
        if (!std::strcmp(argv[i], flag)) {
            return true;
        }
    }
    return false;
}

namespace {

std::string cli_err(const char* code, const std::string& message) {
    return newdb::format_error_line("cli", code, message);
}

int arg_index(int argc, char** argv, const char* flag) {
    for (int i = 1; i < argc; ++i) {
        if (!std::strcmp(argv[i], flag)) {
            return i;
        }
    }
    return -1;
}

void assign_primary(DemoCliInvocation& inv,
                    int argc,
                    char** argv,
                    bool has_run_mdb,
                    const std::string& run_mdb_path,
                    bool has_import_dir,
                    const std::string& import_dir_path,
                    bool has_exec_line,
                    const std::string& exec_line_one) {
    if (!inv.error.empty()) {
        return;
    }

    if (demo_argv_contains(argc, argv, "--dump-log")) {
        const int di = arg_index(argc, argv, "--dump-log");
        std::string dump_arg;
        if (di >= 0 && di + 1 < argc && argv[di + 1][0] != '-') {
            dump_arg = argv[di + 1];
        }
        inv.primary = CliDumpLog{std::move(dump_arg)};
        return;
    }

    if (has_run_mdb) {
        inv.primary = CliRunMdb{run_mdb_path};
        return;
    }

    if (has_import_dir) {
        inv.primary = CliImportDir{import_dir_path};
        return;
    }

    const bool has_exec_flag = demo_argv_contains(argc, argv, "--exec");
    if (has_exec_flag && has_exec_line) {
        inv.error = cli_err("arg_conflict", "--exec and --exec-line cannot be used together");
        return;
    }

    if (has_exec_line) {
        if (exec_line_one.empty()) {
            inv.error = cli_err("arg_invalid", "--exec-line requires a non-empty command");
            return;
        }
        inv.primary = CliExec{exec_line_one};
        return;
    }

    if (has_exec_flag) {
        std::string line;
        bool past = false;
        for (int i = 1; i < argc; ++i) {
            if (!past) {
                if (!std::strcmp(argv[i], "--exec")) {
                    past = true;
                }
                continue;
            }
            if (!line.empty()) {
                line += " ";
            }
            line += argv[i];
        }
        if (line.empty()) {
            inv.error = cli_err("arg_missing", "--exec requires a command (or use --exec-line)");
            return;
        }
        inv.primary = CliExec{std::move(line)};
        return;
    }

    const int iq = arg_index(argc, argv, "--query-balance");
    const int ifind = arg_index(argc, argv, "--find-id");
    const int ipage = arg_index(argc, argv, "--page");
    if (iq < 0 && ifind < 0 && ipage < 0) {
        inv.primary = CliInteractive{};
        return;
    }

    if (demo_count_batch_subcommands(argc, argv) > 1) {
        inv.error = cli_err("arg_conflict", "only one of --query-balance, --find-id, --page is allowed");
        return;
    }

    if (iq >= 0) {
        if (iq + 1 >= argc) {
            inv.error = cli_err("arg_missing", "--query-balance requires MIN_BAL");
            return;
        }
        inv.primary = CliBatchQueryBalance{std::atoi(argv[iq + 1])};
        return;
    }

    if (ifind >= 0) {
        if (ifind + 1 >= argc) {
            inv.error = cli_err("arg_missing", "--find-id requires ID");
            return;
        }
        inv.primary = CliBatchFindId{std::atoi(argv[ifind + 1])};
        return;
    }

    if (ipage >= 0) {
        if (ipage + 2 >= argc) {
            inv.error = cli_err("arg_missing", "--page requires PAGE PAGE_SIZE");
            return;
        }
        CliBatchPage page{};
        page.page_no = static_cast<std::size_t>(std::atoi(argv[ipage + 1]));
        page.page_size = static_cast<std::size_t>(std::atoi(argv[ipage + 2]));
        page.order_key = "id";
        page.descending = false;
        for (int i = ipage + 3; i < argc; ++i) {
            if (!std::strcmp(argv[i], "--order") && i + 1 < argc) {
                page.order_key = argv[i + 1];
                ++i;
            } else if (!std::strcmp(argv[i], "--desc")) {
                page.descending = true;
            } else if (!std::strcmp(argv[i], "--page-json")) {
                page.json_output = true;
            }
        }
        inv.primary = std::move(page);
        return;
    }

    inv.primary = CliInteractive{};
}

}  // namespace

void demo_parse_invocation(int argc, char** argv, DemoCliInvocation& out) {
    out = DemoCliInvocation{};
    bool has_run_mdb = false;
    std::string run_mdb_path;
    bool has_import_dir = false;
    std::string import_dir_path;
    bool has_exec_line = false;
    std::string exec_line_one;

    for (int i = 1; i < argc; ++i) {
        if (!std::strcmp(argv[i], "--data-dir")) {
            if (i + 1 >= argc) {
                out.error = cli_err("arg_missing", "--data-dir requires DIR");
                return;
            }
            out.ws.data_dir = argv[++i];
        } else if (!std::strcmp(argv[i], "--encrypted-log")) {
            out.encrypt_log = true;
        } else if (!std::strcmp(argv[i], "--table")) {
            if (i + 1 >= argc) {
                out.error = cli_err("arg_missing", "--table requires NAME");
                return;
            }
            out.ws.table_name = argv[++i];
            out.ws.table_from_argv = true;
        } else if (!std::strcmp(argv[i], "--verbose") || !std::strcmp(argv[i], "-v")) {
            out.verbose = true;
        } else if (!std::strcmp(argv[i], "--log-file")) {
            if (i + 1 >= argc) {
                out.error = cli_err("arg_missing", "--log-file requires PATH");
                return;
            }
            out.ws.log_file = argv[++i];
        } else if (!std::strcmp(argv[i], "--exec-line")) {
            if (i + 1 >= argc) {
                out.error = cli_err("arg_missing", "--exec-line requires CMD");
                return;
            }
            exec_line_one = argv[++i];
            has_exec_line = true;
        } else if (!std::strcmp(argv[i], "--run-mdb")) {
            if (has_import_dir) {
                out.error = cli_err("arg_conflict", "--run-mdb and --import-dir cannot be used together");
                return;
            }
            if (i + 1 >= argc) {
                out.error = cli_err("arg_missing", "--run-mdb requires PATH");
                return;
            }
            run_mdb_path = argv[++i];
            has_run_mdb = true;
        } else if (!std::strcmp(argv[i], "--import-dir")) {
            if (has_run_mdb) {
                out.error = cli_err("arg_conflict", "--run-mdb and --import-dir cannot be used together");
                return;
            }
            if (i + 1 >= argc) {
                out.error = cli_err("arg_missing", "--import-dir requires PATH");
                return;
            }
            import_dir_path = argv[++i];
            has_import_dir = true;
        }
    }

    if (!out.error.empty()) {
        return;
    }

    if (out.ws.data_dir.empty()) {
        const char* e = std::getenv("NEWDB_DATA_DIR");
        if (e != nullptr && e[0] != '\0') {
            out.ws.data_dir = e;
        }
    }
    if (out.ws.table_name.empty()) {
        const char* t = std::getenv("NEWDB_TABLE");
        if (t != nullptr && t[0] != '\0') {
            out.ws.table_name = t;
        }
    }
    if (out.ws.log_file.empty()) {
        const char* lg = std::getenv("NEWDB_LOG");
        if (lg != nullptr && lg[0] != '\0') {
            out.ws.log_file = lg;
        }
    }

    assign_primary(out,
                   argc,
                   argv,
                   has_run_mdb,
                   run_mdb_path,
                   has_import_dir,
                   import_dir_path,
                   has_exec_line,
                   exec_line_one);
}

std::string demo_default_log_spec(const DemoCliWorkspace& ws) {
    return ws.log_file.empty() ? std::string("demo_log.bin") : ws.log_file;
}

std::string demo_resolve_dump_log_path(const DemoCliWorkspace& ws, const std::string& dump_user_arg) {
    const std::string default_name = demo_default_log_spec(ws);
    if (dump_user_arg.empty()) {
        return demo_resolve_log_path(ws.data_dir, default_name);
    }
    if (fs::path(dump_user_arg).is_absolute()) {
        return demo_weakly_canonical_or_fallback(dump_user_arg);
    }
    return demo_resolve_log_path(ws.data_dir, dump_user_arg);
}

std::string demo_weakly_canonical_or_fallback(const std::string& path_str) {
    std::error_code ec;
    const fs::path p(path_str);
    const fs::path c = fs::weakly_canonical(p, ec);
    return ec ? p.string() : c.string();
}

std::string demo_resolve_log_path(const std::string& data_dir, const std::string& log_path) {
    std::error_code ec;
    fs::path p(log_path.empty() ? "demo_log.bin" : log_path);
    if (p.is_absolute()) {
        const fs::path c = fs::weakly_canonical(p, ec);
        return ec ? p.string() : c.string();
    }
    if (!data_dir.empty()) {
        const fs::path base = fs::absolute(data_dir, ec);
        return (base / p).lexically_normal().string();
    }
    const fs::path c = fs::absolute(p, ec);
    return ec ? (fs::current_path(ec) / p).lexically_normal().string() : c.string();
}

static bool arg_is(const char* a, const char* flag) {
    return a != nullptr && std::strcmp(a, flag) == 0;
}

int demo_count_batch_subcommands(int argc, char** argv) {
    int n = 0;
    for (int i = 1; i < argc; ++i) {
        if (arg_is(argv[i], "--query-balance") || arg_is(argv[i], "--find-id") || arg_is(argv[i], "--page")) {
            ++n;
        }
    }
    return n;
}
