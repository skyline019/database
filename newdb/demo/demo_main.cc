#include <waterfall/config.h>

#include <cstdio>
#include <cstring>
#include <string>
#include <variant>

#include "demo_cli.h"
#include "demo_runner.h"
#include "demo_shell.h"
#include "logging.h"
#include "shell_state.h"

#ifndef NEWDB_DISPLAY_PROJECT
#define NEWDB_DISPLAY_PROJECT "newdb"
#endif

int main(int argc, char** argv) {
    std::printf("Project: %s (demo)\n", NEWDB_DISPLAY_PROJECT);

    if (demo_argv_contains(argc, argv, "--help") || demo_argv_contains(argc, argv, "-h")) {
        std::printf("Options (anywhere in argv):\n");
        std::printf("  --data-dir DIR      workspace for *.bin / .attr / .schema_history\n");
        std::printf("  --table NAME        default table stem (default: users); see NEWDB_TABLE\n");
        std::printf("  --log-file PATH     session log file (relative paths under --data-dir); NEWDB_LOG\n");
        std::printf("  --encrypted-log     legacy XOR-framed session log (default: plain UTF-8 lines)\n");
        std::printf("  -v, --verbose       extra diagnostics on stderr (paths, load errors)\n");
        std::printf("  --exec CMD ...      run one shell command (remaining argv after --exec)\n");
        std::printf("  --exec-line CMD     same as --exec but a single argv token\n");
        std::printf("  --dump-log [PATH]   dump log file (default: resolved --log-file / demo_log.bin)\n");
        std::printf("  --run-mdb PATH      run script at PATH (not with --import-dir)\n");
        std::printf("  --import-dir PATH   import tables from folder, then shell (not with --run-mdb)\n");
        std::printf("Batch (only one of the following per invocation):\n");
        std::printf("  --query-balance MIN_BAL\n");
        std::printf("  --find-id ID\n");
        std::printf("  --page PAGE PAGE_SIZE [--order KEY] [--desc] [--page-json]\n");
        std::printf("Environment:\n");
        std::printf("  NEWDB_DATA_DIR      workspace if --data-dir omitted\n");
        std::printf("  NEWDB_TABLE         default table if --table omitted\n");
        std::printf("  NEWDB_LOG           default session log path if --log-file omitted\n");
        return 0;
    }

    DemoCliInvocation inv;
    demo_parse_invocation(argc, argv, inv);
    if (!inv.error.empty()) {
        logging_stderr_printf("%s\n", inv.error.c_str());
        return 1;
    }

    const std::string data_table =
        inv.ws.table_name.empty() ? std::string("users") : inv.ws.table_name;
    const std::string data_file_str = data_table + ".bin";
    const std::string default_log_name = demo_default_log_spec(inv.ws);

    if (const auto* dump = std::get_if<CliDumpLog>(&inv.primary)) {
        const std::string resolved_dump = demo_resolve_dump_log_path(inv.ws, dump->user_path_arg);
        dump_log_file(resolved_dump.c_str());
        return 0;
    }

    ShellState app;
    demo_init_session_logging(app, inv.ws, default_log_name, inv.encrypt_log, inv.verbose);

    const int phase = demo_try_run_terminal_phase(app, inv, data_table, data_file_str);
    if (phase >= 0) {
        return phase;
    }

    demo_preselect_default_table(app, inv, data_table, data_file_str);
    interactive_shell(app, nullptr, nullptr);
    return 0;
}
