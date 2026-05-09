#include "cli/shell/bootstrap/demo_runner.h"

#include <cstdio>
#include <string>
#include <variant>
#include <vector>

#include <newdb/page_io.h>
#include <newdb/error_format.h>
#include <newdb/schema_io.h>

#include "cli/shell/bootstrap/demo_cli.h"
#include "cli/shell/bootstrap/demo_runner_cli_batch.h"
#include "cli/shell/dispatch/router/dispatch.h"
#include "cli/shell/diag/demo_diag.h"
#include "cli/shell/repl/demo_shell.h"
#include "cli/modules/import_export/import.h"
#include "cli/modules/common/logging/logging.h"
#include "cli/shell/state/shell_state.h"
#include "cli/shell/state/shell_state_facade.h"
#include "cli/shell/state/shell_state_ops.h"
#include "cli/modules/txn/coordinator/txn_manager.h"
namespace {

newdb::Status compact_table_file_local(const ShellStateFacade& app, const std::string& table_name) {
    if (table_name.empty()) {
        return newdb::Status::Fail("empty table name");
    }
    const std::string data_file = app.resolve_table_file(table_name + ".bin");
    newdb::TableSchema schema;
    (void)newdb::load_schema_text(newdb::schema_sidecar_path_for_data_file(data_file), schema);
    return newdb::io::compact_heap_file(data_file.c_str(), table_name, schema);
}

} // namespace

void demo_init_session_logging(ShellState& app,
                               const DemoCliWorkspace& ws,
                               const std::string& default_log_name,
                               bool encrypt_log,
                               bool verbose) {
    app.log_file_path() = demo_resolve_log_path(ws.data_dir, default_log_name);
    app.data_dir() = ws.data_dir;
    app.encrypt_log() = encrypt_log;
    app.verbose() = verbose;
    logging_bind_shell(&app);
    app.txn().set_workspace_root(app.data_dir());
    app.txn().setVacuumCallback([&app](const std::string& table_name) {
        ShellStateFacade vacuum_ctx(app);
        const newdb::Status st = compact_table_file_local(vacuum_ctx, table_name);
        if (st.ok) {
            logging_console_printf("[AUTOVACUUM] compacted table '%s'\n", table_name.c_str());
        } else {
            logging_console_printf("[AUTOVACUUM] table '%s' failed: %s\n", table_name.c_str(), st.message.c_str());
        }
    });
    (void)app.txn().recoverFromWAL();
    {
        const std::string hist = load_log_file_text(app.log_file_path().c_str());
        if (!hist.empty()) {
            logging_console_printf("===== HISTORY (%s) =====\n%s", app.log_file_path().c_str(), hist.c_str());
            if (!hist.empty() && hist.back() != '\n') {
                logging_console_printf("\n");
            }
            logging_console_printf("===== END HISTORY =====\n");
        }
    }
    log_session_separator(app.log_file_path().c_str());
}

int demo_try_run_terminal_phase(ShellState& app,
                                const DemoCliInvocation& inv,
                                const std::string& data_table,
                                const std::string& data_file_str) {
    ShellStateFacade app_f(app);
    if (const auto* mdb = std::get_if<CliRunMdb>(&inv.primary)) {
        // For MDB scripts, avoid implicit default table (e.g. users) because
        // scripts typically start with CREATE/USE and missing default tables
        // should not emit noisy load_failed logs.
        if (inv.ws.table_from_argv && !inv.ws.table_name.empty()) {
            app_f.table_name() = data_table;
        } else {
            app_f.table_name().clear();
            app_f.data_path().clear();
        }
        run_mdb_script(app, mdb->script_path.c_str());
        return 0;
    }

    if (const auto* idir = std::get_if<CliImportDir>(&inv.primary)) {
        (void)import_tables_from_directory(idir->folder_path.c_str(),
                                           app.data_dir().empty() ? nullptr : app.data_dir().c_str(),
                                           app.log_file_path().c_str());
        if (!inv.ws.table_name.empty()) {
            app_f.table_name() = data_table;
            app_f.data_path() = data_file_str;
            reload_schema_from_data_path(app, app_f.data_path());
        }
        return -1;
    }

    if (const auto* ex = std::get_if<CliExec>(&inv.primary)) {
        app_f.table_name() = data_table;
        app_f.data_path() = data_file_str;
        reload_schema_from_data_path(app, data_file_str);
        const newdb::Status rs = app_f.ensure_loaded();
        if (!rs.ok) {
            const std::string msg = newdb::format_error_line("session", "load_failed", rs.message);
            logging_stderr_printf("%s\n", msg.c_str());
            demo_verbose(app_f, "failed path: %s\n", app_f.data_path().c_str());
            return 1;
        }
        process_command_line(app, ex->command_line.c_str());
        return 0;
    }

    if (const auto* qb = std::get_if<CliBatchQueryBalance>(&inv.primary)) {
        return demo_run_cli_batch_query_balance(app, resolve_table_file(app, data_file_str), data_table, *qb);
    }

    if (const auto* bf = std::get_if<CliBatchFindId>(&inv.primary)) {
        return demo_run_cli_batch_find_id(app, resolve_table_file(app, data_file_str), data_table, *bf);
    }

    if (const auto* pg = std::get_if<CliBatchPage>(&inv.primary)) {
        return demo_run_cli_batch_page(app, resolve_table_file(app, data_file_str), data_table, *pg);
    }

    return -1;
}

void demo_preselect_default_table(ShellState& app,
                                  const DemoCliInvocation& inv,
                                  const std::string& data_table,
                                  const std::string& data_file_str) {
    if (!inv.ws.table_name.empty()) {
        ShellStateFacade f(app);
        f.table_name() = data_table;
        f.data_path() = data_file_str;
        reload_schema_from_data_path(app, f.data_path());
        demo_verbose(f, "preselected table=%s path=%s\n", data_table.c_str(), f.data_path().c_str());
    }
}
