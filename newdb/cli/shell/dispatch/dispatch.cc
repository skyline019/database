#include <waterfall/config.h>

#include <array>
#include <cstring>
#include <functional>
#include <string>

#include "cli/shell/dispatch/demo_commands.h"
#include "cli/shell/dispatch/internal/dispatch_internal.h"
#include "cli/modules/logging/logging.h"
#include "cli/shell/state/shell_state.h"

bool process_command_line(ShellState& st, const char* input_line) {
    std::string& current_table = st.session.table_name;
    std::string& current_file = st.session.data_path;
    const char* log_file = st.log_file_path.c_str();
    char line[512];
    std::strncpy(line, input_line, sizeof(line) - 1);
    line[sizeof(line) - 1] = '\0';

    // ??????
    std::size_t len = std::strlen(line);
    while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r')) {
        line[--len] = '\0';
    }
    if (len == 0) return true;

    logging_bind_shell(&st);
    const std::string eff_data = effective_data_path(st);
    append_session_log_line(log_file, line, st.encrypt_log);

    struct ShellHeapGuardClear {
        ShellState* st;
        ~ShellHeapGuardClear() {
            if (st != nullptr) {
                st->session_heap_guard.reset();
            }
        }
    } shell_heap_clear{&st};

    bool session_handled = false;
    const bool keep_going = handle_session_commands(line, log_file, session_handled);
    if (!keep_going) {
        return false;
    }
    if (session_handled) {
        return true;
    }

    // Phase-1 dispatch chain: commands that don't require loaded heap table.
    const std::array<std::function<bool()>, 8> phase1_handlers = {
        [&]() { return handle_txn_commands(st, line, log_file, current_table); },
        [&]() { return handle_workspace_admin_commands(st, line, log_file, eff_data, current_table, current_file); },
        [&]() { return handle_import_defattr_commands(st, line, log_file, eff_data, current_file); },
        [&]() { return handle_schema_catalog_commands(st, line, log_file); },
        [&]() { return handle_ddl_create_use_commands(st, line, log_file, eff_data, current_table, current_file); },
        [&]() { return handle_ddl_alter_rename_commands(st, line, log_file, eff_data, current_table, current_file); },
        [&]() { return handle_schema_show_commands(st, line, log_file, current_table, current_file); },
        [&]() { return handle_dml_insert_command(st, line, log_file, eff_data, current_table, current_file); },
    };
    for (const auto& h : phase1_handlers) {
        if (h()) {
            return true;
        }
    }

    // ??????????????????????????????
    newdb::HeapTable* tbl_ptr = get_cached_table(st);
    if (!tbl_ptr) {
        return true;
    }
    newdb::HeapTable& tbl = *tbl_ptr;

    // Phase-2 dispatch chain: commands requiring loaded table cache.
    const std::array<std::function<bool()>, 9> phase2_handlers = {
        [&]() { return handle_schema_key_command(st, line, log_file, eff_data, tbl); },
        [&]() { return handle_query_where_count_commands(st, line, log_file, eff_data, current_table, tbl); },
        [&]() { return handle_dml_update_delete_commands(st, line, log_file, eff_data, current_table, tbl); },
        [&]() { return handle_dml_attr_commands(st, line, log_file, eff_data, current_table, tbl); },
        [&]() { return handle_query_find_commands(st, line, log_file, eff_data, tbl); },
        [&]() { return handle_query_sum_avg_commands(st, line, log_file, eff_data, tbl); },
        [&]() { return handle_query_min_max_commands(st, line, log_file, tbl); },
        [&]() { return handle_query_page_command(st, line, log_file, eff_data, tbl); },
        [&]() { return handle_export_command(st, line, log_file, current_table, current_file, tbl); },
    };
    for (const auto& h : phase2_handlers) {
        if (h()) {
            return true;
        }
    }

    log_and_print(log_file, "[ERR] unknown command. Type HELP.\n");
    return true;
}




