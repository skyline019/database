#include <waterfall/config.h>

#include <array>
#include <functional>
#include <string>

#include "cli/shell/dispatch/router/dispatch.h"
#include "cli/shell/dispatch/router/dispatch_routing.h"
#include "cli/shell/dispatch/shared/dispatch_internal.h"
#include "cli/modules/common/logging/logging.h"
#include "cli/shell/state/shell_state_ops.h"
#include "cli/shell/state/shell_state_facade.h"

bool process_command_line(ShellState& st, const char* input_line) {
    ShellStateFacade f(st);
    std::string& current_table = f.table_name();
    std::string& current_file = f.data_path();
    const char* log_file = f.log_file_path().c_str();
    std::string line;
    if (input_line != nullptr) {
        line = input_line;
    }
    while (!line.empty() && (line.back() == '\n' || line.back() == '\r')) {
        line.pop_back();
    }
    if (line.empty()) {
        return true;
    }
    const char* line_cstr = line.c_str();

    f.bind_logging();
    const std::string eff_data = f.effective_data_path();
    append_session_log_line(log_file, line_cstr, f.encrypt_log());

    struct ShellHeapGuardClear {
        ShellStateFacade* f;
        ~ShellHeapGuardClear() {
            if (f != nullptr) {
                f->reset_session_heap_guard();
            }
        }
    } shell_heap_clear{&f};

    bool session_handled = false;
    const bool keep_going = handle_session_commands(line_cstr, log_file, session_handled);
    if (!keep_going) {
        return false;
    }
    if (session_handled) {
        return true;
    }

    // Phase-1: commands that do not require a loaded HeapTable (txn, DDL, catalog, insert, ...).
    if (!shell_line_targets_phase2_only(line_cstr)) {
        const std::array<std::function<bool()>, 8> phase1_handlers = {
            [&]() { return handle_txn_commands(st, line_cstr, log_file, current_table); },
            [&]() { return handle_workspace_admin_commands(st, line_cstr, log_file, eff_data, current_table, current_file); },
            [&]() { return handle_import_defattr_commands(st, line_cstr, log_file, eff_data, current_file); },
            [&]() { return handle_schema_catalog_commands(st, line_cstr, log_file); },
            [&]() { return handle_ddl_create_use_commands(st, line_cstr, log_file, eff_data, current_table, current_file); },
            [&]() { return handle_ddl_alter_rename_commands(st, line_cstr, log_file, eff_data, current_table, current_file); },
            [&]() { return handle_schema_show_commands(st, line_cstr, log_file, current_table, current_file); },
            [&]() { return handle_dml_insert_command(st, line_cstr, log_file, eff_data, current_table, current_file); },
        };
        for (const auto& h : phase1_handlers) {
            if (h()) {
                return true;
            }
        }
    }

    // Phase-2: need a loaded heap table (Session::lock_heap via ShellState guard).
    newdb::HeapTable* tbl_ptr = get_cached_table(st);
    if (!tbl_ptr) {
        return true;
    }
    newdb::HeapTable& tbl = *tbl_ptr;

    // Phase-2 dispatch chain: commands requiring loaded table cache.
    const std::array<std::function<bool()>, 9> phase2_handlers = {
        [&]() { return handle_schema_key_command(st, line_cstr, log_file, eff_data, tbl); },
        [&]() { return handle_query_where_count_commands(st, line_cstr, log_file, eff_data, current_table, tbl); },
        [&]() { return handle_dml_update_delete_commands(st, line_cstr, log_file, eff_data, current_table, tbl); },
        [&]() { return handle_dml_attr_commands(st, line_cstr, log_file, eff_data, current_table, tbl); },
        [&]() { return handle_query_find_commands(st, line_cstr, log_file, eff_data, tbl); },
        [&]() { return handle_query_sum_avg_commands(st, line_cstr, log_file, eff_data, tbl); },
        [&]() { return handle_query_min_max_commands(st, line_cstr, log_file, tbl); },
        [&]() { return handle_query_page_command(st, line_cstr, log_file, eff_data, tbl); },
        [&]() { return handle_export_command(st, line_cstr, log_file, current_table, current_file, tbl); },
    };
    for (const auto& h : phase2_handlers) {
        if (h()) {
            return true;
        }
    }

    log_and_print(log_file, "[ERR] unknown command. Type HELP.\n");
    return true;
}




