#include "demo_runner.h"

#include <cstdio>
#include <string>
#include <variant>
#include <vector>

#include <newdb/page_io.h>
#include <newdb/error_format.h>
#include <newdb/schema_io.h>
#include <newdb/session.h>

#include "demo_cli.h"
#include "demo_commands.h"
#include "demo_diag.h"
#include "demo_shell.h"
#include "import.h"
#include "logging.h"
#include "page_index_sidecar.h"
#include "shell_state.h"
#include "table_view.h"

namespace {

std::string json_escape_local(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 8);
    for (unsigned char c : s) {
        switch (c) {
            case '\"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\b': out += "\\b"; break;
            case '\f': out += "\\f"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (c < 0x20) {
                    char buf[7];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned int>(c));
                    out += buf;
                } else {
                    out.push_back(static_cast<char>(c));
                }
        }
    }
    return out;
}

bool row_at_slot_read_local(const newdb::HeapTable& tbl, const std::size_t i, newdb::Row& r) {
    if (tbl.is_heap_storage_backed()) {
        return tbl.decode_heap_slot(i, r);
    }
    r = tbl.rows[i];
    return true;
}

void print_page_json(const newdb::TableSchema& schema,
                     const newdb::HeapTable& tbl,
                     const std::vector<std::size_t>& sorted_idx,
                     const std::size_t page_no,
                     const std::size_t page_size,
                     const std::string& order_key,
                     const bool desc) {
    const std::size_t total = sorted_idx.size();
    const std::size_t total_pages = page_size == 0 ? 0 : (total + page_size - 1) / page_size;
    std::printf("[PAGE_JSON] {");
    std::printf("\"table\":\"%s\",", json_escape_local(tbl.name).c_str());
    std::printf("\"order\":\"%s\",", json_escape_local(order_key).c_str());
    std::printf("\"descending\":%s,", desc ? "true" : "false");
    std::printf("\"page_no\":%zu,\"page_size\":%zu,\"total\":%zu,\"total_pages\":%zu,", page_no, page_size, total, total_pages);

    std::printf("\"headers\":[\"id\"");
    for (const auto& m : schema.attrs) {
        if (m.name == "id") continue;
        std::printf(",\"%s\"", json_escape_local(m.name).c_str());
    }
    std::printf("],");

    std::printf("\"rows\":[");
    if (page_size > 0 && page_no > 0 && page_no <= total_pages) {
        const std::size_t begin = (page_no - 1) * page_size;
        const std::size_t end = std::min(begin + page_size, total);
        bool first_row = true;
        for (std::size_t p = begin; p < end; ++p) {
            newdb::Row r;
            if (!row_at_slot_read_local(tbl, sorted_idx[p], r)) continue;
            if (!first_row) std::printf(",");
            first_row = false;
            std::printf("[\"%d\"", r.id);
            for (const auto& m : schema.attrs) {
                if (m.name == "id") continue;
                const auto it = r.attrs.find(m.name);
                const std::string v = (it == r.attrs.end()) ? std::string() : it->second;
                std::printf(",\"%s\"", json_escape_local(v).c_str());
            }
            std::printf("]");
        }
    }
    std::printf("]}");
    std::printf("\n");
}

} // namespace

void demo_init_session_logging(ShellState& app,
                               const DemoCliWorkspace& ws,
                               const std::string& default_log_name,
                               bool encrypt_log,
                               bool verbose) {
    app.log_file_path = demo_resolve_log_path(ws.data_dir, default_log_name);
    app.data_dir = ws.data_dir;
    app.encrypt_log = encrypt_log;
    app.verbose = verbose;
    logging_bind_shell(&app);
    app.txn.set_workspace_root(app.data_dir);
    (void)app.txn.recoverFromWAL();
    {
        const std::string hist = load_log_file_text(app.log_file_path.c_str());
        if (!hist.empty()) {
            logging_console_printf("===== HISTORY (%s) =====\n%s", app.log_file_path.c_str(), hist.c_str());
            if (!hist.empty() && hist.back() != '\n') {
                logging_console_printf("\n");
            }
            logging_console_printf("===== END HISTORY =====\n");
        }
    }
    log_session_separator(app.log_file_path.c_str());
}

int demo_try_run_terminal_phase(ShellState& app,
                                const DemoCliInvocation& inv,
                                const std::string& data_table,
                                const std::string& data_file_str) {
    if (const auto* mdb = std::get_if<CliRunMdb>(&inv.primary)) {
        // For MDB scripts, avoid implicit default table (e.g. users) because
        // scripts typically start with CREATE/USE and missing default tables
        // should not emit noisy load_failed logs.
        if (inv.ws.table_from_argv && !inv.ws.table_name.empty()) {
            app.session.table_name = data_table;
        } else {
            app.session.table_name.clear();
            app.session.data_path.clear();
        }
        run_mdb_script(app, mdb->script_path.c_str());
        return 0;
    }

    if (const auto* idir = std::get_if<CliImportDir>(&inv.primary)) {
        (void)import_tables_from_directory(idir->folder_path.c_str(),
                                           app.data_dir.empty() ? nullptr : app.data_dir.c_str(),
                                           app.log_file_path.c_str());
        if (!inv.ws.table_name.empty()) {
            app.session.table_name = data_table;
            app.session.data_path = data_file_str;
            reload_schema_from_data_path(app, app.session.data_path);
        }
        return -1;
    }

    if (const auto* ex = std::get_if<CliExec>(&inv.primary)) {
        app.session.table_name = data_table;
        app.session.data_path = data_file_str;
        reload_schema_from_data_path(app, data_file_str);
        const newdb::Status rs = app.session.ensure_loaded();
        if (!rs.ok) {
            const std::string msg = newdb::format_error_line("session", "load_failed", rs.message);
            logging_stderr_printf("%s\n", msg.c_str());
            demo_verbose(app, "failed path: %s\n", app.session.data_path.c_str());
            return 1;
        }
        process_command_line(app, ex->command_line.c_str());
        return 0;
    }

    if (const auto* qb = std::get_if<CliBatchQueryBalance>(&inv.primary)) {
        newdb::Session batch;
        batch.data_path = resolve_table_file(app, data_file_str);
        batch.table_name = data_table;
        const newdb::Status lds = batch.reload();
        if (!lds.ok) {
            const std::string msg = newdb::format_error_line("session", "load_failed", lds.message);
            logging_stderr_printf("%s\n", msg.c_str());
            demo_verbose(app, "batch data_path=%s table=%s\n", batch.data_path.c_str(), batch.table_name.c_str());
            return 1;
        }
        newdb::io::query_attr_int_ge(batch.data_path.c_str(),
                                     batch.schema.default_int_predicate_attr().c_str(),
                                     qb->min_balance);
        return 0;
    }

    if (const auto* bf = std::get_if<CliBatchFindId>(&inv.primary)) {
        newdb::Session batch;
        batch.data_path = resolve_table_file(app, data_file_str);
        batch.table_name = data_table;
        const newdb::Status lds = batch.reload();
        if (!lds.ok) {
            const std::string msg = newdb::format_error_line("session", "load_failed", lds.message);
            logging_stderr_printf("%s\n", msg.c_str());
            demo_verbose(app, "batch data_path=%s table=%s\n", batch.data_path.c_str(), batch.table_name.c_str());
            return 1;
        }
        newdb::HeapTable& tbl = batch.table;
        const newdb::Row* r = tbl.find_by_id(bf->id);
        if (r) {
            logging_console_printf("[FIND] table=%s id=%d", tbl.name.c_str(), r->id);
            for (const auto& kv : r->attrs) {
                logging_console_printf(" %s=%s", kv.first.c_str(), kv.second.c_str());
            }
            logging_console_printf("\n");
            return 0;
        }
        logging_stderr_printf("[FIND] id=%d not found in table=%s\n", bf->id, tbl.name.c_str());
        return 1;
    }

    if (const auto* pg = std::get_if<CliBatchPage>(&inv.primary)) {
        newdb::Session batch;
        batch.data_path = resolve_table_file(app, data_file_str);
        batch.table_name = data_table;
        const newdb::Status lds = batch.reload();
        if (!lds.ok) {
            const std::string msg = newdb::format_error_line("session", "load_failed", lds.message);
            logging_stderr_printf("%s\n", msg.c_str());
            demo_verbose(app, "batch data_path=%s table=%s\n", batch.data_path.c_str(), batch.table_name.c_str());
            return 1;
        }
        newdb::HeapTable& tbl = batch.table;
        const newdb::SortDir dir =
            pg->descending ? newdb::SortDir::Desc : newdb::SortDir::Asc;
        const std::vector<std::size_t> sorted_idx = load_or_build_page_index_sidecar(
            PageSidecarRequest{
                .data_file = batch.data_path,
                .table_name = batch.table_name,
                .order_key = pg->order_key,
                .descending = pg->descending,
            },
            batch.schema,
            tbl);
        logging_console_printf("[PAGE] table=%s order_by=%s %s\n",
                               tbl.name.c_str(),
                               pg->order_key.c_str(),
                               dir == newdb::SortDir::Asc ? "asc" : "desc");
        if (pg->json_output) {
            print_page_json(batch.schema, tbl, sorted_idx, pg->page_no, pg->page_size, pg->order_key, pg->descending);
        } else {
            table_view::print_page_indexed(batch.schema, tbl, sorted_idx, pg->page_no, pg->page_size);
        }
        return 0;
    }

    return -1;
}

void demo_preselect_default_table(ShellState& app,
                                  const DemoCliInvocation& inv,
                                  const std::string& data_table,
                                  const std::string& data_file_str) {
    if (!inv.ws.table_name.empty()) {
        app.session.table_name = data_table;
        app.session.data_path = data_file_str;
        reload_schema_from_data_path(app, app.session.data_path);
        demo_verbose(app, "preselected table=%s path=%s\n", data_table.c_str(), app.session.data_path.c_str());
    }
}
