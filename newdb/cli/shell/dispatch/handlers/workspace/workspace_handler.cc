#include <waterfall/config.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include <newdb/page_io.h>

#include "cli/modules/sidecar/covering/covering_index_sidecar.h"
#include "cli/shell/dispatch/shared/dispatch_internal.h"
#include "cli/modules/import_export/demo_export.h"
#include "cli/modules/import_export/import.h"
#include "cli/modules/common/logging/logging.h"
#include "cli/modules/sidecar/page/page_index_sidecar.h"
#include "cli/modules/catalog/schema_catalog.h"
#include "cli/shell/state/shell_state.h"
#include "cli/modules/common/view/table_view.h"
#include "cli/modules/txn/coordinator/txn_manager.h"
#include "cli/modules/common/util/utils.h"
#include "cli/modules/where/executor/where.h"
#include "cli/shell/dispatch/services/lsm/lsm_lite_service.h"

bool handle_workspace_admin_commands(ShellState& st,
                                     const char* line,
                                     const char* log_file,
                                     const std::string& eff_data,
                                     std::string& current_table,
                                     std::string& current_file) {
    if (strcasecmp_ascii(line, "LIST TABLES") == 0 || strcasecmp_ascii(line, "SHOW TABLES") == 0) {
        namespace fs = std::filesystem;
        std::error_code ec;
        const fs::path cwd = workspace_directory(st);
        std::vector<std::string> tables;
        for (fs::directory_iterator it(cwd, ec), end; !ec && it != end; it.increment(ec)) {
            const fs::directory_entry& ent = *it;
            if (!ent.is_regular_file(ec)) continue;
            fs::path p = ent.path();
            if (p.extension() != ".bin") continue;
            std::string filename = p.filename().string();
            std::string stem = p.stem().string();
            bool looks_like_log = (filename == std::filesystem::path(st.log_file_path).filename().string());
            if (!looks_like_log) {
                const std::string suf = "_log";
                if (stem.size() >= suf.size() && stem.compare(stem.size() - suf.size(), suf.size(), suf) == 0) {
                    looks_like_log = true;
                }
            }
            if (!looks_like_log) tables.push_back(p.stem().string());
        }
        std::sort(tables.begin(), tables.end());
        if (tables.empty()) {
            log_and_print(log_file, "[LIST] no tables (*.bin) in workspace.\n");
            return true;
        }
        log_and_print(log_file, "[LIST] tables (%zu):\n", tables.size());
        for (const auto& t : tables) {
            const std::string df = resolve_table_file(st, t + ".bin");
            const std::string af = newdb::schema_sidecar_path_for_data_file(df);
            std::error_code ec1, ec2;
            bool has_attr = fs::exists(af, ec1);
            std::uintmax_t sz = fs::file_size(df, ec2);
            log_and_print(log_file, "  %s%s  (%s, %zu bytes)%s\n",
                          (t == current_table ? "* " : "  "), t.c_str(), df.c_str(),
                          static_cast<std::size_t>(ec2 ? 0 : sz), (has_attr ? "  [+attr]" : ""));
        }
        return true;
    }
    if (strncasecmp_ascii(line, "DROP TABLE", 10) == 0) {
        std::vector<std::string> args;
        if (!parse_comma_args(line + 10, args) || args.size() != 1) {
            log_and_print(log_file, "[DROP] usage: DROP TABLE(name)\n");
            return true;
        }
        std::string tname = args[0];
        const std::string df = resolve_table_file(st, tname + ".bin");
        const std::string af = newdb::schema_sidecar_path_for_data_file(df);
        namespace fs = std::filesystem;
        std::error_code ec1, ec2;
        bool removed_df = fs::remove(df, ec1);
        bool removed_af = fs::remove(af, ec2);
        if (!removed_df) {
            log_and_print(log_file, "[DROP] table '%s' not found or cannot delete (file=%s)\n", tname.c_str(), df.c_str());
            return true;
        }
        if (tname == current_table) {
            current_table.clear();
            current_file.clear();
            st.session.schema = newdb::TableSchema{};
        }
        lsm_lite_clear_txn_views(st, df);
        log_and_print(log_file, "[DROP] ok: table '%s' dropped (data=%s%s)\n",
                      tname.c_str(), df.c_str(), (removed_af ? ", attr deleted" : ", attr not found"));
        shell_invalidate_session_table(st);
        return true;
    }
    if (strcasecmp_ascii(line, "SHOWLOG") == 0) {
        dump_log_file(log_file);
        return true;
    }
    if (strcasecmp_ascii(line, "RESET") == 0) {
        if (current_file.empty()) {
            log_and_print(log_file, "[RESET] no table selected. Use CREATE TABLE or USE first.\n");
            return true;
        }
        std::vector<newdb::Row> rows;
        if (newdb::io::create_heap_file(eff_data.c_str(), rows)) {
            newdb::save_schema_text(newdb::schema_sidecar_path_for_data_file(eff_data), st.session.schema);
            log_and_print(log_file, "[RESET] empty table '%s' recreated (file=%s).\n", current_table.c_str(), current_file.c_str());
        } else {
            log_and_print(log_file, "[RESET] failed.\n");
        }
        lsm_lite_clear_txn_views(st, eff_data);
        shell_invalidate_session_table(st);
        return true;
    }
    if (strcasecmp_ascii(line, "VACUUM") == 0) {
        if (current_file.empty()) {
            log_and_print(log_file, "[VACUUM] no table selected. Use CREATE TABLE or USE first.\n");
            return true;
        }
        std::size_t rows_after = 0;
        const newdb::Status vst =
            newdb::io::compact_heap_file(eff_data.c_str(), current_table, st.session.schema, &rows_after);
        if (vst.ok) {
            (void)newdb::save_schema_text(newdb::schema_sidecar_path_for_data_file(eff_data), st.session.schema);
            log_and_print(log_file, "[VACUUM] compacted table '%s' (file=%s), rows=%zu.\n",
                          current_table.c_str(), current_file.c_str(), rows_after);
        } else {
            log_and_print(log_file, "[VACUUM] failed to compact table '%s': %s\n",
                          current_table.c_str(),
                          vst.message.c_str());
        }
        shell_invalidate_session_table(st);
        return true;
    }
    if (strcasecmp_ascii(line, "CONFIRM_REORDER") == 0) {
        if (current_file.empty()) {
            log_and_print(log_file, "[REORDER] no table selected. Use CREATE TABLE or USE first.\n");
            return true;
        }
        log_and_print(log_file,
                      "[REORDER] confirmed: deleted rows will not be recovered; reassigning row ids to 1..N "
                      "(primary key must be id).\n");
        std::size_t rows_after = 0;
        bool file_changed = false;
        const newdb::Status rst =
            newdb::io::reorder_heap_ids_dense(eff_data.c_str(), current_table, st.session.schema, &rows_after,
                                             &file_changed);
        if (rst.ok) {
            if (file_changed) {
                (void)newdb::save_schema_text(newdb::schema_sidecar_path_for_data_file(eff_data), st.session.schema);
                log_and_print(log_file, "[REORDER] ok: table '%s' (file=%s), rows=%zu, ids dense 1..%zu.\n",
                              current_table.c_str(), current_file.c_str(), rows_after, rows_after);
            } else {
                log_and_print(log_file,
                              "[REORDER] noop: table '%s' (file=%s), rows=%zu, ids already dense 1..%zu (no rewrite).\n",
                              current_table.c_str(), current_file.c_str(), rows_after, rows_after);
            }
        } else {
            log_and_print(log_file, "[REORDER] failed: %s\n", rst.message.c_str());
        }
        shell_invalidate_session_table(st);
        return true;
    }
    if (strcasecmp_ascii(line, "SCAN") == 0) {
        if (current_file.empty()) {
            log_and_print(log_file, "[SCAN] no table selected. Use CREATE TABLE or USE first.\n");
            return true;
        }
        newdb::io::scan_heap_file(eff_data.c_str());
        return true;
    }
    return false;
}




