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
#include <newdb/schema.h>
#include <newdb/schema_io.h>

#include "cli/modules/sidecar/covering/covering_index_sidecar.h"
#include "cli/shell/dispatch/shared/dispatch_internal.h"
#include "cli/modules/import_export/demo_export.h"
#include "cli/modules/import_export/import.h"
#include "cli/modules/common/logging/logging.h"
#include "cli/modules/sidecar/page/page_index_sidecar.h"
#include "cli/modules/catalog/schema_catalog.h"
#include "cli/shell/state/shell_state_ops.h"
#include "cli/shell/state/shell_state_facade.h"
#include "cli/modules/common/view/table_view.h"
#include "cli/modules/txn/coordinator/txn_manager.h"
#include "cli/modules/common/util/utils.h"
#include "cli/modules/where/executor/where.h"
#include "cli/shell/dispatch/services/lsm/lsm_lite_service.h"

bool handle_ddl_create_use_commands(ShellState& st,
                                    const char* line,
                                    const char* log_file,
                                    const std::string& eff_data,
                                    std::string& current_table,
                                    std::string& current_file) {
    ShellStateFacade f(st);
    // CREATE TABLE(name)
    if (strncasecmp_ascii(line, "CREATE TABLE", 12) == 0) {
        std::vector<std::string> args;
        if (!parse_comma_args(line + 12, args) || args.size() != 1) {
            log_and_print(log_file,
                          "[CREATE] usage: CREATE TABLE(name)\n");
            return true;
        }
        std::string new_table = args[0];
        std::string new_file  = new_table + ".bin";
        if (!f.schema().valid_primary_key()) {
            f.schema().primary_key = "id";
        }
        std::vector<newdb::Row> empty_rows;
        const std::string abs_new = f.resolve_table_file(new_file);
        if (newdb::io::create_heap_file(abs_new.c_str(), empty_rows)) {
            newdb::save_schema_text(newdb::schema_sidecar_path_for_data_file(abs_new), f.schema());
            current_table = new_table;
            current_file  = new_file;
            reload_schema_from_data_path(st, new_file);
            shell_invalidate_session_table(st);
            log_and_print(log_file,
                          "[CREATE] table '%s' created, file=%s\n",
                          current_table.c_str(), f.data_path().c_str());
        } else {
            log_and_print(log_file,
                          "[CREATE] failed to create table '%s'\n",
                          new_table.c_str());
        }
        return true;
    }

    // USE(name)
    if (strncasecmp_ascii(line, "USE", 3) == 0) {
        std::vector<std::string> args;
        if (!parse_comma_args(line + 3, args) || args.size() != 1) {
            log_and_print(log_file, "[USE] usage: USE(name)\n");
            return true;
        }
        std::string new_table = args[0];
        std::string new_file  = new_table + ".bin";
        if (!current_table.empty() && !current_file.empty() &&
            new_table == current_table && new_file == current_file) {
            log_and_print(log_file,
                          "[USE] already using table '%s' (file=%s)\n",
                          current_table.c_str(), eff_data.c_str());
            return true;
        }
        const std::string try_path = f.resolve_table_file(new_file);
        FILE* tf = std::fopen(try_path.c_str(), "rb");
        if (!tf) {
            log_and_print(log_file,
                          "[USE] table '%s' not found (file=%s)\n",
                          new_table.c_str(), try_path.c_str());
        } else {
            std::fclose(tf);
            const std::string prev_data = eff_data;
            current_table = new_table;
            current_file  = new_file;
            f.schema() = newdb::TableSchema{};
            reload_schema_from_data_path(st, current_file);
            shell_invalidate_session_table(st);
            if (!prev_data.empty()) {
                lsm_lite_clear_txn_views(st, prev_data);
            }
            log_and_print(log_file,
                          "[USE] now using table '%s' (file=%s)\n",
                          current_table.c_str(), f.effective_data_path().c_str());
        }
        return true;
    }
    return false;
}


bool handle_schema_key_command(ShellState& st,
                               const char* line,
                               const char* log_file,
                               const std::string& eff_data,
                               newdb::HeapTable& tbl) {
    ShellStateFacade f(st);
    if (strncasecmp_ascii(line, "SET PRIMARY KEY", 15) != 0) {
        return false;
    }
    std::vector<std::string> args;
    if (!parse_comma_args(line + 15, args) || args.size() != 1) {
        log_and_print(log_file, "[KEY] usage: SET PRIMARY KEY(key)\n");
        return true;
    }
    std::string key = args[0];
    if (key.empty()) {
        log_and_print(log_file, "[KEY] key cannot be empty.\n");
        return true;
    }
    if (key == "id") {
        f.schema().primary_key = "id";
        newdb::save_schema_text(newdb::schema_sidecar_path_for_data_file(eff_data), f.schema());
        log_and_print(log_file, "[KEY] primary key set to id\n");
        return true;
    }
    if (newdb::find_attr_meta(f.schema(), key) == nullptr) {
        log_and_print(log_file,
                      "[KEY] unknown attribute '%s'. Use DEFATTR first.\n",
                      key.c_str());
        return true;
    }
    std::set<std::string> seen;
    newdb::Row r;
    for (std::size_t ri = 0; ri < tbl.logical_row_count(); ++ri) {
        if (tbl.is_heap_storage_backed()) {
            if (!tbl.decode_heap_slot(ri, r)) {
                log_and_print(log_file, "[KEY] failed to read row at slot %zu\n", ri);
                return true;
            }
        } else {
            r = tbl.rows[ri];
        }
        auto it = r.attrs.find(key);
        if (it == r.attrs.end()) {
            log_and_print(log_file,
                          "[KEY] cannot set primary key to '%s': row(id=%d) missing this attribute\n",
                          key.c_str(), r.id);
            return true;
        }
        const std::string& v = it->second;
        if (seen.find(v) != seen.end()) {
            log_and_print(log_file,
                          "[KEY] cannot set primary key to '%s': duplicate value '%s'\n",
                          key.c_str(), v.c_str());
            return true;
        }
        seen.insert(v);
    }
    f.schema().primary_key = key;
    newdb::save_schema_text(newdb::schema_sidecar_path_for_data_file(eff_data), f.schema());
    log_and_print(log_file, "[KEY] primary key set to %s\n", f.schema().primary_key.c_str());
    return true;
}


bool handle_schema_catalog_commands(ShellState& st, const char* line, const char* log_file) {
    ShellStateFacade f(st);
    if (strncasecmp_ascii(line, "CREATE SCHEMA", 13) == 0) {
        std::vector<std::string> args;
        if (!parse_comma_args(line + 13, args) || args.size() != 1) {
            log_and_print(log_file, "[CREATE SCHEMA] usage: CREATE SCHEMA(schema_name)\n");
            return true;
        }
        const std::string schema_name = args[0];
        if (schema_name.empty()) {
            log_and_print(log_file, "[CREATE SCHEMA] schema name cannot be empty\n");
            return true;
        }
        bool ok = create_schema(f.data_dir(), schema_name);
        if (ok) log_and_print(log_file, "[CREATE SCHEMA] schema '%s' created\n", schema_name.c_str());
        else log_and_print(log_file, "[CREATE SCHEMA] failed to create schema '%s' (may already exist)\n", schema_name.c_str());
        return true;
    }
    if (strncasecmp_ascii(line, "DROP SCHEMA", 11) == 0) {
        std::vector<std::string> args;
        if (!parse_comma_args(line + 11, args) || args.size() != 1) {
            log_and_print(log_file, "[DROP SCHEMA] usage: DROP SCHEMA(schema_name)\n");
            return true;
        }
        const std::string schema_name = args[0];
        if (schema_name.empty()) {
            log_and_print(log_file, "[DROP SCHEMA] schema name cannot be empty\n");
            return true;
        }
        std::vector<std::string> tables_in_schema;
        get_tables_in_schema(f.data_dir(), schema_name, tables_in_schema);
        if (!tables_in_schema.empty()) {
            log_and_print(log_file, "[DROP SCHEMA] cannot drop schema '%s' - it contains %zu tables\n",
                          schema_name.c_str(), tables_in_schema.size());
            return true;
        }
        bool ok = delete_schema(f.data_dir(), schema_name);
        if (ok) log_and_print(log_file, "[DROP SCHEMA] schema '%s' deleted\n", schema_name.c_str());
        else log_and_print(log_file, "[DROP SCHEMA] failed to delete schema '%s'\n", schema_name.c_str());
        return true;
    }
    if (strcasecmp_ascii(line, "LIST SCHEMAS") == 0 || strcasecmp_ascii(line, "SHOW SCHEMAS") == 0) {
        std::vector<std::string> schemas;
        list_schemas(f.data_dir(), schemas);
        if (schemas.empty()) {
            log_and_print(log_file, "[LIST SCHEMAS] no schemas found\n");
            return true;
        }
        log_and_print(log_file, "[LIST SCHEMAS] schemas (%zu):\n", schemas.size());
        for (const auto& s : schemas) {
            std::vector<std::string> tables;
            get_tables_in_schema(f.data_dir(), s, tables);
            log_and_print(log_file, "  %s  (%zu tables)\n", s.c_str(), tables.size());
        }
        return true;
    }
    return false;
}


bool handle_ddl_alter_rename_commands(ShellState& st,
                                      const char* line,
                                      const char* log_file,
                                      const std::string& eff_data,
                                      std::string& current_table,
                                      std::string& current_file) {
    ShellStateFacade f(st);
    if (strncasecmp_ascii(line, "ALTER TABLE", 11) == 0) {
        std::string full = trim(line);
        std::istringstream iss(full);
        std::string kw1, kw2, tableName, op1, op2;
        iss >> kw1 >> kw2 >> tableName >> op1;
        if (tableName.empty() || op1.empty()) {
            log_and_print(log_file, "[ALTER] usage: ALTER TABLE name SET SCHEMA(schema_name)\n");
            return true;
        }
        if (op1 == "SET") {
            std::string rest;
            std::getline(iss, rest);
            rest = trim(rest);
            // support both "SCHEMA(x)" and "SCHEMA (x)"
            if (rest.rfind("SCHEMA", 0) == 0) {
                std::string schemaPart = trim(rest.substr(6));
                if (!schemaPart.empty() && schemaPart.front() == '(' && schemaPart.back() == ')') {
                    schemaPart = schemaPart.substr(1, schemaPart.size() - 2);
                    const size_t commaPos = schemaPart.find(',');
                    std::string schemaName = (commaPos != std::string::npos) ? schemaPart.substr(commaPos + 1) : schemaPart;
                    schemaName = trim(schemaName);
                    if (schemaName.empty()) {
                        log_and_print(log_file, "[ALTER] usage: ALTER TABLE name SET SCHEMA(schema_name)\n");
                        return true;
                    }
                    reload_schema_from_data_path(st, tableName + ".bin");
                    f.schema().table_label = schemaName;
                    newdb::save_schema_text(newdb::schema_sidecar_path_for_data_file(f.data_path()), f.schema());
                    log_and_print(log_file, "[ALTER] table '%s' schema set to '%s'\n", tableName.c_str(), schemaName.c_str());
                    shell_invalidate_session_table(st);
                    return true;
                }
            }
            log_and_print(log_file, "[ALTER] usage: ALTER TABLE name SET SCHEMA(schema_name)\n");
            return true;
        }
        if (op1 == "REMOVE") {
            iss >> op2;
            if (op2 == "SCHEMA") {
                reload_schema_from_data_path(st, tableName + ".bin");
                f.schema().table_label = "";
                newdb::save_schema_text(newdb::schema_sidecar_path_for_data_file(f.data_path()), f.schema());
                log_and_print(log_file, "[ALTER] table '%s' schema removed\n", tableName.c_str());
                shell_invalidate_session_table(st);
                return true;
            }
            log_and_print(log_file, "[ALTER] usage: ALTER TABLE name REMOVE SCHEMA\n");
            return true;
        }
        log_and_print(log_file, "[ALTER] unknown subcommand. Use: ALTER TABLE name SET SCHEMA(schema_name) or ALTER TABLE name REMOVE SCHEMA\n");
        return true;
    }

    if (strncasecmp_ascii(line, "RENAME TABLE", 12) == 0) {
        std::vector<std::string> args;
        if (!parse_comma_args(line + 12, args) || args.size() != 1) {
            log_and_print(log_file, "[RENAME] usage: RENAME TABLE(new_name)\n");
            return true;
        }
        const std::string new_table = args[0];
        const std::string new_file = new_table + ".bin";
        const std::string old_abs = eff_data;
        const std::string new_abs = f.resolve_table_file(new_file);
        if (new_abs == old_abs) {
            log_and_print(log_file, "[RENAME] new name is same as current, ignored.\n");
            return true;
        }
        const std::string old_attr = newdb::schema_sidecar_path_for_data_file(old_abs);
        const std::string new_attr = newdb::schema_sidecar_path_for_data_file(new_abs);
        if (std::rename(old_abs.c_str(), new_abs.c_str()) != 0) {
            log_and_print(log_file, "[RENAME] failed to rename file '%s' to '%s'\n", old_abs.c_str(), new_abs.c_str());
            return true;
        }
        if (old_attr != new_attr) {
            (void)std::rename(old_attr.c_str(), new_attr.c_str());
        }
        lsm_lite_clear_txn_views(st, old_abs);
        current_table = new_table;
        current_file = new_file;
        reload_schema_from_data_path(st, new_file);
        shell_invalidate_session_table(st);
        log_and_print(log_file, "[RENAME] table now '%s' (file=%s)\n", current_table.c_str(), f.data_path().c_str());
        return true;
    }
    return false;
}


bool handle_schema_show_commands(ShellState& st,
                                 const char* line,
                                 const char* log_file,
                                 const std::string& current_table,
                                 const std::string& current_file) {
    ShellStateFacade f(st);
    if (strcasecmp_ascii(line, "SHOW ATTR") == 0 || strcasecmp_ascii(line, "DESCRIBE") == 0) {
        if (current_file.empty()) {
            log_and_print(log_file, "[ATTR] no table selected. Use CREATE TABLE or USE first.\n");
            return true;
        }
        if (f.schema().attrs.empty()) {
            reload_schema_from_data_path(st, current_file);
        }
        log_and_print(log_file, "[ATTR] table='%s' file=%s\n", current_table.c_str(), current_file.c_str());
        log_and_print(log_file, "  id:int%s\n", (f.schema().primary_key == "id" ? "  [PK]" : ""));
        if (f.schema().attrs.empty()) {
            log_and_print(log_file, "  (no DEFATTR)\n");
            return true;
        }
        for (const auto& m : f.schema().attrs) {
            log_and_print(log_file, "  %s:%s%s\n", m.name.c_str(),
                          newdb::TableSchema::type_name(m.type),
                          (m.name == f.schema().primary_key ? "  [PK]" : ""));
        }
        return true;
    }
    if (strcasecmp_ascii(line, "SHOW KEY") == 0 || strcasecmp_ascii(line, "SHOW PRIMARY KEY") == 0) {
        if (current_file.empty()) {
            log_and_print(log_file, "[KEY] no table selected. Use CREATE TABLE or USE first.\n");
            return true;
        }
        log_and_print(log_file, "[KEY] table='%s' primary_key=%s\n", current_table.c_str(),
                      (f.schema().primary_key.empty() ? "id" : f.schema().primary_key.c_str()));
        return true;
    }
    return false;
}




