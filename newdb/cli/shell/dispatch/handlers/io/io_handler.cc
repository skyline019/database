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

#include <newdb/schema_io.h>

#include "cli/modules/sidecar/covering/covering_index_sidecar.h"
#include "cli/shell/dispatch/shared/dispatch_internal.h"
#include "cli/modules/import_export/demo_export.h"
#include "cli/modules/import_export/import.h"
#include "cli/modules/common/logging/logging.h"
#include "cli/modules/sidecar/page/page_index_sidecar.h"
#include "cli/modules/catalog/schema_catalog.h"
#include "cli/shell/state/shell_state_facade.h"
#include "cli/modules/common/view/table_view.h"
#include "cli/modules/txn/coordinator/txn_manager.h"
#include "cli/modules/common/util/utils.h"
#include "cli/modules/where/executor/where.h"

bool handle_export_command(ShellState& st,
                           const char* line,
                           const char* log_file,
                           const std::string& current_table,
                           const std::string& current_file,
                           newdb::HeapTable& tbl) {
    ShellStateFacade f(st);
    if (strncasecmp_ascii(line, "EXPORT", 6) != 0) {
        return false;
    }
    std::istringstream iss(line);
    std::string cmd;
    std::string fmt;
    iss >> cmd >> fmt;
    std::string rest;
    std::getline(iss, rest);
    while (!rest.empty() && std::isspace((unsigned char)rest.front())) rest.erase(rest.begin());
    std::string out_file = rest;
    if (out_file.empty() && !current_file.empty()) {
        if (fmt == "CSV") out_file = current_table + ".csv";
        else if (fmt == "JSON") out_file = current_table + ".json";
    }
    if (out_file.empty()) {
        log_and_print(log_file, "[EXPORT] error: no output file specified\n");
        return true;
    }
    {
        std::error_code ec;
        std::filesystem::path p(out_file);
        if (!p.is_absolute()) {
            std::filesystem::path base;
            if (!f.data_dir().empty()) {
                base = std::filesystem::absolute(f.data_dir(), ec);
            }
            if (base.empty()) {
                base = f.workspace_directory();
            }
            p = base / p;
            out_file = p.lexically_normal().string();
        }
    }
    if (fmt == "CSV") {
        export_table_csv(f.schema(), tbl, out_file, log_file);
    } else if (fmt == "JSON") {
        export_table_json(f.schema(), tbl, out_file, log_file);
    } else {
        log_and_print(log_file, "[EXPORT] unknown format '%s'\n", fmt.c_str());
    }
    return true;
}


bool handle_import_defattr_commands(ShellState& st,
                                    const char* line,
                                    const char* log_file,
                                    const std::string& eff_data,
                                    const std::string& current_file) {
    ShellStateFacade f(st);
    if (strncasecmp_ascii(line, "IMPORTDIR", 9) == 0) {
        std::vector<std::string> args;
        if (!parse_comma_args(line + 9, args) || args.size() != 1) {
            log_and_print(log_file, "[IMPORTDIR] usage: IMPORTDIR(path)\n");
            return true;
        }
        if (import_tables_from_directory(args[0].c_str(),
                                         f.data_dir().empty() ? nullptr : f.data_dir().c_str(),
                                         log_file)) {
            log_and_print(log_file, "[IMPORTDIR] done.\n");
        } else {
            log_and_print(log_file, "[IMPORTDIR] failed.\n");
        }
        return true;
    }
    if (strncasecmp_ascii(line, "DEFATTR", 7) == 0) {
        std::vector<newdb::AttrMeta> attrs;
        std::set<std::string> seen_names;
        std::vector<std::string> args;
        if (!parse_comma_args(line + 7, args) || args.empty()) {
            log_and_print(log_file, "[DEFATTR] usage: DEFATTR(name:type, name:type, ...) (excluding 'id')\n");
            return true;
        }
        for (const auto& a : args) {
            auto pos = a.find(':');
            if (pos == std::string::npos || pos == 0 || pos + 1 >= a.size()) {
                log_and_print(log_file, "[DEFATTR] each item must be name:type, e.g. age:int, nickname:string\n");
                attrs.clear();
                break;
            }
            std::string name = a.substr(0, pos);
            std::string type_str = a.substr(pos + 1);
            if (name == "id") continue;
            newdb::AttrType tp = newdb::TableSchema::parse_type(type_str);
            if (tp == newdb::AttrType::Unknown || seen_names.find(name) != seen_names.end()) {
                attrs.clear();
                break;
            }
            seen_names.insert(name);
            attrs.push_back(newdb::AttrMeta{name, tp});
        }
        if (attrs.empty()) {
            log_and_print(log_file, "[DEFATTR] usage: DEFATTR <name:type> [name:type] ... (excluding 'id')\n");
            return true;
        }
        f.schema().attrs = std::move(attrs);
        if (!f.schema().valid_primary_key()) f.schema().primary_key = "id";
        log_and_print(log_file, "[DEFATTR] attributes set to:");
        for (const auto& m : f.schema().attrs) {
            log_and_print(log_file, " %s(%s)", m.name.c_str(), newdb::TableSchema::type_name(m.type));
        }
        log_and_print(log_file, "\n");
        if (!current_file.empty()) {
            newdb::save_schema_text(newdb::schema_sidecar_path_for_data_file(eff_data), f.schema());
        }
        return true;
    }
    return false;
}




