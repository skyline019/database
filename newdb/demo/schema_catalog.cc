#include "schema_catalog.h"

#include <newdb/schema_io.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <string>

namespace fs = std::filesystem;

static fs::path schema_history_path(const std::string& workspace_root) {
    std::error_code ec;
    if (workspace_root.empty()) {
        return fs::current_path(ec) / ".schema_history";
    }
    return fs::absolute(workspace_root, ec) / ".schema_history";
}

bool create_schema(const std::string& workspace_root, const std::string& schema_name) {
    if (schema_name.empty()) {
        return false;
    }
    std::vector<std::string> schemas;
    list_schemas(workspace_root, schemas);
    for (const auto& s : schemas) {
        if (s == schema_name) {
            return false;
        }
    }
    const fs::path hist = schema_history_path(workspace_root);
    std::error_code ec;
    fs::create_directories(hist.parent_path(), ec);
    std::ofstream out(hist.string(), std::ios::app);
    if (!out) {
        return false;
    }
    out << schema_name << '\n';
    return true;
}

bool delete_schema(const std::string& workspace_root, const std::string& schema_name) {
    if (schema_name.empty()) {
        return false;
    }
    std::vector<std::string> tables;
    get_tables_in_schema(workspace_root, schema_name, tables);
    if (!tables.empty()) {
        return false;
    }

    std::vector<std::string> schemas;
    list_schemas(workspace_root, schemas);
    std::vector<std::string> kept;
    for (const auto& s : schemas) {
        if (s != schema_name) {
            kept.push_back(s);
        }
    }
    std::ofstream out(schema_history_path(workspace_root).string(), std::ios::trunc);
    if (!out) {
        return false;
    }
    for (const auto& s : kept) {
        out << s << '\n';
    }
    return true;
}

void list_schemas(const std::string& workspace_root, std::vector<std::string>& schemas) {
    schemas.clear();
    std::ifstream in(schema_history_path(workspace_root).string());
    if (!in) {
        return;
    }
    std::string line;
    while (std::getline(in, line)) {
        if (!line.empty()) {
            schemas.push_back(line);
        }
    }
    std::sort(schemas.begin(), schemas.end());
}

void get_tables_in_schema(const std::string& workspace_root,
                          const std::string& schema_name,
                          std::vector<std::string>& tables) {
    tables.clear();
    std::error_code ec;
    fs::path cwd;
    if (workspace_root.empty()) {
        cwd = fs::current_path(ec);
    } else {
        cwd = fs::absolute(workspace_root, ec);
    }
    if (ec) {
        return;
    }

    for (fs::directory_iterator it(cwd, ec), end; !ec && it != end; it.increment(ec)) {
        const fs::directory_entry& ent = *it;
        if (!ent.is_regular_file(ec)) {
            continue;
        }
        const fs::path p = ent.path();
        if (p.extension() != ".bin") {
            continue;
        }
        const std::string stem = p.stem().string();
        const std::string suf = "_log";
        if (stem.size() >= suf.size() &&
            stem.compare(stem.size() - suf.size(), suf.size(), suf) == 0) {
            continue;
        }
        newdb::TableSchema sch;
        (void)newdb::load_schema_text(newdb::schema_sidecar_path_for_data_file(p.string()), sch);
        if (sch.table_label == schema_name) {
            tables.push_back(stem);
        }
    }
    std::sort(tables.begin(), tables.end());
}
