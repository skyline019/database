#include <waterfall/config.h>

#include <fstream>
#include <iomanip>
#include <sstream>
#include <string>

#include <newdb/heap_table.h>
#include <newdb/row.h>
#include <newdb/schema.h>

#include "csv_export.h"
#include "demo_export.h"
#include "logging.h"

namespace {

std::string json_escape(const std::string& s) {
    std::ostringstream oss;
    for (char c : s) {
        switch (c) {
        case '"':
            oss << "\\\"";
            break;
        case '\\':
            oss << "\\\\";
            break;
        case '\b':
            oss << "\\b";
            break;
        case '\f':
            oss << "\\f";
            break;
        case '\n':
            oss << "\\n";
            break;
        case '\r':
            oss << "\\r";
            break;
        case '\t':
            oss << "\\t";
            break;
        default:
            if ((unsigned char)c < 0x20) {
                oss << "\\u" << std::hex << std::setw(4) << std::setfill('0')
                    << (int)(unsigned char)c;
            } else {
                oss << c;
            }
        }
    }
    return oss.str();
}

} // namespace

bool export_table_csv(const newdb::TableSchema& schema,
                      const newdb::HeapTable& tbl,
                      const std::string& filename,
                      const char* log_file) {
    std::ofstream ofs(filename);
    if (!ofs) {
        log_and_print(log_file, "[EXPORT] failed to open %s for writing\n", filename.c_str());
        return false;
    }
    ofs << csv_escape_cell("id");
    for (const auto& m : schema.attrs) {
        ofs << "," << csv_escape_cell(m.name);
    }
    ofs << "\n";
    newdb::Row r;
    for (std::size_t i = 0; i < tbl.logical_row_count(); ++i) {
        if (tbl.is_heap_storage_backed()) {
            if (!tbl.decode_heap_slot(i, r)) {
                continue;
            }
        } else {
            r = tbl.rows[i];
        }
        ofs << csv_escape_cell(std::to_string(r.id));
        for (const auto& m : schema.attrs) {
            ofs << ",";
            auto it = r.attrs.find(m.name);
            const std::string cell = (it != r.attrs.end()) ? it->second : std::string{};
            ofs << csv_escape_cell(cell);
        }
        ofs << "\n";
    }
    log_and_print(log_file, "[EXPORT] csv saved to %s\n", filename.c_str());
    return true;
}

bool export_table_json(const newdb::TableSchema& schema,
                       const newdb::HeapTable& tbl,
                       const std::string& filename,
                       const char* log_file) {
    std::ofstream ofs(filename);
    if (!ofs) {
        log_and_print(log_file, "[EXPORT] failed to open %s for writing\n", filename.c_str());
        return false;
    }
    ofs << "[\n";
    bool first_row = true;
    newdb::Row r;
    for (std::size_t i = 0; i < tbl.logical_row_count(); ++i) {
        if (tbl.is_heap_storage_backed()) {
            if (!tbl.decode_heap_slot(i, r)) {
                continue;
            }
        } else {
            r = tbl.rows[i];
        }
        if (!first_row) {
            ofs << ",\n";
        }
        first_row = false;
        ofs << "  {\n";
        ofs << "    \"id\": " << r.id;
        for (const auto& m : schema.attrs) {
            ofs << ",\n    \"" << m.name << "\": \""
                << json_escape(r.attrs.count(m.name) ? r.attrs.at(m.name) : std::string(""))
                << "\"";
        }
        ofs << "\n  }";
    }
    ofs << "\n]\n";
    log_and_print(log_file, "[EXPORT] json saved to %s\n", filename.c_str());
    return true;
}
