#include <newdb/schema_io.h>

#include <cstdint>
#include <cstring>
#include <fstream>
#include <sstream>

namespace newdb {

std::string schema_sidecar_path_for_data_file(const std::string& data_file) {
    std::string p = data_file;
    const auto pos = p.rfind(".bin");
    if (pos != std::string::npos && pos + 4 == p.size()) {
        p.replace(pos, 4, ".attr");
    } else {
        p += ".attr";
    }
    return p;
}

static std::string trim(std::string s) {
    while (!s.empty() && (s.back() == '\r' || s.back() == ' ' || s.back() == '\t')) {
        s.pop_back();
    }
    std::size_t i = 0;
    while (i < s.size() && (s[i] == ' ' || s[i] == '\t')) ++i;
    return s.substr(i);
}

Status save_schema_text(const std::string& path, const TableSchema& schema) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) {
        return Status::Fail("cannot open schema file for write: " + path);
    }
    out << "PRIMARY_KEY:" << (schema.primary_key.empty() ? "id" : schema.primary_key) << '\n';
    {
        const std::uint32_t hf = schema.heap_format_version == 0 ? 1u : schema.heap_format_version;
        out << "HEAP_FORMAT:" << hf << '\n';
    }
    if (!schema.table_label.empty()) {
        out << "SCHEMA:" << schema.table_label << '\n';
    }
    for (const auto& m : schema.attrs) {
        if (m.type == AttrType::Unknown) continue;
        out << m.name << ':' << TableSchema::type_name(m.type) << '\n';
    }
    out.flush();
    if (!out) {
        return Status::Fail("failed writing schema file: " + path);
    }
    return Status::Ok();
}

Status load_schema_text(const std::string& path, TableSchema& out_schema) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        out_schema = TableSchema{};
        return Status::Ok(); // missing sidecar is valid: caller uses defaults
    }
    std::stringstream ss;
    ss << in.rdbuf();
    const std::string content = ss.str();

    TableSchema loaded;
    std::string loaded_pk;
    std::string loaded_label;
    std::uint32_t loaded_heap_format = 0;
    std::vector<AttrMeta> attrs;

    std::size_t line_start = 0;
    for (std::size_t i = 0; i <= content.size(); ++i) {
        if (i == content.size() || content[i] == '\n') {
            std::string line = trim(content.substr(line_start, i - line_start));
            line_start = i + 1;
            if (line.empty()) continue;
            constexpr const char* kPk = "PRIMARY_KEY:";
            constexpr const char* kSc = "SCHEMA:";
            constexpr const char* kHf = "HEAP_FORMAT:";
            if (line.rfind(kPk, 0) == 0 && line.size() > std::strlen(kPk)) {
                loaded_pk = trim(line.substr(std::strlen(kPk)));
                continue;
            }
            if (line.rfind(kHf, 0) == 0 && line.size() > std::strlen(kHf)) {
                try {
                    loaded_heap_format =
                        static_cast<std::uint32_t>(std::stoul(trim(line.substr(std::strlen(kHf)))));
                } catch (...) {
                    loaded_heap_format = 0;
                }
                continue;
            }
            if (line.rfind(kSc, 0) == 0 && line.size() > std::strlen(kSc)) {
                loaded_label = trim(line.substr(std::strlen(kSc)));
                continue;
            }
            const auto colon = line.find(':');
            if (colon == std::string::npos || colon == 0 || colon + 1 >= line.size()) {
                continue;
            }
            const std::string name = trim(line.substr(0, colon));
            const std::string type_str = trim(line.substr(colon + 1));
            const AttrType tp = TableSchema::parse_type(type_str);
            if (tp == AttrType::Unknown) continue;
            attrs.push_back(AttrMeta{name, tp});
        }
    }

    loaded.attrs = std::move(attrs);
    loaded.primary_key = loaded_pk.empty() ? "id" : loaded_pk;
    loaded.table_label = std::move(loaded_label);
    loaded.heap_format_version = loaded_heap_format;
    if (!loaded.valid_primary_key()) {
        loaded.primary_key = "id";
    }
    out_schema = std::move(loaded);
    return Status::Ok();
}

} // namespace newdb
