#include <newdb/schema.h>

#include <cctype>
#include <string>

namespace newdb {

AttrType TableSchema::parse_type(std::string t) {
    for (auto& c : t) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    if (t == "int" || t == "integer") return AttrType::Int;
    if (t == "char") return AttrType::Char;
    if (t == "string" || t == "str") return AttrType::String;
    if (t == "timestamp") return AttrType::Timestamp;
    if (t == "date") return AttrType::Date;
    if (t == "datetime") return AttrType::DateTime;
    if (t == "float") return AttrType::Float;
    if (t == "double") return AttrType::Double;
    if (t == "bool" || t == "boolean") return AttrType::Bool;
    return AttrType::Unknown;
}

const char* TableSchema::type_name(AttrType t) {
    switch (t) {
    case AttrType::Int: return "int";
    case AttrType::Char: return "char";
    case AttrType::String: return "string";
    case AttrType::Timestamp: return "timestamp";
    case AttrType::Date: return "date";
    case AttrType::DateTime: return "datetime";
    case AttrType::Float: return "float";
    case AttrType::Double: return "double";
    case AttrType::Bool: return "bool";
    default: return "unknown";
    }
}

const AttrMeta* TableSchema::find_attr(const std::string& name) const {
    for (const auto& m : attrs) {
        if (m.name == name) return &m;
    }
    return nullptr;
}

AttrType TableSchema::type_of(const std::string& key) const {
    if (key == "id") return AttrType::Int;
    const AttrMeta* m = find_attr(key);
    return m ? m->type : AttrType::String;
}

bool TableSchema::valid_primary_key() const {
    if (primary_key.empty()) return false;
    if (primary_key == "id") return true;
    return find_attr(primary_key) != nullptr;
}

std::string TableSchema::default_int_predicate_attr() const {
    const AttrMeta* bal = find_attr("balance");
    if (bal != nullptr && bal->type == AttrType::Int) {
        return "balance";
    }
    for (const auto& m : attrs) {
        if (m.type == AttrType::Int && m.name != primary_key) {
            return m.name;
        }
    }
    return "balance";
}

namespace {

bool storage_value_parseable(AttrType t, const std::string& v) {
    switch (t) {
    case AttrType::Int:
        try {
            (void)std::stoll(v);
            return true;
        } catch (...) {
            return false;
        }
    case AttrType::Float:
    case AttrType::Double:
        try {
            (void)std::stod(v);
            return true;
        } catch (...) {
            return false;
        }
    case AttrType::Char:
        return v.size() == 1;
    case AttrType::Bool: {
        std::string s = v;
        for (auto& c : s) {
            c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        }
        return s == "1" || s == "0" || s == "true" || s == "false" || s == "y" || s == "n" || s == "yes" || s == "no";
    }
    case AttrType::String:
    case AttrType::Timestamp:
    case AttrType::Date:
    case AttrType::DateTime:
        return true;
    default:
        return true;
    }
}

}  // namespace

Status validate_storage_row(const TableSchema& schema, const Row& row) {
    for (const auto& kv : row.attrs) {
        if (kv.first == "__deleted") {
            continue;
        }
        const AttrMeta* m = schema.find_attr(kv.first);
        if (m == nullptr || m->type == AttrType::Unknown) {
            continue;
        }
        if (!storage_value_parseable(m->type, kv.second)) {
            return Status::Fail("storage row invalid for attr '" + kv.first + "': value not parseable as " +
                                std::string(TableSchema::type_name(m->type)));
        }
    }
    return Status::Ok();
}

int TableSchema::compare_attr(const std::string& key,
                              const std::string& va,
                              const std::string& vb) const {
    const AttrType t = type_of(key);
    switch (t) {
    case AttrType::Int:
        try {
            const long long a = std::stoll(va);
            const long long b = std::stoll(vb);
            if (a < b) return -1;
            if (a > b) return 1;
            return 0;
        } catch (...) {
            break;
        }
        break;
    case AttrType::Float:
    case AttrType::Double:
        try {
            const double a = std::stod(va);
            const double b = std::stod(vb);
            if (a < b) return -1;
            if (a > b) return 1;
            return 0;
        } catch (...) {
            break;
        }
        break;
    case AttrType::Bool: {
        auto norm_bool = [](const std::string& s, bool& ok) -> int {
            std::string v = s;
            for (auto& c : v) {
                c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
            }
            if (v == "1" || v == "true" || v == "y" || v == "yes") {
                ok = true;
                return 1;
            }
            if (v == "0" || v == "false" || v == "n" || v == "no") {
                ok = true;
                return 0;
            }
            ok = false;
            return 0;
        };
        bool ok_a = false;
        bool ok_b = false;
        const int ia = norm_bool(va, ok_a);
        const int ib = norm_bool(vb, ok_b);
        if (ok_a && ok_b) {
            if (ia < ib) return -1;
            if (ia > ib) return 1;
            return 0;
        }
        break;
    }
    default:
        break;
    }
    if (va < vb) return -1;
    if (va > vb) return 1;
    return 0;
}

} // namespace newdb
