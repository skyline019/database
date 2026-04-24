#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <newdb/error.h>
#include <newdb/row.h>

namespace newdb {

enum class AttrType {
    Int,
    Char,
    String,
    Timestamp,
    Date,
    DateTime,
    Float,
    Double,
    Bool,
    Unknown
};

struct AttrMeta {
    std::string name;
    AttrType type{AttrType::Unknown};
};

// Session-local schema: pass by const reference; no globals.
struct TableSchema {
    std::string table_label; // optional display / catalog name
    std::vector<AttrMeta> attrs;
    std::string primary_key{"id"};
    // From sidecar HEAP_FORMAT (0 = legacy file without line; >=1 = documented on-disk tuple rules).
    std::uint32_t heap_format_version{0};

    static AttrType parse_type(std::string t);
    static const char* type_name(AttrType t);

    const AttrMeta* find_attr(const std::string& name) const;
    AttrType type_of(const std::string& key) const;
    bool valid_primary_key() const;

    // First int column suitable for ad-hoc range scans (prefers "balance" when typed int).
    std::string default_int_predicate_attr() const;

    // Lexicographic / typed comparison for ORDER BY and predicates.
    int compare_attr(const std::string& key,
                     const std::string& va,
                     const std::string& vb) const;
};

// Typed attrs declared in schema must have values parseable as that type (extra attrs allowed).
Status validate_storage_row(const TableSchema& schema, const Row& row);

} // namespace newdb
