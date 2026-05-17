#pragma once

#include <string>
#include <string_view>

namespace structdb::storage::mdb_keyspace {

/// LSM key prefixes for newdb-compatible logical tables (StructDB-native encoding; not newdb `.bin` files).
inline constexpr std::string_view kV2 = "mdb$v2$";
inline constexpr std::string_view kRow = "mdb$v2$row$";       // + table + '$' + pk
inline constexpr std::string_view kSchema = "mdb$v2$schema$";  // + table
inline constexpr std::string_view kRowIndex = "mdb$v2$idxrows$";  // + table → newline-separated primary keys
inline constexpr std::string_view kCatalog = "mdb$v2$cat$";   // + table → marker "1" when table exists
inline constexpr std::string_view kSecIdx = "mdb$v2$idx$";    // reserved: + table + '$' + col + '$' + tokenized value
/// Named index definition: + table + '$' + index_name → column name (plain text).
inline constexpr std::string_view kNamedIdxDef = "mdb$v2$nidxdef$";
/// Named index posting: + table + '$' + index_name + '$' + col + '$' + token + '$' + pk.
inline constexpr std::string_view kNamedIdx = "mdb$v2$nidx$";

inline std::string row_key(std::string_view table, std::string_view pk) {
  return std::string(kRow) + std::string(table) + '$' + std::string(pk);
}
inline std::string schema_key(std::string_view table) { return std::string(kSchema) + std::string(table); }
inline std::string row_index_key(std::string_view table) { return std::string(kRowIndex) + std::string(table); }
inline std::string catalog_key(std::string_view table) { return std::string(kCatalog) + std::string(table); }
inline std::string named_index_def_key(std::string_view table, std::string_view index_name) {
  return std::string(kNamedIdxDef) + std::string(table) + '$' + std::string(index_name);
}
inline std::string named_index_posting_key(std::string_view table, std::string_view index_name,
                                           std::string_view col, std::string_view token, std::string_view pk) {
  return std::string(kNamedIdx) + std::string(table) + '$' + std::string(index_name) + '$' + std::string(col) +
         '$' + std::string(token) + '$' + std::string(pk);
}

}  // namespace structdb::storage::mdb_keyspace
