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

inline std::string row_key(std::string_view table, std::string_view pk) {
  return std::string(kRow) + std::string(table) + '$' + std::string(pk);
}
inline std::string schema_key(std::string_view table) { return std::string(kSchema) + std::string(table); }
inline std::string row_index_key(std::string_view table) { return std::string(kRowIndex) + std::string(table); }
inline std::string catalog_key(std::string_view table) { return std::string(kCatalog) + std::string(table); }

}  // namespace structdb::storage::mdb_keyspace
