#pragma once

#include <cstdint>
#include <set>
#include <string>

enum class IndexKind {
    Eq,
    Range,
    Covering,
    PageOrder,
    Visibility,
};

struct IndexDescriptor {
    std::string table_name;
    std::string index_name;
    IndexKind kind{IndexKind::Eq};
    std::uint64_t data_lsn{0};
    std::uint64_t schema_version{0};
    std::uint64_t built_at_ms{0};
    bool valid{true};
};

/// Returns false when descriptor is stale vs current table generation (phase 6 hook).
bool index_descriptor_matches_runtime(const IndexDescriptor& d,
                                      std::uint64_t table_schema_version,
                                      std::uint64_t table_data_lsn);

/// Human-readable reason when `index_descriptor_matches_runtime` is false (empty when valid).
std::string explain_index_descriptor_mismatch(const IndexDescriptor& d,
                                                std::uint64_t table_schema_version,
                                                std::uint64_t table_data_lsn);

/// Count of `sidecar_invalidate_all_indexes_for_data_file` calls with non-empty data path.
std::uint64_t index_catalog_sidecar_invalidate_request_count();

/// UTC wall time ms for `IndexDescriptor::built_at_ms` when writing sidecar headers.
std::uint64_t index_catalog_built_at_wall_ms();

/// FNV-1a 64-bit over UTF-8 bytes (stable identity for table path / index key in sidecar headers).
std::uint64_t index_catalog_fnv1a64(const std::string& s);

struct IndexCatalogParsedTail {
    std::uint64_t idx_sv{0};
    std::uint64_t idx_dl{0};
    std::uint64_t tbl_fnv{0};
    std::uint64_t inx_fnv{0};
    std::uint64_t built_ms{0};
    /// Catalog build lifecycle tail (`;bld=`): 0 absent/legacy, 1 building, 2 ready (writers emit ready on finalize).
    std::uint8_t catalog_build_state{0};
};

/// Optional percent-encoded plaintext table / index names (`;tbl_n=`, `;inx_n=`).
struct IndexCatalogPlaintextNames {
    std::string table_name;
    std::string index_name;
    bool has_plaintext{false};
    bool malformed_plaintext{false};
};

/// Strip trailing `.bin` for catalog display name; empty if suffix missing.
std::string index_catalog_infer_table_plain_from_data_file(const std::string& data_file);

std::string index_catalog_pct_encode(const std::string& s);
std::string index_catalog_pct_decode(const std::string& s);

/// Parses optional `;idx_sv=`, `;idx_dl=`, `;tbl_fnv=`, `;inx_fnv=`, `;built_ms=` digit runs from a header line.
void index_catalog_parse_header_tail(const std::string& hdr, IndexCatalogParsedTail& out);

/// Parses `;tbl_n=` / `;inx_n=` percent-encoded segments. Both must be present together or neither.
void index_catalog_parse_plaintext_names(const std::string& hdr, IndexCatalogPlaintextNames& out);

/// True when plaintext is absent, or both decoded names equal expected. False if malformed or mismatch.
bool index_catalog_plaintext_matches(const IndexCatalogPlaintextNames& parsed,
                                     const std::string& expected_table_plain,
                                     const std::string& expected_index_plain);

/// FNV tail + optional plaintext names + optional `idx_kind` (when present must match `expected_kind`).
bool index_catalog_header_identity_ok(const std::string& hdr,
                                      const IndexCatalogParsedTail& tail,
                                      const std::string& table_identity_path,
                                      const std::string& index_identity_key,
                                      const std::string& expected_table_plain,
                                      const std::string& expected_index_plain,
                                      IndexKind expected_kind);

/// When `tail` carries non-zero FNV fields, they must match `index_catalog_fnv1a64` of the given paths.
bool index_catalog_tail_identity_matches(const IndexCatalogParsedTail& tail,
                                         const std::string& table_identity_path,
                                         const std::string& index_identity_key);

/// Appended after `wal_lsn` (before `;ver=` / `;buckets=` / body). Writes explicit catalog snapshot:
/// `idx_kind`, `built_ms`, `idx_sv` / `idx_dl` (align with `IndexDescriptor::schema_version` / `data_lsn`),
/// `tbl_fnv` / `inx_fnv` (hashed `table_identity_path` / `index_identity_key`).
/// When `table_plain_name` and `index_plain_name` are both non-empty, appends `;tbl_n=` / `;inx_n=`
/// (percent-encoded) for human-readable catalog alignment.
std::string index_catalog_sidecar_meta_suffix(IndexKind kind,
                                              std::uint64_t idx_schema_sig,
                                              std::uint64_t idx_data_lsn,
                                              const std::string& table_identity_path,
                                              const std::string& index_identity_key,
                                              IndexCatalogParsedTail* out_written = nullptr,
                                              const std::string& table_plain_name = std::string(),
                                              const std::string& index_plain_name = std::string());

/// Single entry point for sidecar invalidation after writes (phase 6 centralization).
void sidecar_invalidate_all_indexes_for_data_file(const std::string& eff_data);
void sidecar_invalidate_all_indexes_for_data_file(const std::string& eff_data,
                                                 const std::set<std::string>& attrs);
