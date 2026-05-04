#pragma once

#include <cstddef>
#include <set>
#include <string>
#include <vector>

#include <newdb/heap_table.h>
#include <newdb/schema.h>

struct WhereQueryContext;

struct EqIndexRequest {
    std::string data_file;
    std::string attr_name;
    /// Logical table name for catalog plaintext (`;tbl_n=`); empty => infer from `data_file` (.bin strip).
    std::string table_name;
};

struct EqLookupResult {
    bool used_index{false};
    std::vector<std::size_t> slots;
};

// Build or reuse persistent equality sidecar index and return matched slots for `value`.
EqLookupResult lookup_or_build_eq_index_sidecar(const EqIndexRequest& req,
                                                const newdb::TableSchema& schema,
                                                const newdb::HeapTable& table,
                                                const std::string& value,
                                                WhereQueryContext* where_obs = nullptr);

// Invalidate cached sidecar state and remove on-disk eqidx files for data file.
void invalidate_eq_index_sidecars_for_data_file(const std::string& data_file);

// Invalidate selected eqidx sidecars for changed attributes.
void invalidate_eq_index_sidecars_for_attrs(const std::string& data_file,
                                            const std::set<std::string>& attr_names);

/// Count of on-disk `.eqidx` / `.eqbloom` removals that returned a filesystem error (excluding missing files).
std::uint64_t eq_sidecar_invalidate_remove_fail_count();

/// Count of eq sidecar disk loads skipped because `memory_budget_used_bytes + file_size` exceeded
/// `NEWDB_MEMORY_BUDGET_MAX_BYTES` / `NEWDB_PAGE_CACHE_MAX_BYTES` (see `equality_index_sidecar.cc`).
std::uint64_t eq_sidecar_memory_budget_skip_count();
