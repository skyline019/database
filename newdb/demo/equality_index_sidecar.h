#pragma once

#include <cstddef>
#include <set>
#include <string>
#include <vector>

#include <newdb/heap_table.h>
#include <newdb/schema.h>

struct EqIndexRequest {
    std::string data_file;
    std::string attr_name;
};

struct EqLookupResult {
    bool used_index{false};
    std::vector<std::size_t> slots;
};

// Build or reuse persistent equality sidecar index and return matched slots for `value`.
EqLookupResult lookup_or_build_eq_index_sidecar(const EqIndexRequest& req,
                                                const newdb::TableSchema& schema,
                                                const newdb::HeapTable& table,
                                                const std::string& value);

// Invalidate cached sidecar state and remove on-disk eqidx files for data file.
void invalidate_eq_index_sidecars_for_data_file(const std::string& data_file);

// Invalidate selected eqidx sidecars for changed attributes.
void invalidate_eq_index_sidecars_for_attrs(const std::string& data_file,
                                            const std::set<std::string>& attr_names);
