#pragma once

#include <set>
#include <string>

#include <newdb/heap_table.h>
#include <newdb/schema.h>

struct CoveringAggLookup {
    bool used{false};
    std::size_t count{0};
    long double sum{0.0L};
};

struct CoveringProjRow {
    int id{0};
    std::string value;
};

CoveringAggLookup lookup_or_build_covering_agg_sidecar(const std::string& data_file,
                                                       const std::string& key_attr,
                                                       const std::string& include_attr,
                                                       const std::string& key_value,
                                                       const newdb::TableSchema& schema,
                                                       const newdb::HeapTable& table);

// Projection sidecar for WHERE(eq) + single projected attribute (stringified).
// Stores first `limit` rows for each key. Optimizes printing without decoding heap rows.
std::vector<CoveringProjRow> lookup_or_build_covering_proj_sidecar(const std::string& data_file,
                                                                   const std::string& key_attr,
                                                                   const std::string& proj_attr,
                                                                   const std::string& key_value,
                                                                   const std::size_t limit,
                                                                   const newdb::TableSchema& schema,
                                                                   const newdb::HeapTable& table);

void invalidate_covering_sidecars_for_data_file(const std::string& data_file);
void invalidate_covering_sidecars_for_attrs(const std::string& data_file,
                                            const std::set<std::string>& attrs);
