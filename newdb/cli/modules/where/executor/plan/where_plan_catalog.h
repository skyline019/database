#pragma once

#include <cstddef>
#include <string>
#include <vector>

namespace newdb {
struct HeapTable;
struct TableSchema;
}  // namespace newdb

struct WhereQueryContext;

/// Narrow bridge for WHERE planning: sidecar load/build lives in `where_plan_catalog.cc` only.
std::vector<std::size_t> where_plan_catalog_visibility_slots(const newdb::HeapTable& tbl,
                                                             const newdb::TableSchema& schema,
                                                             std::size_t logical_row_count);

struct WherePlanEqLookupResult {
    bool used_index{false};
    std::vector<std::size_t> slots;
};

WherePlanEqLookupResult where_plan_catalog_eq_lookup(const newdb::HeapTable& tbl,
                                                     const newdb::TableSchema& schema,
                                                     const std::string& attr_name,
                                                     const std::string& value,
                                                     WhereQueryContext* where_obs);

void where_plan_catalog_prewarm_eq_probe(const newdb::HeapTable& tbl,
                                       const newdb::TableSchema& schema,
                                       const std::string& attr_name,
                                       const std::string& probe_value,
                                       WhereQueryContext& ctx);
