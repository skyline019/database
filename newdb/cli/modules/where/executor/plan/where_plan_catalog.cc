#include <waterfall/config.h>

#include "cli/modules/where/executor/plan/where_plan_catalog.h"

#include "cli/modules/sidecar/eq/equality_index_sidecar.h"
#include "cli/modules/sidecar/visibility/visibility_checkpoint_sidecar.h"

#include <newdb/heap_table.h>
#include <newdb/schema.h>

#include <numeric>

std::vector<std::size_t> where_plan_catalog_visibility_slots(const newdb::HeapTable& tbl,
                                                             const newdb::TableSchema& schema,
                                                             const std::size_t logical_row_count) {
    if (tbl.name.empty()) {
        std::vector<std::size_t> all(logical_row_count);
        std::iota(all.begin(), all.end(), 0);
        return all;
    }
    return load_or_build_visibility_checkpoint_sidecar(tbl.name + ".bin", schema, tbl);
}

WherePlanEqLookupResult where_plan_catalog_eq_lookup(const newdb::HeapTable& tbl,
                                                     const newdb::TableSchema& schema,
                                                     const std::string& attr_name,
                                                     const std::string& value,
                                                     WhereQueryContext* where_obs) {
    const EqLookupResult inner = lookup_or_build_eq_index_sidecar(
        EqIndexRequest{
            .data_file = tbl.name + ".bin",
            .attr_name = attr_name,
            .table_name = tbl.name,
        },
        schema,
        tbl,
        value,
        where_obs);
    WherePlanEqLookupResult out;
    out.used_index = inner.used_index;
    out.slots = inner.slots;
    return out;
}

void where_plan_catalog_prewarm_eq_probe(const newdb::HeapTable& tbl,
                                       const newdb::TableSchema& schema,
                                       const std::string& attr_name,
                                       const std::string& probe_value,
                                       WhereQueryContext& ctx) {
    (void)lookup_or_build_eq_index_sidecar(
        EqIndexRequest{
            .data_file = tbl.name + ".bin",
            .attr_name = attr_name,
            .table_name = tbl.name,
        },
        schema,
        tbl,
        probe_value,
        &ctx);
}
