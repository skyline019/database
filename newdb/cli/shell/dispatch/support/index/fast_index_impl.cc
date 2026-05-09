#include <waterfall/config.h>

#include <newdb/heap_table.h>

#include <cstddef>
#include <optional>
#include <string>

#include "cli/shell/dispatch/shared/dispatch_internal.h"

void fast_index_insert(newdb::HeapTable& tbl,
                       const newdb::TableSchema& schema,
                       const newdb::Row& row,
                       const std::size_t slot) {
    tbl.index_by_id[row.id] = slot;
    if (!schema.primary_key.empty() && schema.primary_key != "id") {
        std::string pkv;
        if (newdb::HeapTable::row_get_pk_value(row, schema.primary_key, pkv)) {
            tbl.index_by_pk_value[pkv] = slot;
        }
    }
    if (tbl.attr_index.empty() && !schema.attrs.empty()) {
        for (std::size_t i = 0; i < schema.attrs.size(); ++i) {
            tbl.attr_index[schema.attrs[i].name] = i;
        }
    }
    // Any write invalidates ordering caches.
    tbl.sorted_cache_asc.clear();
    tbl.sorted_cache_desc.clear();
}

void fast_index_update_slot(newdb::HeapTable& tbl,
                            const newdb::TableSchema& schema,
                            const newdb::Row& old_row,
                            const newdb::Row& new_row,
                            const std::size_t slot) {
    tbl.index_by_id.erase(old_row.id);
    tbl.index_by_id[new_row.id] = slot;
    if (!schema.primary_key.empty() && schema.primary_key != "id") {
        std::string old_pk;
        if (newdb::HeapTable::row_get_pk_value(old_row, schema.primary_key, old_pk)) {
            tbl.index_by_pk_value.erase(old_pk);
        }
        std::string new_pk;
        if (newdb::HeapTable::row_get_pk_value(new_row, schema.primary_key, new_pk)) {
            tbl.index_by_pk_value[new_pk] = slot;
        }
    }
    tbl.sorted_cache_asc.clear();
    tbl.sorted_cache_desc.clear();
}

void fast_index_remove_slot(newdb::HeapTable& tbl,
                            const newdb::TableSchema& schema,
                            const newdb::Row& removed_row,
                            const std::size_t removed_slot,
                            const std::optional<newdb::Row>& moved_row) {
    tbl.index_by_id.erase(removed_row.id);
    if (!schema.primary_key.empty() && schema.primary_key != "id") {
        std::string old_pk;
        if (newdb::HeapTable::row_get_pk_value(removed_row, schema.primary_key, old_pk)) {
            tbl.index_by_pk_value.erase(old_pk);
        }
    }
    // O(1) remove path: swap-with-last then pop, only fix moved row slot.
    if (moved_row.has_value()) {
        tbl.index_by_id[moved_row->id] = removed_slot;
        if (!schema.primary_key.empty() && schema.primary_key != "id") {
            std::string moved_pk;
            if (newdb::HeapTable::row_get_pk_value(*moved_row, schema.primary_key, moved_pk)) {
                tbl.index_by_pk_value[moved_pk] = removed_slot;
            }
        }
    }
    tbl.sorted_cache_asc.clear();
    tbl.sorted_cache_desc.clear();
}




