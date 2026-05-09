#include <waterfall/config.h>

#include "cli/modules/where/executor/plan/plan_impl_support.h"

#include <newdb/heap_table.h>
#include <newdb/memory_registry.h>
#include <newdb/row.h>

bool where_plan_impl_try_admit_query_temp(const std::uint64_t bytes) {
    return newdb::memory_registry_try_admit(newdb::MemoryKind::QueryTemp, bytes);
}

void where_plan_impl_release_query_temp(const std::uint64_t bytes) {
    newdb::memory_registry_release(newdb::MemoryKind::QueryTemp, bytes);
}

bool where_plan_impl_row_at_logical_slot(const newdb::HeapTable& tbl, const std::size_t slot, newdb::Row& out) {
    if (tbl.is_heap_storage_backed()) {
        return tbl.decode_heap_slot(slot, out);
    }
    out = tbl.rows[slot];
    return true;
}
