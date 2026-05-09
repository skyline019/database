#pragma once

#include <cstddef>
#include <cstdint>

namespace newdb {
struct HeapTable;
struct Row;
}

/// Wraps `memory_registry` for QueryTemp so `plan_impl.cc` does not include engine alloc headers.
bool where_plan_impl_try_admit_query_temp(std::uint64_t bytes);
void where_plan_impl_release_query_temp(std::uint64_t bytes);

/// Row decode for heap-backed vs in-memory table rows (implementation in `plan_impl_support.cc`).
bool where_plan_impl_row_at_logical_slot(const newdb::HeapTable& tbl, std::size_t slot, newdb::Row& out);
