#pragma once

#include <newdb/heap_table.h>
#include <newdb/schema.h>

#include <cstddef>
#include <vector>

namespace table_view {

/// Stable keyset slice on `id` ordering: `descending=false` keeps rows with id > after_id;
/// `descending=true` keeps rows with id < after_id. No-op when `use_keyset` is false.
std::vector<std::size_t> page_indices_keyset_after_id(const newdb::HeapTable& tbl,
                                                      const std::vector<std::size_t>& sorted_indices,
                                                      bool descending,
                                                      int after_id,
                                                      bool use_keyset);

void print_page_indexed(const newdb::TableSchema& schema,
                         const newdb::HeapTable& tbl,
                         const std::vector<std::size_t>& row_indices,
                         std::size_t page_no,
                         std::size_t page_size);

} // namespace table_view
