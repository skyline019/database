#pragma once

#include <newdb/heap_table.h>
#include <newdb/schema.h>

#include <cstddef>
#include <vector>

namespace table_view {

void print_page_indexed(const newdb::TableSchema& schema,
                         const newdb::HeapTable& tbl,
                         const std::vector<std::size_t>& row_indices,
                         std::size_t page_no,
                         std::size_t page_size);

} // namespace table_view
