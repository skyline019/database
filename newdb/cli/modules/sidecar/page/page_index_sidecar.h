#pragma once

#include <cstddef>
#include <set>
#include <string>
#include <vector>

#include <newdb/heap_table.h>
#include <newdb/schema.h>

struct PageSidecarRequest {
    std::string data_file;
    std::string table_name;
    std::string order_key;
    bool descending{false};
};

// Build or reuse persistent sidecar index for PAGE sorting.
// Returns row-slot indices sorted by `order_key` (same slot semantic as HeapTable::sorted_indices).
std::vector<std::size_t> load_or_build_page_index_sidecar(const PageSidecarRequest& req,
                                                          const newdb::TableSchema& schema,
                                                          const newdb::HeapTable& table);

void invalidate_page_index_sidecars_for_data_file(const std::string& data_file);
void invalidate_page_index_sidecars_for_order_attrs(const std::string& data_file,
                                                    const std::set<std::string>& order_key_names);
