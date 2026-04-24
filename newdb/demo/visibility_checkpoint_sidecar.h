#pragma once

#include <cstddef>
#include <set>
#include <string>
#include <vector>

#include <newdb/heap_table.h>
#include <newdb/schema.h>

std::vector<std::size_t> load_or_build_visibility_checkpoint_sidecar(const std::string& data_file,
                                                                     const newdb::TableSchema& schema,
                                                                     const newdb::HeapTable& table);

void invalidate_visibility_checkpoint_sidecars_for_data_file(const std::string& data_file);
void invalidate_visibility_checkpoint_sidecars_for_attrs(const std::string& data_file,
                                                         const std::set<std::string>& attr_names);
