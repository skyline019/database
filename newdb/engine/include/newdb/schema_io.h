#pragma once

#include <newdb/error.h>
#include <newdb/schema.h>

#include <string>

namespace newdb {

// Plain-text schema sidecar (transparent): UTF-8 lines, no XOR.
// Lines:
//   PRIMARY_KEY:name
//   HEAP_FORMAT:uint   (written on save; 0 when missing on load = legacy)
//   SCHEMA:optional_label
//   attr:type
Status save_schema_text(const std::string& path, const TableSchema& schema);
Status load_schema_text(const std::string& path, TableSchema& out_schema);

std::string schema_sidecar_path_for_data_file(const std::string& data_file);

} // namespace newdb
