#pragma once

#include <newdb/heap_table.h>
#include <newdb/schema.h>

#include <string>

bool export_table_csv(const newdb::TableSchema& schema,
                      const newdb::HeapTable& tbl,
                      const std::string& filename,
                      const char* log_file);

bool export_table_json(const newdb::TableSchema& schema,
                       const newdb::HeapTable& tbl,
                       const std::string& filename,
                       const char* log_file);
