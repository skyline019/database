#pragma once

#include <newdb/error.h>
#include <newdb/heap_storage.h>
#include <newdb/heap_table.h>
#include <newdb/schema.h>

#include <vector>

namespace newdb::io {

// Read all pages, verify CRC, merge append-only versions + tombstones (__deleted=1).
// When `opts.lazy_decode` is true, keeps rows on disk / mmap (`HeapTable::is_heap_storage_backed()`).
Status load_heap_file(const char* path,
                      const std::string& table_name,
                      const TableSchema& schema,
                      HeapTable& out,
                      const HeapLoadOptions& opts = {});

// Append one row as a new physical record on the tail page (or new page).
Status append_row(const char* path, const Row& row);

// Append multiple rows in one I/O session (higher throughput than repeated append_row).
Status append_rows(const char* path, const std::vector<Row>& rows);

// Create (truncate) a heap file from seed rows.
Status create_heap_file(const char* path, const std::vector<Row>& rows);

// Debug / ad-hoc scan of raw pages (stdout).
void scan_heap_file(const char* path);

// Scan pages for rows where integer column `attr_name` >= min_balance (no index; demo / tooling).
void query_attr_int_ge(const char* path, const char* attr_name, int min_balance);

// Same as query_attr_int_ge with attr_name fixed to "balance".
void query_balance_ge(const char* path, int min_balance);

} // namespace newdb::io
