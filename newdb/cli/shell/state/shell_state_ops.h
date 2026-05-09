#pragma once

#include <newdb/error.h>

#include <string>

#include "cli/shell/state/shell_state_fwd.h"

namespace newdb {
struct HeapTable;
struct TableSchema;
} // namespace newdb

newdb::HeapTable* get_cached_table(ShellState& st);

void shell_invalidate_session_table(ShellState& st);

newdb::Status newdb_materialize_heap_if_lazy(newdb::HeapTable& t,
                                             const newdb::TableSchema& sch,
                                             ShellState* stats_sink = nullptr);

void reload_schema_from_data_path(ShellState& st, const std::string& data_path);
