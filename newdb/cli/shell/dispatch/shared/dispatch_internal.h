#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace newdb {
struct HeapTable;
struct Row;
struct TableSchema;
struct AttrMeta;
} // namespace newdb

#include "cli/modules/common/util/utils.h"
#include "cli/shell/state/shell_state_fwd.h"

struct WhereCond;

// ---- shared low-level helpers (args / parsing / validation) ----
// strcasecmp_ascii / strncasecmp_ascii: see utils.h

bool row_at_slot_read(const newdb::HeapTable& tbl, std::size_t i, newdb::Row& r);
bool parse_int64_fast(const std::string& s, long long& out);

bool validate_typed_attr_value(const char* tag,
                               const char* log_file,
                               const newdb::AttrMeta& meta,
                               const std::string& value);

void refresh_schema_if_missing(ShellState& st, const std::string& eff_data);

bool all_and_chain_fast(const std::vector<WhereCond>& conds);
bool is_index_friendly_single(const WhereCond& c, const newdb::TableSchema& schema);
std::size_t seed_cost_simple(const WhereCond& c, const newdb::TableSchema& schema);

// ---- hot index helpers ----
void fast_index_insert(newdb::HeapTable& tbl,
                       const newdb::TableSchema& schema,
                       const newdb::Row& row,
                       std::size_t slot);
void fast_index_update_slot(newdb::HeapTable& tbl,
                            const newdb::TableSchema& schema,
                            const newdb::Row& old_row,
                            const newdb::Row& new_row,
                            std::size_t slot);
void fast_index_remove_slot(newdb::HeapTable& tbl,
                            const newdb::TableSchema& schema,
                            const newdb::Row& removed_row,
                            std::size_t removed_slot,
                            const std::optional<newdb::Row>& moved_row);

// ---- sidecar invalidation ----
void invalidate_eq_sidecars_after_write(const std::string& eff_data);
void invalidate_eq_sidecars_after_write(const std::string& eff_data,
                                        const std::set<std::string>& attrs);

// ---- command handlers ----
bool handle_txn_commands(ShellState& st, const char* line, const char* log_file, const std::string& current_table);

bool handle_query_page_command(ShellState& st,
                               const char* line,
                               const char* log_file,
                               const std::string& eff_data,
                               newdb::HeapTable& tbl);
bool handle_query_where_count_commands(ShellState& st,
                                       const char* line,
                                       const char* log_file,
                                       const std::string& eff_data,
                                       const std::string& current_table,
                                       newdb::HeapTable& tbl);
bool handle_query_min_max_commands(ShellState& st,
                                   const char* line,
                                   const char* log_file,
                                   newdb::HeapTable& tbl);
bool handle_query_find_commands(ShellState& st,
                                const char* line,
                                const char* log_file,
                                const std::string& eff_data,
                                newdb::HeapTable& tbl);
bool handle_query_sum_avg_commands(ShellState& st,
                                   const char* line,
                                   const char* log_file,
                                   const std::string& eff_data,
                                   newdb::HeapTable& tbl);

bool handle_ddl_create_use_commands(ShellState& st,
                                    const char* line,
                                    const char* log_file,
                                    const std::string& eff_data,
                                    std::string& current_table,
                                    std::string& current_file);
bool handle_ddl_alter_rename_commands(ShellState& st,
                                      const char* line,
                                      const char* log_file,
                                      const std::string& eff_data,
                                      std::string& current_table,
                                      std::string& current_file);
bool handle_schema_show_commands(ShellState& st,
                                 const char* line,
                                 const char* log_file,
                                 const std::string& current_table,
                                 const std::string& current_file);
bool handle_schema_key_command(ShellState& st,
                               const char* line,
                               const char* log_file,
                               const std::string& eff_data,
                               newdb::HeapTable& tbl);
bool handle_schema_catalog_commands(ShellState& st, const char* line, const char* log_file);

bool handle_dml_insert_command(ShellState& st,
                               const char* line,
                               const char* log_file,
                               const std::string& eff_data,
                               const std::string& current_table,
                               const std::string& current_file);
bool handle_dml_update_delete_commands(ShellState& st,
                                       const char* line,
                                       const char* log_file,
                                       const std::string& eff_data,
                                       const std::string& current_table,
                                       newdb::HeapTable& tbl);
bool handle_dml_attr_commands(ShellState& st,
                              const char* line,
                              const char* log_file,
                              const std::string& eff_data,
                              const std::string& current_table,
                              newdb::HeapTable& tbl);

bool handle_export_command(ShellState& st,
                           const char* line,
                           const char* log_file,
                           const std::string& current_table,
                           const std::string& current_file,
                           newdb::HeapTable& tbl);
bool handle_import_defattr_commands(ShellState& st,
                                    const char* line,
                                    const char* log_file,
                                    const std::string& eff_data,
                                    const std::string& current_file);
bool handle_workspace_admin_commands(ShellState& st,
                                     const char* line,
                                     const char* log_file,
                                     const std::string& eff_data,
                                     std::string& current_table,
                                     std::string& current_file);

bool handle_session_commands(const char* line, const char* log_file, bool& handled);

