#pragma once

#include "structdb/client/detail/mdb_engine_ports.hpp"
#include "structdb/client/mdb_logical_table.hpp"
#include "structdb/client/mdb_persistence.hpp"
#include "structdb/client/mdb_query_paging.hpp"
#include "structdb/client/mdb_command_parser.hpp"
#include "structdb/client/mdb_runner.hpp"
#include "structdb/facade/config.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <map>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace structdb::client {
class EmbedClient;
struct CommandBatch;
}  // namespace structdb::client

namespace structdb::facade {
class Engine;
}

namespace structdb::client::mdb {

/// Maps TXN_INNODB_MAP §2 session durability to embed fsync flags.
struct MdbDurabilityFsyncFlags {
  bool fsync_each_batch{false};
  bool fsync_each_session_txn_op{false};
};

MdbDurabilityFsyncFlags mdb_durability_level_to_fsync(int level);
/// Applies optional session level over REPL/script defaults.
MdbDurabilityFsyncFlags mdb_effective_durability_fsync(std::optional<int> session_level, bool default_batch,
                                                        bool default_session_txn_op);

struct ReplSessionState {
  LogicalTable current;
  /// Session override when script runs `IMPORT MODE ON|OFF` (else uses engine `mdb_bulk_import_mode`).
  std::optional<bool> bulk_import_mode_override;
  /// `SET DURABILITY n` for this REPL session (`n` in 0..2); unset = use caller defaults.
  std::optional<int> session_durability_level;
  /// `IMPORT SEGMENT (token)` suffix for persist idempotency tokens (PHASE44).
  std::optional<std::string> import_segment_token;
  bool txn_active = false;
  LogicalTable txn_base;
  bool txn_read_committed = false;
  std::uint64_t txn_snap_seq = 0;
  std::map<std::string, LogicalTable> savepoints;
  std::size_t repl_line_no = 0;
  bool attempted_txn_recovery = false;
  bool allow_persist_while_txn_active_experimental = false;
  std::optional<std::size_t> txn_undo_stack_depth_at_begin;
  MdbQueryPagingState query_paging;
};

constexpr std::string_view kSnapPrefix = "mdb$v1$table$";
std::string snapshot_key_for(std::string_view table);

bool ascii_starts_with_ci(std::string_view s, std::string_view pfx);
void trim_inplace(std::string& s);
std::string trim_copy(std::string_view sv);

bool is_int_literal(std::string_view s);
bool field_type_is_char(std::string_view typ);
bool known_logical_type(std::string_view typ);
bool parse_full_double_str(const std::string& t, double* out);
bool is_double_literal(std::string_view sv);
bool is_float_literal(std::string_view sv);
bool parse_datetime_calendar(std::string_view s, int* y, int* mo, int* d, int* hh, int* mi, int* ss, bool* has_time);
bool is_datetime_literal(std::string_view s);
int cmp_datetime_parts(int y1, int mo1, int d1, int h1, int mi1, int s1, int y2, int mo2, int d2, int h2, int mi2, int s2);

bool type_matches(std::string_view type, std::string_view value);
/// Default cell for a newly appended column (ADDATTR); must satisfy `type_matches(type, value)`.
/// String-like types use `"0"` so existing rows match GUI quick-insert / grid placeholders (not empty).
std::string default_cell_for_new_column(std::string_view logical_type);
std::string type_mismatch_msg(std::string_view col, std::string_view type, std::string_view got);

std::string hex_encode(std::string_view in);
bool hex_decode(std::string_view in, std::string* out);
std::string wire_encode_snapshot_blob(std::string_view raw,
                                      structdb::facade::MdbWireEncoding enc = structdb::facade::MdbWireEncoding::Hex);
bool wire_decode_snapshot_blob(std::string_view stored, std::string* raw_out, std::string* err);

void logical_row_index_rebuild_from_rows(LogicalTable* t);
void logical_row_index_insert(LogicalTable* t, const std::string& id);
void logical_row_index_remove(LogicalTable* t, const std::string& id);
std::string logical_row_index_newline_blob(const LogicalTable& t);

/// Numeric-aware row-id compare when both operands are int literals; else lexicographic.
int compare_row_ids(std::string_view a, std::string_view b);
bool logical_row_ids_all_int_literals(const std::vector<std::string>& ids);
void logical_row_ids_sort_numeric(std::vector<std::string>& ids, bool descending);
bool logical_row_ids_is_numeric_sorted(const std::vector<std::string>& ids, bool descending);
/// When `row_ids_ordered` matches `rows.size()` and is numeric-sorted, use for O(page) id paging.
const std::vector<std::string>* logical_row_ids_for_id_page(const LogicalTable& t, bool descending,
                                                            std::vector<std::string>* scratch);

std::string explain_where_index_hint(const LogicalTable& t, std::string_view pred_col, std::string_view pred_op,
                                     std::string_view pred_val);

std::vector<std::string_view> split_pipe_rows(std::string_view inner);
std::string json_quote_cell(std::string_view s);
bool parse_export_json_path(const std::string& tail, std::filesystem::path* path_out, std::string* err);

std::string_view effective_pk_column_name(const LogicalTable& t);
std::string serialize_table(const LogicalTable& t);
bool deserialize_table(std::string_view blob, LogicalTable* out, std::string* err);

bool parse_u64_dec_sv(std::string_view s, std::uint64_t* out);
bool col_name_eq(std::string_view a, std::string_view b);
bool is_string_type(std::string_view typ);
bool is_int_type(std::string_view typ);
int schema_col_index(const LogicalTable& t, std::string_view col);

void logical_rebuild_str_index(LogicalTable* t);
void logical_str_index_remove_row(LogicalTable* t, const std::string& id, const std::vector<std::string>& cells);
void logical_str_index_add_row(LogicalTable* t, const std::string& id, const std::vector<std::string>& cells);
void logical_rebuild_int_index(LogicalTable* t);
void logical_int_index_remove_row(LogicalTable* t, const std::string& id, const std::vector<std::string>& cells);
void logical_int_index_add_row(LogicalTable* t, const std::string& id, const std::vector<std::string>& cells);
void logical_col_sort_invalidate(LogicalTable* t);
/// Cached row-id order for non-id sort column; builds lazily into `t.col_sort_cache`.
const std::vector<std::string>* logical_col_sort_order(const LogicalTable& t, std::string_view sort_col, bool desc);
LogicalTable clone_table(const LogicalTable& s);

void logical_persist_clear_dirty(LogicalTable* t);
void logical_persist_invalidate_incremental(LogicalTable* t);
void logical_persist_mark_insert(LogicalTable* t, const std::string& id);
void logical_persist_mark_update(LogicalTable* t, const std::string& id, std::vector<std::string> prev_cells);
void logical_persist_mark_delete_before_erase(LogicalTable* t, const std::string& id,
                                              const std::vector<std::string>& prev_cells);
void logical_persist_mark_schema_change(LogicalTable* t);

void mdb_append_iso_datetime(std::ostringstream& o, int y, int mo, int d, int h, int mi, int s);

bool op_is_like(std::string_view op_trimmed);
bool compare_scalar(std::string_view cell, std::string_view op, std::string_view rhs);
bool compare_typed_cell(std::string_view cell, std::string_view col_type, std::string_view pred_op,
                        std::string_view pred_val);
bool row_matches_predicate(const LogicalTable& t, const std::string& row_id, const std::vector<std::string>& cells,
                           std::string_view pred_col, std::string_view pred_op, std::string_view pred_val);
std::size_t count_matching_row_ids(const LogicalTable& t, std::string_view pred_col, std::string_view pred_op,
                                   std::string_view pred_val, const MdbEnginePorts* ports = nullptr,
                                   std::uint64_t read_max_seq = 0);
std::vector<std::string> collect_matching_row_ids(const LogicalTable& t, std::string_view pred_col,
                                                   std::string_view pred_op, std::string_view pred_val,
                                                   const MdbEnginePorts* ports = nullptr,
                                                   std::uint64_t read_max_seq = 0,
                                                   std::size_t max_collect = 0);

std::vector<std::string> split_csv_paren_content(std::string_view inner);
bool extract_paren_block(std::string_view line, std::size_t open_pos, std::string_view* inner, std::string* err);
bool parse_defattrs(std::string_view inner, std::vector<std::pair<std::string, std::string>>* attrs,
                    std::string* err);
/// Append or insert one `name:type` column (`inner` is `name:type`, or `index,name:type` with decimal index).
/// Used by ADDATTR and txn replay.
bool mdb_apply_addattr_inner(LogicalTable* current, std::string_view inner, std::string* err);
/// Maps `pl.paren_inner` to txn v2 payload: `name:type` or `index` + TAB + `name:type`.
bool mdb_addattr_paren_inner_to_txn_payload(std::string_view paren_inner, std::string* out, std::string* err);
/// Maps txn v2 ADDATTR payload back to `inner` accepted by `mdb_apply_addattr_inner`.
bool mdb_addattr_txn_payload_to_apply_inner(std::string_view payload, std::string* inner_out, std::string* err);

std::filesystem::path txn_log_path_for(const structdb::client::EmbedClient& c);
void txn_log_remove_if_exists(const structdb::client::EmbedClient& c);
bool txn_log_write_begin(const structdb::client::EmbedClient& client, const LogicalTable& txn_base_snapshot,
                         bool txn_read_committed, std::uint64_t txn_snap_seq, std::string* err);
bool txn_log_append_v2_op(const structdb::client::EmbedClient& client, std::string_view kind,
                          std::string_view payload_utf8, bool fsync_wal, std::string* err);
bool split_v2op_line(std::string_view line, std::string_view* kind, std::string_view* hex_payload, std::string* err);
bool txn_replay_one_v2_op(const MdbEnginePorts& ports, LogicalTable* current, std::uint64_t read_max_seq,
                          std::string_view line, std::string* err);

/// Mutates `current` rows + `str_idx` on success; sets `log_line_out` to one `[REORDER_MAP_JSON]…` line.
bool mdb_confirm_reorder_execute(LogicalTable* current, std::string_view paren_inner, std::string* log_line_out,
                                 std::string* err);

void log_line(std::vector<std::string>* sink, const std::string& s);
bool txn_log_try_recover_repl_session(const MdbEnginePorts& ports, ReplSessionState* st,
                                      std::vector<std::string>* log_sink, std::string* err_out);

void append_embed_journal_tail(const structdb::client::EmbedClient& client, std::size_t max_lines,
                               std::vector<std::string>* sink);

bool import_mdb_directory(const MdbEnginePorts& ports, std::string_view dir_path_sv,
                        std::vector<std::string>* log_accum, std::string* err);

bool mdb_session_bulk_import_active(const structdb::facade::Engine& engine,
                                    const std::optional<bool>& session_override, const MdbRunOptions& opt);
bool mdb_session_persist_coalesce_active(const structdb::facade::Engine& engine, const MdbRunOptions& opt);
bool mdb_script_amortize_bulk_dml_active(const structdb::facade::Engine& engine, const MdbRunOptions& opt);
bool mdb_flush_coalesced_persist(const MdbEnginePorts& ports, LogicalTable& current, const std::string& idem,
                                 bool fsync, std::string* err);
void mdb_ports_set_bulk_import_persist(MdbEnginePorts* ports, bool bulk_active);

/// When import segment token set, persist uses `idem:import:<table>:seg:<token>`.
std::string mdb_resolve_persist_idem(const std::string& base_idem, const LogicalTable& t,
                                     const std::optional<std::string>* import_segment_token);

}  // namespace structdb::client::mdb
