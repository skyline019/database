#pragma once

#include <cstddef>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace structdb::facade {
class Engine;
}

namespace structdb::client {
class EmbedClient;
}

#include "structdb/client/mdb_logical_table.hpp"
#include "structdb/client/mdb_persistence.hpp"
#include "structdb/client/mdb_query_paging.hpp"
#include "structdb/infra/long_task_progress.hpp"

namespace structdb::client::mdb {

struct ReplSessionState;

/// Runs `.mdb` scripts with newdb-like line semantics (trim, `#` comments, empty lines skipped).
/// `session.txn`：BEGIN 时写入基线快照，并在 `\nTXNV2\n` 之后追加 **v2 增量行**（`V2OP\tKIND\t<hex(payload)>`）；崩溃恢复时重放到 `current`。
/// 若 `MdbRunOptions::fsync_each_session_txn_op` / REPL 的 `fsync_session_txn_op` 为 false，尾部 OP 可能在崩溃时丢失（见 `Docs/CHANGELOG.md`）。
/// **READ VIEW（`TXNISOLATION snapshot`）**：`BEGIN` 将 `txn_snap_seq` 固定为当时的 `engine.latest_commit_seq()` 并写入 `session.txn` 的 `SNAP`；从 `session.txn` 恢复后 **不重算** `txn_snap_seq`（仍为文件中 `SNAP`），也 **不回拨**；事务内存储读使用该序号，因此可能低于重启后的 `latest_commit_seq`（只读视图不自动“追上”新提交，除非切换 `read_committed` 或新会话）。**事务激活时不得执行 `TXNISOLATION`**（实现将拒绝）。
///
/// 与 `Engine::kv_get(..., read_max_seq)` 的对应关系见 `Docs/POLICY.md` §4.1；本函数为脚本/REPL 在调用 `mdb_dispatch_execute_line` 前计算 **`storage_read_seq`** 的单一入口。
/// When `txn_active` and snapshot mode, the returned seq is `min(txn_snap_seq, engine.latest_commit_seq())` so
/// `read_max_seq` never exceeds the engine watermark (defensive; correct sessions have `txn_snap_seq <= latest`).
std::uint64_t mdb_storage_read_seq_for_script(facade::Engine& engine, EmbedClient& client, bool txn_active,
                                              bool txn_read_committed, std::uint64_t txn_snap_seq);
/// `run_mdb_script` / `mdb_repl_execute_line` 在分派前计算 `storage_read_seq`（见 `mdb_storage_read_seq_for_script`）；命令体在 `mdb_dispatch.cpp` 中经 `mdb_dispatch_execute_line` 执行（`Docs/PHASE26.md`）。
/// Commands are classified via `mdb_parse_command_line` (newdb phase-1/2 prefix alignment).
/// Table snapshots use `mdbhex1:` + hex for embed journal safety; v2 row/schema/catalog keys use `mdb$v2$*`
/// (see `structdb/storage/mdb_keyspace.hpp`). Reads go through `Engine::kv_get` (MemTable + SST).
struct MdbRunOptions {
  std::filesystem::path script_path;
  /// If true, append script output lines to this buffer (e.g. `[COUNT]`, `[PAGE]`, errors).
  std::vector<std::string>* log_sink{nullptr};
  /// Passed to `persist_table` as fsync flag → `EmbedClient::submit(..., fsync_journal)` on COMMIT/persist paths.
  /// **InnoDB durability analogy** (`Docs/TXN_INNODB_MAP.md` §2): contributes to **Level 1** when true (batch WAL fsync boundary).
  bool fsync_each_batch{false};
  /// After each `session.txn` v2 op line appended while `BEGIN` is active, fsync the file (stronger crash safety).
  /// Complements `fsync_each_batch` (WAL); see `Docs/TXN_INNODB_MAP.md` §2 for **InnoDB durability analogy** tables.
  bool fsync_each_session_txn_op{false};
  /// If true, `run_mdb_script` returns failure when the script ends with an open `BEGIN` (no `COMMIT`/`ROLLBACK`).
  /// Default false: EOF implicitly rolls back in-memory session state only (same as today).
  bool fail_if_unclosed_txn{false};
  /// Per-script latch ANDed with `EngineConfigSnapshot::mdb_persist_in_begin`. When false, skips `persist_table`
  /// while `BEGIN` is active even if the engine allows it (tests / compatibility). Default true.
  bool allow_persist_while_txn_active_experimental{true};
  /// Optional cooperative cancel + progress (`units` = executable script lines). Not owned.
  infra::LongTaskReporter* long_task{nullptr};
};

struct MdbRunResult {
  bool ok{true};
  std::size_t last_line_no{0};
  std::string last_error;
  /// When true, interactive REPL should exit (after processing `EXIT`).
  bool repl_exit_requested{false};
  /// Set when `MdbRunOptions::long_task` cancel was requested before finishing all lines.
  bool cancelled{false};
};

/// Executes script lines through `EmbedClient` (must already be `open`). Stops on hard errors (unknown command,
/// parse errors, type mismatch on INSERT/UPDATE) similar to newdb script behavior.
MdbRunResult run_mdb_script(facade::Engine& engine, EmbedClient& client, const MdbRunOptions& opt);

/// Persistent line-oriented MDB session (current table, txn, savepoints) for REPL or embedding.
class MdbInteractiveSession {
 public:
  MdbInteractiveSession();
  ~MdbInteractiveSession();
  MdbInteractiveSession(const MdbInteractiveSession&) = delete;
  MdbInteractiveSession& operator=(const MdbInteractiveSession&) = delete;
  MdbInteractiveSession(MdbInteractiveSession&& o) noexcept;
  MdbInteractiveSession& operator=(MdbInteractiveSession&& o) noexcept;

  void set_allow_persist_while_txn_active_experimental(bool enable);
  bool allow_persist_while_txn_active_experimental() const;

 private:
  friend MdbRunResult mdb_repl_execute_line(facade::Engine& engine, EmbedClient& client, MdbInteractiveSession& session,
                                            std::string_view line, std::vector<std::string>* log_sink,
                                            bool fsync_each_batch, bool fsync_session_txn_op, std::string* err_out,
                                            bool allow_persist_while_txn_active_experimental);
  friend void mdb_repl_reset(MdbInteractiveSession& session);
  std::unique_ptr<ReplSessionState> impl_;
};

/// Runs one trimmed script line using `session` state; updates `session.last_line` semantics via internal counter.
MdbRunResult mdb_repl_execute_line(facade::Engine& engine, EmbedClient& client, MdbInteractiveSession& session,
                                   std::string_view line, std::vector<std::string>* log_sink, bool fsync_each_batch,
                                   bool fsync_session_txn_op, std::string* err_out,
                                   bool allow_persist_while_txn_active_experimental = true);

void mdb_repl_reset(MdbInteractiveSession& session);

/// Snapshot key for a logical table (ASCII table name).
/// Legacy `mdb$v1$table$` hex snapshot; **not** written once a table has v2 schema (`mdb$v2$schema$` path).
std::string mdb_table_snapshot_key(std::string_view table_name);

/// Decodes a value returned by `Engine::kv_get(mdb_table_snapshot_key(...))` to the internal snapshot text
/// (`v1` …) for assertions or tooling. Returns false if the payload is missing or malformed.
bool mdb_decode_stored_snapshot(std::string_view stored, std::string* decoded_out);

}  // namespace structdb::client::mdb
