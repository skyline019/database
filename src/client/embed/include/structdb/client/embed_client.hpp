#pragma once

#include <cstdint>
#include <filesystem>
#include <limits>
#include <mutex>
#include <string>
#include <unordered_set>
#include <vector>

#include "structdb/infra/file_handle.hpp"

namespace structdb::facade {
class Engine;
}

namespace structdb::client {

/// Subdirectory under `EmbedClient::open(session_dir)` where journal, checkpoint, activity log, and `session.txn`
/// are stored so `session_dir` stays clean for host-owned files.
inline constexpr const char kEmbedSessionArtifactsDir[] = "_structdb_embed";

struct CommandBatch {
  std::string client_session_id;
  std::uint64_t term{0};
  std::string idempotency_token;
  std::vector<std::pair<std::string, std::string>> puts;
  /// Keys to remove (tombstone in storage); encoded in journal when non-empty.
  std::vector<std::string> dels;
};

/// Embedded durable session: journal of batches + checkpointed ack seq.
class EmbedClient {
 public:
  explicit EmbedClient(facade::Engine& engine);

  bool open(const std::filesystem::path& session_dir, std::string* error_out = nullptr);
  void close();

  /// Session directory passed to `open` (empty until opened).
  const std::filesystem::path& session_directory() const { return dir_; }

  /// Append-only embed batch journal (`session.journal` under `session_directory()/kEmbedSessionArtifactsDir`).
  const std::filesystem::path& embed_journal_path() const { return journal_path_; }

  /// Append-only session activity log (`session_log.txt`): each successful `open` appends a **SESSION_OPEN** line (UTC
  /// time, pid, seq watermarks); `close` appends **SESSION_CLOSE**. When the file exceeds a size cap it is renamed to
  /// `session_log.arch.<UTC>.txt` and the oldest archives are pruned to bound disk use.
  const std::filesystem::path& embed_session_log_path() const { return session_log_path_; }

  /// Applies batch to engine storage and appends to journal; optional fsync.
  /// When `fsync_journal` is true, issues one `StorageEngine::wal_sync()` after all puts/dels (batch WAL durability boundary).
  /// **InnoDB durability analogy** (`Docs/TXN_INNODB_MAP.md` §2, `POLICY` §4.5): **Level 1** when true for that batch.
  /// Replays with the same non-empty `idempotency_token` are ignored (already committed).
  bool submit(const CommandBatch& batch, bool fsync_journal, std::string* error_out = nullptr);
  /// When `write_journal` is false, storage/WAL still commit; journal line skipped (bulk import; WAL authoritative).
  bool submit_ex(const CommandBatch& batch, bool fsync_journal, bool write_journal, std::string* error_out = nullptr);

  /// Persists last committed seq and engine checkpoint pointer.
  bool save_checkpoint(std::string* error_out = nullptr);

  /// Replays journal entries with seq > last_ack.
  bool recover(std::string* error_out = nullptr);

  std::uint64_t next_seq() const { return next_seq_; }
  std::uint64_t last_ack_seq() const { return last_ack_; }
  /// Engine `Manifest::version()` captured at last successful `save_checkpoint` (0 if none).
  std::uint64_t last_engine_checkpoint_manifest() const { return last_engine_checkpoint_manifest_; }
  /// Engine `CheckpointState::checkpoint_seq` after last successful `save_checkpoint` (0 if unknown / old session file).
  std::uint64_t last_engine_checkpoint_seq() const { return last_engine_checkpoint_seq_; }

  /// Pinned read snapshot for MVCC-style visibility (`mdb$` keys use `StorageEngine` commit seq).
  /// Updated after `open`/`recover`, each successful `submit`, and via `refresh_read_snapshot()`.
  std::uint64_t read_snapshot_seq() const { return read_snapshot_seq_; }
  void refresh_read_snapshot();

 private:
  static bool split_journal_fields(const std::string& line, std::vector<std::string>* fields);
  bool append_journal_line(std::uint64_t seq, const CommandBatch& batch, bool fsync);
  bool parse_journal_for_recovery(std::string* error_out);

  facade::Engine& engine_;
  std::filesystem::path dir_;
  std::filesystem::path journal_path_;
  std::filesystem::path ckpt_path_;
  std::filesystem::path session_log_path_;
  std::uint64_t next_seq_{1};
  std::uint64_t last_ack_{0};
  std::uint64_t last_engine_checkpoint_manifest_{0};
  std::uint64_t last_engine_checkpoint_seq_{0};
  std::uint64_t read_snapshot_seq_{(std::numeric_limits<std::uint64_t>::max)()};
  bool opened_{false};
  std::mutex submit_mu_;
  std::mutex idem_mu_;
  std::unordered_set<std::string> idem_completed_;
  /// Append-only handle for `session.journal` (reopened lazily; closed on `close()`).
  infra::FileWriter journal_w_;
};

}  // namespace structdb::client
