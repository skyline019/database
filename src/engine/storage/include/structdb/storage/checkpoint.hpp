#pragma once

#include <cstdint>
#include <filesystem>
#include <string>

#include "structdb/infra/file_handle.hpp"

namespace structdb::storage {

struct CheckpointState {
  std::uint64_t wal_offset{0};
  std::uint64_t redo_offset{0};
  std::uint64_t manifest_version{0};
  /// Logical catalog / `.mdb` schema generation; 0 if omitted in older checkpoints.
  std::uint64_t mdb_catalog_epoch{0};
  /// Monotonic generation for dual-slot / chain records (0 if read from legacy text only).
  std::uint64_t checkpoint_seq{0};
  /// Phase 10 / 9C: byte length of `undo.log` prefix provably safe to recycle (see `Docs/CHECKPOINT_UNDO_PREFIX.md`,
  /// `Docs/PHASE10.md`). Persisted in binary checkpoint **v2** slots; 0 when read from legacy text or v1-only slots.
  std::uint64_t undo_log_safe_prefix_bytes{0};
};

class CheckpointWriter {
 public:
  /// Legacy single-file text `checkpoint` (backward compatible).
  bool write(const std::filesystem::path& dir, const CheckpointState& st);
  bool read(const std::filesystem::path& dir, CheckpointState* out);

  /// Phase 5: read best available state — binary `checkpoint.a` / `checkpoint.b` + `checkpoint.active`,
  /// else legacy `checkpoint` text. Returns false only on malformed data when some checkpoint file exists.
  bool read_latest(const std::filesystem::path& dir, CheckpointState* out, std::string* error_out = nullptr);

  /// Phase 5: write non-active slot + fsync, then `checkpoint.active` + fsync, then legacy `checkpoint` line
  /// (dual-write). Increments `checkpoint_seq` relative to `read_latest` before persist.
  bool write_rotating(const std::filesystem::path& dir, const CheckpointState& st, std::string* error_out = nullptr);

  /// Phase 43: write both slots with **exact** `st.checkpoint_seq` (no increment). Appends `checkpoint.chain`.
  bool write_recovery_checkpoint(const std::filesystem::path& dir, const CheckpointState& st,
                                 std::uint64_t written_unix_ns, std::string* error_out = nullptr);
};

}  // namespace structdb::storage
