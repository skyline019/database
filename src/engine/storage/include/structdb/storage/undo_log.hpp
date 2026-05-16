#pragma once

#include <filesystem>
#include <string_view>

#include "structdb/infra/file_handle.hpp"

namespace structdb::storage {

class UndoLog {
 public:
  bool open(const std::filesystem::path& dir);
  void close();
  bool append(const void* data, std::size_t len, bool fsync);
  /// Flush OS buffers so `file_size` reflects appended bytes (used before undo segment roll).
  bool sync();
  /// Binary frame: magic `STRDBUV1` + u32le klen + key + u32le vlen + previous stored bytes (for versioned keys).
  bool append_versioned_prev_snapshot(std::string_view key, std::string_view prev_raw_stored, bool fsync);

  /// Truncate `undo.log` to zero length and reopen for append. Caller must ensure higher-level safety (see `Docs/UNDO_LOG_4C.md`).
  bool truncate_to_empty(std::string* error_out = nullptr);

  /// Drop the first `prefix_bytes` of `undo.log` and reopen for append (rewrite tail to a new file).
  /// `StorageEngine` must adjust any persisted frame offsets after this returns true.
  bool truncate_prefix(std::uint64_t prefix_bytes, std::string* error_out = nullptr);

  /// Phase 22C: same rewrite semantics as `truncate_prefix` on an arbitrary file (caller manages active writer).
  static bool truncate_prefix_at_path(const std::filesystem::path& path, std::uint64_t prefix_bytes,
                                        std::string* error_out = nullptr);

 private:
  infra::FileWriter w_;
  std::filesystem::path path_;
};

}  // namespace structdb::storage
