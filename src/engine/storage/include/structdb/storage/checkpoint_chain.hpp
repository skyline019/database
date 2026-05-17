#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "structdb/storage/checkpoint.hpp"

namespace structdb::storage {

/// One record appended to `checkpoint.chain` after each successful `write_rotating`.
struct CheckpointChainEntry {
  std::uint64_t checkpoint_seq{0};
  std::uint64_t wal_offset{0};
  std::uint64_t redo_offset{0};
  std::uint64_t manifest_version{0};
  std::uint64_t mdb_catalog_epoch{0};
  std::uint64_t undo_log_safe_prefix_bytes{0};
  std::uint64_t written_unix_ns{0};
};

/// Append entry (idempotent skip if last line already has same seq).
bool checkpoint_chain_append(const std::filesystem::path& data_dir, const CheckpointState& st,
                             std::uint64_t written_unix_ns, std::string* error_out = nullptr);

/// Read all entries in file order (oldest first).
bool checkpoint_chain_read_all(const std::filesystem::path& data_dir, std::vector<CheckpointChainEntry>* out,
                               std::string* error_out = nullptr);

/// Find entry with exact `checkpoint_seq`; false if missing.
bool checkpoint_chain_find(const std::filesystem::path& data_dir, std::uint64_t checkpoint_seq,
                           CheckpointChainEntry* out, std::string* error_out = nullptr);

/// Destructive: truncate `wal.log` to `wal_offset`, write checkpoint slots to `target`, trim chain tail after target.
/// Engine must be **closed**. See `Docs/phases/PHASE43.md`.
bool recover_data_dir_to_checkpoint_seq(const std::filesystem::path& data_dir, std::uint64_t target_checkpoint_seq,
                                        std::string* error_out = nullptr);

/// Validate `checkpoint.chain` monotonicity and consistency with `read_latest` / `wal.log` size.
/// When `strict` is false, mismatches against latest checkpoint are warnings only (returns true).
bool checkpoint_chain_validate(const std::filesystem::path& data_dir, bool strict, std::string* error_out = nullptr);

}  // namespace structdb::storage
