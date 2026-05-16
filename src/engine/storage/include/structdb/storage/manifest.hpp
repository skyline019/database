#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "structdb/infra/file_handle.hpp"

namespace structdb::storage {

/// One SST entry in MANIFEST (phase 12+: persisted level; legacy loads as L0).
struct ManifestSst {
  std::uint8_t level{0};
  std::string relative_path;
};

class Manifest {
 public:
  bool load(const std::filesystem::path& path);
  bool save(const std::filesystem::path& path) const;

  void set_version(std::uint64_t v) { version_ = v; }
  std::uint64_t version() const { return version_; }

  const std::vector<ManifestSst>& sst_entries() const { return sst_entries_; }

  /// Replace SST list (e.g. after compaction). Caller sets `version_` separately if needed.
  void set_sst_entries(std::vector<ManifestSst> entries) { sst_entries_ = std::move(entries); }

  /// Append a new L0 SST after the last L0 entry and before any L1 entries (phase 12 layout).
  void push_l0_sst(std::string relative_path);

  /// All relative paths in MANIFEST order (L0 block then L1 block). For tests / observability.
  std::vector<std::string> sst_files() const;

  /// Number of leading entries with `level == 0` (the L0 block length).
  std::size_t l0_prefix_length() const;

 private:
  std::uint64_t version_{0};
  std::vector<ManifestSst> sst_entries_;
};

}  // namespace structdb::storage
