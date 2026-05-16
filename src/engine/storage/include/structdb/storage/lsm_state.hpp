#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace structdb::storage {

class Manifest;

/// LSM-side view: L0 SST list + last sealed generation (MVP; compaction / L1+ later).
struct LsmState {
  std::uint64_t last_manifest_version{0};
  std::vector<std::string> l0_sst_relative_paths;

  void sync_from_manifest(const Manifest& m);
};

}  // namespace structdb::storage
