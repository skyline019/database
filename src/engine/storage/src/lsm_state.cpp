#include "structdb/storage/lsm_state.hpp"

#include "structdb/storage/manifest.hpp"

namespace structdb::storage {

void LsmState::sync_from_manifest(const Manifest& m) {
  last_manifest_version = m.version();
  l0_sst_relative_paths.clear();
  for (const auto& e : m.sst_entries()) {
    if (e.level != 0) break;
    l0_sst_relative_paths.push_back(e.relative_path);
  }
}

}  // namespace structdb::storage
