#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace structdb::storage {

/// Immutable inputs for an L0 pair merge (two oldest L0 SSTs → one output SST).
struct CompactionL0MergeSnapshot {
  std::uint64_t base_manifest_version{0};
  std::string s0_rel;
  std::string s1_rel;
  bool l1_output{false};
  std::string out_basename;
};

/// Immutable inputs for tiered pair merges (L1→L2, L2→L3, L3→L4).
struct CompactionTieredPairSnapshot {
  std::uint64_t base_manifest_version{0};
  std::size_t first_idx{0};
  std::string s0_rel;
  std::string s1_rel;
  std::uint32_t src_level{0};
  std::uint32_t dst_level{0};
  std::string out_basename;
};

}  // namespace structdb::storage
