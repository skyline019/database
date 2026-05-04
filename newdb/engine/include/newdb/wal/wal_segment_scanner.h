#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace newdb {

struct WalSegmentFileInfo {
    std::string path;
    std::uint64_t size_bytes{0};
};

/// Lists on-disk WAL segment files under `wal_dir` (OPTIMIZATION_PLAN phase 7 building block).
/// Paths are sorted lexicographically by `std::filesystem::path` for stable cross-platform ordering.
std::vector<std::string> list_wal_segment_paths(const std::string& wal_dir);

/// Same inventory as `list_wal_segment_paths` plus best-effort file sizes (0 if unreadable).
std::vector<WalSegmentFileInfo> list_wal_segment_inventory(const std::string& wal_dir);

}  // namespace newdb
