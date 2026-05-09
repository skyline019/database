#pragma once

#include <newdb/row.h>

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

/// LSM-lite staging structures embedded in `ShellState::lsm` (extracted to narrow `shell_state.h` bulk).
struct LsmEntry {
    newdb::Row row;
    bool deleted{false};
    std::uint64_t seq{0};
};
struct LsmSegmentMeta {
    std::string path;
    std::uint32_t level{0};
    int min_key{0};
    int max_key{0};
    std::uint64_t entry_count{0};
    std::uint64_t max_seq{0};
};

struct LsmShellCache {
    std::unordered_map<int, newdb::Row> hot_index_recent;
    std::unordered_map<int, LsmEntry> lsm_memtable;
    std::unordered_map<int, LsmEntry> lsm_immutable;
    std::vector<LsmSegmentMeta> lsm_segments;
    std::uint64_t lsm_memtable_bytes{0};
    std::uint64_t lsm_seq{0};
    std::string lsm_table_name;
};
