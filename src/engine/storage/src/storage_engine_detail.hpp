#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <map>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "structdb/storage/manifest.hpp"

namespace structdb::storage::storage_engine_detail {

void append_u32_le(std::string* s, std::uint32_t v);
bool read_u32_le(const std::uint8_t*& p, const std::uint8_t* end, std::uint32_t* out);
bool sst_get_key(const std::filesystem::path& path, const std::string& key, std::string* value_out);
bool sst_visit_prefix(const std::filesystem::path& path, std::string_view prefix,
                      const std::function<bool(std::string_view, std::string_view)>& visitor);
bool sst_load_all_entries(const std::filesystem::path& path, std::vector<std::pair<std::string, std::string>>* out,
                          std::string* error_out, bool sequential_scan_hint = false, std::size_t read_chunk_bytes = 0,
                          const std::function<void(std::size_t)>& on_read_progress = {});
bool write_sst_sorted_entries(const std::filesystem::path& path, const std::map<std::string, std::string>& sorted,
                              std::string* error_out, std::size_t write_chunk_bytes = 0,
                              const std::function<void(std::size_t)>& on_write_progress = {});
/// Writes one SST from a sorted source; `for_each_sorted` must invoke `visitor(k,v)` in non-decreasing `k` order.
bool write_sst_sorted_entries_from_for_each(
    const std::filesystem::path& path,
    const std::function<bool(const std::function<bool(const std::string&, const std::string&)>& visitor)>& for_each_sorted,
    std::string* error_out, std::size_t write_chunk_bytes = 0,
    const std::function<void(std::size_t)>& on_write_progress = {});
std::uint64_t file_size_u64_or_zero(const std::filesystem::path& p);
bool persist_wal_segments_metadata_v2(const std::filesystem::path& data_dir, std::uint64_t next_roll_seq,
                                        const std::vector<std::string>& sealed_relative, std::string* error_out);
bool persist_undo_segments_metadata_v2(const std::filesystem::path& data_dir, std::uint64_t next_roll_seq,
                                       const std::vector<std::string>& sealed_relative, std::string* error_out);
bool wal_segment_rel_path_safe(const std::string& rel);
bool undo_segment_rel_path_safe(const std::string& rel);
bool persist_wal_segments_metadata_v1(const std::filesystem::path& data_dir, std::uint32_t segment_count,
                                        std::string* error_out);
std::uint32_t read_wal_segments_metadata_or_default(const std::filesystem::path& data_dir);
void manifest_sst_paths_lookup_order(const Manifest& m, const std::filesystem::path& dir,
                                     std::vector<std::filesystem::path>* out);

}  // namespace structdb::storage::storage_engine_detail
