#include "structdb/storage/storage_engine.hpp"

#include <filesystem>
#include <functional>
#include <mutex>
#include <shared_mutex>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "storage_engine_detail.hpp"

namespace sed = structdb::storage::storage_engine_detail;

namespace structdb::storage {

bool StorageEngine::decode_get_visible_(std::string_view raw_stored, std::uint64_t read_max_seq,
                                        std::string* logical_out) const {
  if (logical_out) logical_out->clear();
  if (raw_stored == versioned_kv::kTomb) return false;
  std::string body;
  std::uint64_t ignored = 0;
  if (!versioned_kv::unwrap_visible(raw_stored, read_max_seq, &body, &ignored)) return false;
  if (body == versioned_kv::kTomb) return false;
  if (logical_out) *logical_out = std::move(body);
  return true;
}

bool StorageEngine::mem_layers_get_raw_unlocked_(const std::string& key, std::string* raw_out) const {
  if (mem_mgr_.active().get_raw(key, raw_out)) return true;
  const auto snap = mem_mgr_.frozen_flush();
  if (snap && snap->get_raw(key, raw_out)) return true;
  return false;
}

bool StorageEngine::get(const std::string& key, std::string* value_out, std::uint64_t read_max_seq) const {
  if (!opened_) return false;
  std::shared_lock<std::shared_mutex> lk(mu_);
  std::string raw;
  if (mem_layers_get_raw_unlocked_(key, &raw)) {
    return decode_get_visible_(raw, read_max_seq, value_out);
  }
  std::vector<std::filesystem::path> paths;
  sed::manifest_sst_paths_lookup_order(manifest_, dir_, &paths);
  for (const auto& pth : paths) {
    if (!sed::sst_get_key(pth, key, &raw)) continue;
    return decode_get_visible_(raw, read_max_seq, value_out);
  }
  return false;
}

void StorageEngine::visit_prefix(std::string_view prefix,
                                 const std::function<bool(std::string_view, std::string_view)>& visitor,
                                 std::uint64_t read_max_seq) const {
  if (!opened_) return;
  std::shared_lock<std::shared_mutex> lk(mu_);
  std::unordered_set<std::string> mem_shadow;
  bool stopped = false;
  const IMemTable* snap = mem_mgr_.frozen_flush().get();
  const auto walk_mem = [&](const std::string& k, const std::string& raw) {
    if (stopped) return false;
    if (raw == versioned_kv::kTomb) {
      mem_shadow.insert(k);
      return true;
    }
    std::string logical;
    if (decode_get_visible_(raw, read_max_seq, &logical)) {
      mem_shadow.insert(k);
      if (!visitor(std::string_view(k), std::string_view(logical))) {
        stopped = true;
        return false;
      }
    }
    return true;
  };
  if (snap) {
    (void)mem_mgr_.active().for_each_sorted_prefix_overlay(prefix, *snap, walk_mem);
  } else {
    (void)mem_mgr_.active().for_each_sorted_prefix(prefix, walk_mem);
  }
  if (stopped) return;
  std::vector<std::filesystem::path> paths;
  sed::manifest_sst_paths_lookup_order(manifest_, dir_, &paths);
  for (const auto& pth : paths) {
    if (!sed::sst_visit_prefix(pth, prefix, [&](std::string_view k, std::string_view raw) {
          if (mem_shadow.count(std::string(k))) return true;
          std::string logical;
          if (!decode_get_visible_(raw, read_max_seq, &logical)) return true;
          return visitor(k, std::string_view(logical));
        }))
      return;
  }
}

}  // namespace structdb::storage
