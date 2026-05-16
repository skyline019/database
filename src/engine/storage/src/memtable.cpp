#include "structdb/storage/memtable.hpp"
#include "structdb/storage/versioned_kv.hpp"

#include <cstdlib>

namespace structdb::storage {

void MemTable::put(std::string key, std::string value) {
  const auto it = map_.find(key);
  if (it != map_.end()) {
    bytes_total_ -= it->first.size() + it->second.size();
    it->second = std::move(value);
    bytes_total_ += it->first.size() + it->second.size();
    return;
  }
  bytes_total_ += key.size() + value.size();
  map_.emplace(std::move(key), std::move(value));
}

bool MemTable::has_key(std::string_view key) const { return map_.find(key) != map_.end(); }

bool MemTable::get_raw(std::string_view key, std::string* stored_out) const {
  auto it = map_.find(key);
  if (it == map_.end()) return false;
  if (stored_out) *stored_out = it->second;
  return true;
}

bool MemTable::get(std::string_view key, std::string* value_out) const {
  auto it = map_.find(key);
  if (it == map_.end()) return false;
  if (it->second == versioned_kv::kTomb) return false;
  if (value_out) *value_out = it->second;
  return true;
}

std::size_t MemTable::bytes_approx() const { return bytes_total_; }

std::size_t MemTable::size() const { return map_.size(); }

void MemTable::clear() {
  map_.clear();
  bytes_total_ = 0;
}

void MemTable::reserve_capacity(std::size_t approx_entries) {
  (void)approx_entries;
#if defined(__cpp_lib_map_reserve) && __cpp_lib_map_reserve >= 202110L
  map_.reserve(approx_entries);
#endif
}

void MemTable::swap(MemTable& other) noexcept {
  map_.swap(other.map_);
  std::swap(bytes_total_, other.bytes_total_);
}

void MemTable::swap_with(IMemTable& other) noexcept {
  auto* o = dynamic_cast<MemTable*>(&other);
  if (!o) {
#if !defined(NDEBUG)
    std::terminate();
#else
    return;
#endif
  }
  swap(*o);
}

void MemTable::merge_missing_from(const IMemTable& donor) {
  (void)donor.for_each_sorted([this](const std::string& k, const std::string& v) {
    if (!has_key(k)) put(k, v);
    return true;
  });
}

bool MemTable::seal_dump(const std::function<bool(const void*, std::size_t)>& chunk_writer) const {
  for (const auto& kv : map_) {
    const std::uint32_t klen = static_cast<std::uint32_t>(kv.first.size());
    const std::uint32_t vlen = static_cast<std::uint32_t>(kv.second.size());
    if (!chunk_writer(&klen, sizeof(klen))) return false;
    if (!chunk_writer(kv.first.data(), kv.first.size())) return false;
    if (!chunk_writer(&vlen, sizeof(vlen))) return false;
    if (!chunk_writer(kv.second.data(), kv.second.size())) return false;
  }
  return true;
}

bool MemTable::for_each_sorted(const std::function<bool(const std::string&, const std::string&)>& visitor) const {
  for (const auto& kv : map_) {
    if (!visitor(kv.first, kv.second)) return false;
  }
  return true;
}

bool MemTable::for_each_sorted_prefix(std::string_view prefix,
                                      const std::function<bool(const std::string&, const std::string&)>& visitor) const {
  if (prefix.empty()) return for_each_sorted(visitor);
  const std::string pfx{prefix};
  for (auto it = map_.lower_bound(pfx); it != map_.end(); ++it) {
    const std::string& k = it->first;
    if (k.size() < pfx.size() || k.compare(0, pfx.size(), pfx) != 0) break;
    if (!visitor(k, it->second)) return false;
  }
  return true;
}

bool MemTable::for_each_sorted_prefix_overlay(std::string_view prefix, const IMemTable& older,
                                              const std::function<bool(const std::string&, const std::string&)>& visitor) const {
  const auto* o = dynamic_cast<const MemTable*>(&older);
  if (!o) return false;
  const std::string pfx = prefix.empty() ? std::string{} : std::string{prefix};
  const auto in_prefix = [&pfx](const std::string& k) {
    if (pfx.empty()) return true;
    return k.size() >= pfx.size() && k.compare(0, pfx.size(), pfx) == 0;
  };
  auto it_a = pfx.empty() ? map_.begin() : map_.lower_bound(pfx);
  auto it_b = pfx.empty() ? o->map_.begin() : o->map_.lower_bound(pfx);
  while (it_a != map_.end() && in_prefix(it_a->first) && it_b != o->map_.end() && in_prefix(it_b->first)) {
    if (it_a->first < it_b->first) {
      if (!visitor(it_a->first, it_a->second)) return false;
      ++it_a;
    } else if (it_b->first < it_a->first) {
      if (!visitor(it_b->first, it_b->second)) return false;
      ++it_b;
    } else {
      if (!visitor(it_a->first, it_a->second)) return false;
      ++it_a;
      ++it_b;
    }
  }
  while (it_a != map_.end() && in_prefix(it_a->first)) {
    if (!visitor(it_a->first, it_a->second)) return false;
    ++it_a;
  }
  while (it_b != o->map_.end() && in_prefix(it_b->first)) {
    if (!visitor(it_b->first, it_b->second)) return false;
    ++it_b;
  }
  return true;
}

}  // namespace structdb::storage
