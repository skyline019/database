#pragma once

#include <cstddef>
#include <functional>
#include <map>
#include <utility>
#include <string>
#include <string_view>

#include "structdb/storage/imemtable.hpp"

namespace structdb::storage {

/// Default `IMemTable` backend: sorted `std::map` (correctness-first; hot path may be swapped later).
///
/// **Threading**: not synchronized. Production uses are under `StorageEngine`'s `mu_` (exclusive or shared lock)
/// or read-only views of a frozen table during flush; do not mutate concurrently.
class MemTable final : public IMemTable {
 public:
  void put(std::string key, std::string value) override;
  /// True if `key` is present (including tombstone); use with `get` to distinguish miss vs tomb.
  bool has_key(const std::string& key) const { return has_key(std::string_view(key)); }
  bool has_key(std::string_view key) const override;
  /// Returns stored bytes including tombstone; false if absent.
  bool get_raw(const std::string& key, std::string* stored_out) const { return get_raw(std::string_view(key), stored_out); }
  bool get_raw(std::string_view key, std::string* stored_out) const override;
  bool get(const std::string& key, std::string* value_out) const { return get(std::string_view(key), value_out); }
  bool get(std::string_view key, std::string* value_out) const override;
  std::size_t bytes_approx() const override;
  std::size_t size() const override;
  void clear() override;
  /// Hint `std::map` bucket count after bulk load (no semantic effect).
  void reserve_capacity(std::size_t approx_entries) override;
  void swap(MemTable& other) noexcept;
  void swap_with(IMemTable& other) noexcept override;

  /// Insert keys from `other` only when absent in `this` (caller uses this for flush rollback when `this` wins).
  void merge_missing_from(const IMemTable& donor) override;

  /// Serialize to writer (length-prefixed kv pairs).
  bool seal_dump(const std::function<bool(const void*, std::size_t)>& chunk_writer) const override;

  /// In-order visit (key order). Stops early if visitor returns false.
  bool for_each_sorted(const std::function<bool(const std::string&, const std::string&)>& visitor) const override;

  /// Visit sorted keys in `[prefix, +∞)` until the first key that does **not** start with `prefix`
  /// (lexicographic range scan). Intended for `visit_prefix` to avoid scanning unrelated keys.
  bool for_each_sorted_prefix(std::string_view prefix,
                              const std::function<bool(const std::string&, const std::string&)>& visitor) const override;

  /// Sorted walk on `[prefix, +∞)`; per key prefers `this` over `older` (same-key overlay). Same concrete type required.
  bool for_each_sorted_prefix_overlay(std::string_view prefix, const IMemTable& older,
                                      const std::function<bool(const std::string&, const std::string&)>& visitor) const override;

 private:
  std::map<std::string, std::string, std::less<>> map_;
  std::size_t bytes_total_{0};
};

}  // namespace structdb::storage
