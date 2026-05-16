#pragma once

#include <cstddef>
#include <functional>
#include <string>
#include <string_view>

namespace structdb::storage {

/// Abstract sorted in-memory KV table used by `StorageEngine`.
///
/// Default implementation is `MemTable` (`std::map` backend). Other backends (skip-list, sharded
/// tables, etc.) can implement this interface without changing engine read/overlay contracts that
/// go through `MemTableManager` + concrete `MemTable` helpers (e.g. prefix overlay).
class IMemTable {
 public:
  virtual ~IMemTable() = default;

  virtual void put(std::string key, std::string value) = 0;
  virtual bool has_key(std::string_view key) const = 0;
  virtual bool get_raw(std::string_view key, std::string* stored_out) const = 0;
  virtual bool get(std::string_view key, std::string* value_out) const = 0;
  virtual std::size_t bytes_approx() const = 0;
  virtual std::size_t size() const = 0;
  virtual void clear() = 0;
  virtual void reserve_capacity(std::size_t approx_entries) = 0;

  virtual bool seal_dump(const std::function<bool(const void*, std::size_t)>& chunk_writer) const = 0;
  virtual bool for_each_sorted(const std::function<bool(const std::string&, const std::string&)>& visitor) const = 0;
  virtual bool for_each_sorted_prefix(std::string_view prefix,
                                      const std::function<bool(const std::string&, const std::string&)>& visitor) const = 0;

  /// Exchange contents with `other` **iff** same concrete type (otherwise `std::terminate` in debug builds).
  virtual void swap_with(IMemTable& other) noexcept = 0;
  /// Insert keys from `donor` when absent in `this` (sorted walk on donor).
  virtual void merge_missing_from(const IMemTable& donor) = 0;
  /// Merge-walk sorted keys in `[prefix, +∞)`; on duplicate keys **this** wins. `older` must be the same backend as `*this`.
  virtual bool for_each_sorted_prefix_overlay(
      std::string_view prefix, const IMemTable& older,
      const std::function<bool(const std::string&, const std::string&)>& visitor) const = 0;
};

}  // namespace structdb::storage
