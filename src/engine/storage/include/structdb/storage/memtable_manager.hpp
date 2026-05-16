#pragma once

#include <memory>
#include <string>

#include "structdb/storage/imemtable.hpp"
#include "structdb/storage/memtable_backend.hpp"

namespace structdb::storage {

/// Factory for `MemTable` / `MemTableSkipList` backends (see `MemTableBackend`).
std::unique_ptr<IMemTable> make_memtable(MemTableBackend b);

/// Active mutable memtable plus optional frozen snapshot during `flush_memtable` materialization.
class MemTableManager {
 public:
  explicit MemTableManager(MemTableBackend b = MemTableBackend::SkipList);

  IMemTable& active() noexcept { return *active_; }
  const IMemTable& active() const noexcept { return *active_; }

  const std::shared_ptr<IMemTable>& frozen_flush() const noexcept { return frozen_flush_; }
  MemTableBackend backend() const noexcept { return backend_; }

  /// Replace active table with a fresh empty instance of `b` and drop any frozen snapshot (call before WAL replay).
  void reset_to_backend(MemTableBackend b);

  /// Moves all keys from `active_` into a new `frozen_flush_` shared table; leaves `active_` empty.
  /// Returns false if a frozen snapshot already exists (`flush_memtable already in progress`).
  bool begin_flush_move_active_to_frozen(std::string* error_out);

  void merge_frozen_into_active_and_clear();
  void clear_frozen_flush_only();
  /// Used after WAL replay during `open`: drop any stale frozen handle without merging (defensive).
  void discard_frozen_snapshot();

 private:
  MemTableBackend backend_{MemTableBackend::SkipList};
  std::unique_ptr<IMemTable> active_;
  std::shared_ptr<IMemTable> frozen_flush_;
};

}  // namespace structdb::storage
