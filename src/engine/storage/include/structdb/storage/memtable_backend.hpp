#pragma once

#include <cstdint>

namespace structdb::storage {

/// Selects the in-memory sorted table implementation for `StorageEngine` / `MemTableManager`.
enum class MemTableBackend : std::uint8_t {
  Map = 0,
  SkipList = 1,
};

}  // namespace structdb::storage
