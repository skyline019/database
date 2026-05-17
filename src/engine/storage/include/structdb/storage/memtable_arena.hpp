#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace structdb::storage {

/// Bump allocator for MemTable skip-list nodes (reset on table clear; not thread-safe).
class MemTableArena {
 public:
  void* allocate(std::size_t nbytes, std::size_t alignment);
  void reset();

 private:
  static constexpr std::size_t kDefaultBlockBytes = 256 * 1024;
  std::vector<std::unique_ptr<unsigned char[]>> blocks_;
  std::size_t block_index_{0};
  std::size_t offset_{0};
};

}  // namespace structdb::storage
