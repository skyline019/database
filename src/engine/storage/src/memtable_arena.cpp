#include "structdb/storage/memtable_arena.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <new>

namespace structdb::storage {

void* MemTableArena::allocate(std::size_t nbytes, std::size_t alignment) {
  if (nbytes == 0) nbytes = 1;
  if (alignment == 0 || (alignment & (alignment - 1)) != 0) alignment = alignof(std::max_align_t);

  auto ensure_block = [&]() {
    if (blocks_.empty()) {
      blocks_.push_back(std::make_unique<unsigned char[]>(kDefaultBlockBytes));
      block_index_ = 0;
      offset_ = 0;
    }
  };
  ensure_block();

  for (;;) {
    unsigned char* base = blocks_[block_index_].get();
    std::uintptr_t addr = reinterpret_cast<std::uintptr_t>(base + offset_);
    const std::uintptr_t mis = addr % alignment;
    const std::size_t pad = mis == 0 ? 0 : alignment - mis;
    const std::size_t need = pad + nbytes;
    if (offset_ + need <= kDefaultBlockBytes) {
      void* out = base + offset_ + pad;
      offset_ += need;
      return out;
    }
    blocks_.push_back(std::make_unique<unsigned char[]>(kDefaultBlockBytes));
    block_index_ = blocks_.size() - 1;
    offset_ = 0;
  }
}

void MemTableArena::reset() {
  blocks_.clear();
  block_index_ = 0;
  offset_ = 0;
}

}  // namespace structdb::storage
