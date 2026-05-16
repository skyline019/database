#pragma once

#include <cstddef>
#include <memory>
#include <mutex>
#include <vector>

#include <waterfall/storage/page.h>

namespace structdb::storage {

/// Fixed-size page frames (uses wf::storage::kPageSize).
class BufferPool {
 public:
  explicit BufferPool(std::size_t frame_count);

  /// Returns pinned frame index or (size_t)-1 on failure.
  std::size_t pin();
  void unpin(std::size_t frame_index);

  unsigned char* frame_data(std::size_t frame_index);

  std::size_t capacity() const { return frames_.size(); }
  std::size_t pinned() const;

 private:
  struct Frame {
    std::unique_ptr<unsigned char[]> bytes;
    bool in_use{false};
  };
  std::vector<Frame> frames_;
  mutable std::mutex mu_;
};

}  // namespace structdb::storage
