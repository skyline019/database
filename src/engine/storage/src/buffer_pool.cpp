#include "structdb/storage/buffer_pool.hpp"

namespace structdb::storage {

BufferPool::BufferPool(std::size_t frame_count) {
  frames_.resize(frame_count);
  for (auto& f : frames_) {
    f.bytes = std::make_unique<unsigned char[]>(wf::storage::kPageSize);
    f.in_use = false;
  }
}

std::size_t BufferPool::pin() {
  std::lock_guard<std::mutex> lock(mu_);
  for (std::size_t i = 0; i < frames_.size(); ++i) {
    if (!frames_[i].in_use) {
      frames_[i].in_use = true;
      return i;
    }
  }
  return static_cast<std::size_t>(-1);
}

void BufferPool::unpin(std::size_t frame_index) {
  std::lock_guard<std::mutex> lock(mu_);
  if (frame_index >= frames_.size()) return;
  frames_[frame_index].in_use = false;
}

unsigned char* BufferPool::frame_data(std::size_t frame_index) {
  if (frame_index >= frames_.size()) return nullptr;
  return frames_[frame_index].bytes.get();
}

std::size_t BufferPool::pinned() const {
  std::lock_guard<std::mutex> lock(mu_);
  std::size_t n = 0;
  for (const auto& f : frames_) {
    if (f.in_use) ++n;
  }
  return n;
}

}  // namespace structdb::storage
