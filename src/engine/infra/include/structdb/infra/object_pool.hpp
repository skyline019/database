#pragma once

#include <cstddef>
#include <memory>
#include <mutex>
#include <vector>

namespace structdb::infra {

/// Fixed-capacity pool of pre-constructed Ts; thread-safe acquire/release.
template <typename T>
class ObjectPool {
 public:
  explicit ObjectPool(std::size_t capacity) {
    storage_.reserve(capacity);
    free_.reserve(capacity);
    for (std::size_t i = 0; i < capacity; ++i) {
      auto up = std::make_unique<T>();
      T* raw = up.get();
      storage_.push_back(std::move(up));
      free_.push_back(raw);
    }
  }

  std::size_t capacity() const { return storage_.size(); }

  T* acquire() {
    std::lock_guard<std::mutex> lock(mu_);
    if (free_.empty()) return nullptr;
    T* p = free_.back();
    free_.pop_back();
    return p;
  }

  void release(T* p) {
    if (!p) return;
    std::lock_guard<std::mutex> lock(mu_);
    free_.push_back(p);
  }

  std::size_t outstanding() const {
    std::lock_guard<std::mutex> lock(mu_);
    return storage_.size() - free_.size();
  }

 private:
  std::vector<std::unique_ptr<T>> storage_;
  std::vector<T*> free_;
  mutable std::mutex mu_;
};

}  // namespace structdb::infra
