#pragma once

#include <condition_variable>
#include <deque>
#include <mutex>
#include <optional>
#include <utility>

namespace structdb::infra {

/// Narrow queue interface for scheduler/worker handoff. Backed by mutex + deque; swap in
/// `boost::lockfree::spsc_queue` later without changing call sites.
template <typename T>
class TaskQueue {
 public:
  void push(T v) {
    std::lock_guard<std::mutex> lock(mu_);
    q_.push_back(std::move(v));
    cv_.notify_one();
  }

  std::optional<T> try_pop() {
    std::lock_guard<std::mutex> lock(mu_);
    if (q_.empty()) return std::nullopt;
    T out = std::move(q_.front());
    q_.pop_front();
    return out;
  }

  /// Blocks until an item is available or `close()` was called. Returns false if closed and empty.
  bool wait_pop(T& out) {
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait(lock, [this] { return closed_ || !q_.empty(); });
    if (q_.empty()) return false;
    out = std::move(q_.front());
    q_.pop_front();
    return true;
  }

  void close() {
    std::lock_guard<std::mutex> lock(mu_);
    closed_ = true;
    cv_.notify_all();
  }

 private:
  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<T> q_;
  bool closed_{false};
};

}  // namespace structdb::infra
