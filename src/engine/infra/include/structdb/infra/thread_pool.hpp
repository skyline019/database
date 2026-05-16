#pragma once

#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace structdb::infra {

/// Minimal thread pool for blocking I/O and background work (Windows-first).
class ThreadPool {
 public:
  explicit ThreadPool(std::size_t threads);
  ~ThreadPool();

  void submit(std::function<void()> job);
  void wait_idle();

  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;

 private:
  void worker_loop();

  std::vector<std::thread> workers_;
  std::queue<std::function<void()>> jobs_;
  std::mutex mu_;
  std::condition_variable cv_;
  bool stop_{false};
  std::size_t active_{0};
};

}  // namespace structdb::infra
