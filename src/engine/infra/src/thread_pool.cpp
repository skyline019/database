#include "structdb/infra/thread_pool.hpp"

namespace structdb::infra {

ThreadPool::ThreadPool(std::size_t threads) {
  workers_.reserve(threads);
  for (std::size_t i = 0; i < threads; ++i) {
    workers_.emplace_back([this] { worker_loop(); });
  }
}

ThreadPool::~ThreadPool() {
  {
    std::lock_guard<std::mutex> lock(mu_);
    stop_ = true;
  }
  cv_.notify_all();
  for (auto& t : workers_) {
    if (t.joinable()) t.join();
  }
}

void ThreadPool::submit(std::function<void()> job) {
  {
    std::lock_guard<std::mutex> lock(mu_);
    jobs_.push(std::move(job));
  }
  cv_.notify_one();
}

void ThreadPool::wait_idle() {
  std::unique_lock<std::mutex> lock(mu_);
  cv_.wait(lock, [this] { return jobs_.empty() && active_ == 0; });
}

void ThreadPool::worker_loop() {
  for (;;) {
    std::function<void()> job;
    {
      std::unique_lock<std::mutex> lock(mu_);
      cv_.wait(lock, [this] { return stop_ || !jobs_.empty(); });
      if (stop_ && jobs_.empty()) return;
      if (jobs_.empty()) continue;
      job = std::move(jobs_.front());
      jobs_.pop();
      ++active_;
    }
    if (job) job();
    {
      std::lock_guard<std::mutex> lock(mu_);
      --active_;
    }
    cv_.notify_all();
  }
}

}  // namespace structdb::infra
