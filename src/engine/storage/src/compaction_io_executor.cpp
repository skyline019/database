#include "structdb/storage/compaction_io_executor.hpp"

#include "structdb/infra/thread_compaction_sched.hpp"

#include <cstdio>

namespace structdb::storage {

CompactionIoExecutor::CompactionIoExecutor() = default;

CompactionIoExecutor::~CompactionIoExecutor() { stop(); }

void CompactionIoExecutor::start(std::size_t num_workers, bool low_priority_thread) {
  std::lock_guard<std::mutex> lk(mu_);
  if (joinable_) return;
  if (num_workers == 0) num_workers = 1;
  if (num_workers > 32) num_workers = 32;
  low_priority_thread_ = low_priority_thread;
  stop_ = false;
  worker_count_ = num_workers;
  workers_.reserve(num_workers);
  for (std::size_t i = 0; i < num_workers; ++i) {
    workers_.emplace_back([this, i] { worker_loop_(i); });
  }
  joinable_ = true;
}

void CompactionIoExecutor::stop() {
  std::vector<std::thread> join_workers;
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (!joinable_) return;
    stop_ = true;
    join_workers = std::move(workers_);
    workers_.clear();
    joinable_ = false;
    worker_count_ = 0;
  }
  cv_.notify_all();
  for (auto& th : join_workers) {
    if (th.joinable()) th.join();
  }
  {
    std::lock_guard<std::mutex> lk(mu_);
    stop_ = false;
    tasks_.clear();
  }
}

bool CompactionIoExecutor::run_sync(std::function<bool()> fn) {
  std::packaged_task<bool()> task(std::move(fn));
  std::future<bool> fut = task.get_future();
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (!joinable_ || stop_) return false;
    tasks_.push_back(std::move(task));
  }
  cv_.notify_one();
  return fut.get();
}

void CompactionIoExecutor::worker_loop_(std::size_t worker_index) {
  char tname[16];
  (void)std::snprintf(tname, sizeof tname, "structdb_cio%zu", worker_index);
  structdb::infra::apply_compaction_background_thread_scheduling(low_priority_thread_, tname);
  for (;;) {
    std::unique_lock<std::mutex> lk(mu_);
    cv_.wait(lk, [&] { return !tasks_.empty() || stop_; });
    while (!tasks_.empty()) {
      std::packaged_task<bool()> job = std::move(tasks_.front());
      tasks_.pop_front();
      lk.unlock();
      job();
      lk.lock();
    }
    if (stop_) return;
  }
}

}  // namespace structdb::storage
