#pragma once

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <future>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

namespace structdb::storage {

/// Thread-pool for compaction **materialize** disk work (merge SST read/write loops) so foreground / WAL paths
/// do not execute large I/O directly when enabled via `EngineConfigSnapshot::compaction_dedicated_io_executor`.
/// WAL is never opened on this pool — only SST / temp files under `data_dir` (see `WalWriter` vs merge materialize).
class CompactionIoExecutor {
 public:
  CompactionIoExecutor();
  ~CompactionIoExecutor();

  CompactionIoExecutor(const CompactionIoExecutor&) = delete;
  CompactionIoExecutor& operator=(const CompactionIoExecutor&) = delete;

  /// @param num_workers Must be >= 1 when starting. Typical values 2–8.
  /// @param low_priority_thread When true, apply OS-specific background scheduling on each worker (Linux: nice + SCHED_IDLE).
  void start(std::size_t num_workers, bool low_priority_thread);
  void stop();
  bool started() const { return joinable_; }
  std::size_t worker_count() const { return worker_count_; }

  /// Runs `fn` on a worker thread; blocks until completion. Returns false if executor is stopped.
  bool run_sync(std::function<bool()> fn);

 private:
  void worker_loop_(std::size_t worker_index);

  mutable std::mutex mu_;
  std::condition_variable cv_;
  std::deque<std::packaged_task<bool()>> tasks_;
  bool stop_{false};
  bool low_priority_thread_{true};
  std::vector<std::thread> workers_;
  bool joinable_{false};
  std::size_t worker_count_{0};
};

}  // namespace structdb::storage
