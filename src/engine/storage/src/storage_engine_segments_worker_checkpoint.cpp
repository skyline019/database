#include "storage_engine_detail.hpp"
#include "structdb/storage/storage_engine.hpp"

#include "structdb/infra/file_handle.hpp"
#include "structdb/infra/long_task_progress.hpp"
#include "structdb/infra/thread_compaction_sched.hpp"
#include "structdb/infra/tracer.hpp"
#include "structdb/storage/storage_trace.hpp"
#include "structdb/storage/wal.hpp"

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <future>
#include <iomanip>
#include <mutex>
#include <shared_mutex>
#include <sstream>
#include <thread>
#include <string>

#if defined(_WIN32)
#  ifndef NOMINMAX
#    define NOMINMAX
#  endif
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif
#  include <windows.h>
#endif

namespace sed = structdb::storage::storage_engine_detail;

namespace structdb::storage {

void StorageEngine::report_active_long_task_(structdb::infra::LongTaskKind kind, structdb::infra::LongTaskStatus status,
                                             std::uint64_t units_done, std::uint64_t units_total,
                                             std::string_view detail) {
  if (auto* lt = active_long_task()) {
    lt->set_kind(kind);
    if (!detail.empty()) lt->set_detail(std::string(detail));
    lt->report(status, units_done, units_total);
  }
}

void StorageEngine::start_compaction_worker(std::size_t queue_max_depth) {
  if (compaction_worker_joinable_) return;
  compaction_queue_cap_ = queue_max_depth == 0 ? 64u : queue_max_depth;
  compaction_worker_stop_ = false;
  compaction_worker_thread_ = std::thread([this] { compaction_worker_loop_(); });
  compaction_worker_joinable_ = true;
}

void StorageEngine::stop_compaction_worker() {
  if (!compaction_worker_joinable_) return;
  {
    std::lock_guard<std::mutex> lk(compaction_worker_mu_);
    compaction_worker_stop_ = true;
  }
  compaction_worker_cv_.notify_all();
  compaction_worker_thread_.join();
  compaction_worker_joinable_ = false;
  compaction_worker_stop_ = false;
}

void StorageEngine::compaction_worker_loop_() {
#if defined(_WIN32)
  BOOL bg_mode = FALSE;
  if (compaction_worker_low_priority_thread_) {
    bg_mode = SetThreadPriority(GetCurrentThread(), THREAD_MODE_BACKGROUND_BEGIN);
  }
#endif
#if defined(__linux__)
  structdb::infra::apply_compaction_background_thread_scheduling(compaction_worker_low_priority_thread_, "structdb_cmpw");
#endif
  for (;;) {
    std::unique_lock<std::mutex> lk(compaction_worker_mu_);
    compaction_worker_cv_.wait(lk, [&] { return compaction_worker_stop_ || !compaction_tasks_.empty(); });
    while (!compaction_tasks_.empty()) {
      std::size_t best = 0;
      for (std::size_t i = 1; i < compaction_tasks_.size(); ++i) {
        const auto& a = compaction_tasks_[best];
        const auto& b = compaction_tasks_[i];
        if (b.drain_priority > a.drain_priority ||
            (b.drain_priority == a.drain_priority && b.enqueue_seq < a.enqueue_seq)) {
          best = i;
        }
      }
      CompactionWorkerJob job = std::move(compaction_tasks_[best]);
      if (best != compaction_tasks_.size() - 1) {
        compaction_tasks_[best] = std::move(compaction_tasks_.back());
      }
      compaction_tasks_.pop_back();
      const std::uint64_t completed_before =
          compaction_worker_tasks_completed_total_.load(std::memory_order_relaxed);
      const std::uint64_t submitted =
          compaction_worker_tasks_submitted_total_.load(std::memory_order_relaxed);
      const std::size_t queue_remaining = compaction_tasks_.size();
      lk.unlock();
      report_active_long_task_(infra::LongTaskKind::CompactionFlush, infra::LongTaskStatus::Running,
                               completed_before + 1, submitted > 0 ? submitted : 1,
                               "compaction_worker executing queue_remaining=" + std::to_string(queue_remaining));
      {
        infra::SpanGuard trace_wtask(trace::kCompactionWorkerTask, 0);
        std::string err;
        if (active_long_task() && active_long_task()->cancel_requested()) {
          err = "compact: cancelled";
          job.task(&err);
        } else {
          job.task(&err);
        }
      }
      compaction_worker_tasks_completed_total_.fetch_add(1, std::memory_order_relaxed);
      lk.lock();
    }
    if (compaction_worker_stop_) {
#if defined(_WIN32)
      if (bg_mode) (void)SetThreadPriority(GetCurrentThread(), THREAD_MODE_BACKGROUND_END);
#endif
      return;
    }
  }
}

bool StorageEngine::enqueue_drain_l0_compaction_and_wait(std::uint32_t max_rounds, std::string* error_out,
                                                           std::uint32_t wait_ms, int drain_priority) {
  if (!opened_) {
    if (error_out) *error_out = "not open";
    return false;
  }
  if (!compaction_worker_joinable_) return drain_pending_l0_compactions(max_rounds, error_out);
  std::packaged_task<bool(std::string*)> pt(
      [this, max_rounds](std::string* e) { return drain_pending_l0_compactions(max_rounds, e); });
  std::future<bool> fut = pt.get_future();
  {
    std::lock_guard<std::mutex> lk(compaction_worker_mu_);
    if (compaction_tasks_.size() >= compaction_queue_cap_) {
      if (error_out) *error_out = "compaction queue full";
      return false;
    }
    CompactionWorkerJob job;
    job.drain_priority = drain_priority;
    job.enqueue_seq = compaction_worker_enqueue_seq_.fetch_add(1, std::memory_order_relaxed);
    job.task = std::move(pt);
    compaction_tasks_.push_back(std::move(job));
    compaction_worker_tasks_submitted_total_.fetch_add(1, std::memory_order_relaxed);
    const std::uint64_t submitted =
        compaction_worker_tasks_submitted_total_.load(std::memory_order_relaxed);
    report_active_long_task_(infra::LongTaskKind::CompactionFlush, infra::LongTaskStatus::Running, 0, submitted,
                             "compaction_worker drain enqueued");
  }
  compaction_worker_cv_.notify_one();
  if (wait_ms == 0) {
    const bool ok = fut.get();
    if (!ok && error_out && error_out->empty()) *error_out = "compaction worker drain failed";
    return ok;
  }
  if (fut.wait_for(std::chrono::milliseconds(wait_ms)) == std::future_status::timeout) {
    if (error_out) *error_out = "compaction worker drain wait timeout";
    return false;
  }
  const bool ok = fut.get();
  if (!ok && error_out && error_out->empty()) *error_out = "compaction worker drain failed";
  return ok;
}

bool StorageEngine::checkpoint(std::string* error_out) {
  if (!opened_) {
    if (error_out) *error_out = "not open";
    return false;
  }
  std::lock_guard<std::shared_mutex> lk(mu_);
  {
    const std::size_t fdx = pool_->pin();
    if (fdx != static_cast<std::size_t>(-1)) {
      unsigned char* p = pool_->frame_data(fdx);
      if (p) p[0] = static_cast<unsigned char>(manifest_.version() & 0xFF);
      pool_->unpin(fdx);
    }
  }
  CheckpointState ck{};
  (void)ckpt_.read_latest(dir_, &ck, nullptr);
  ck.manifest_version = manifest_.version();
  checkpoint_undo_coordinator_.fill_checkpoint_undo_safe_prefix_unlocked(&ck);
  if (!ckpt_.write_rotating(dir_, ck, error_out)) {
    if (error_out && error_out->empty()) *error_out = "checkpoint write";
    return false;
  }
  std::string err;
  (void)persist_commit_seq_hw_(&err);
  checkpoint_success_total_.fetch_add(1, std::memory_order_relaxed);
  return true;
}

}  // namespace structdb::storage
