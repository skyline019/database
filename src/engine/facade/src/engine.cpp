#include "structdb/facade/engine.hpp"

#include <filesystem>
#include <future>
#include <memory>
#include <mutex>
#include <algorithm>
#include <thread>
#include <string_view>
#include <utility>

#include "structdb/infra/logging.hpp"
#include "structdb/infra/tracer.hpp"
#include "structdb/orchestrator/orchestrator.hpp"
#include "structdb/planner/execution_plan.hpp"
#include "structdb/runtime/graph_executor.hpp"
#include "structdb/runtime/operator.hpp"
#include "structdb/scheduler/budget.hpp"
#include "structdb/scheduler/scheduler.hpp"
#include "structdb/storage/storage_engine.hpp"
#include "structdb/storage/versioned_kv.hpp"

namespace structdb::facade {

Engine::WalMutationBudget::WalMutationBudget(Engine* e, std::uint64_t approx_wal_frame_bytes) : eng(e) {
  if (!eng) return;
  const auto snap = eng->config().snapshot();
  slots = eng->wal_scheduler_slots_for_frame_bytes_(approx_wal_frame_bytes, snap);
  if (slots) eng->wal_scheduler_acquire_blocking_(slots);
}

Engine::WalMutationBudget::~WalMutationBudget() {
  if (eng && slots) eng->wal_scheduler_release_slots_(slots);
}

std::uint32_t Engine::wal_scheduler_slots_for_frame_bytes_(std::uint64_t approx_frame_bytes,
                                                           const EngineConfigSnapshot& snap) const {
  if (snap.wal_scheduler_bytes_per_depth_slot == 0) return 0;
  constexpr std::uint64_t k_max_frame_cap = static_cast<std::uint64_t>(256) * 1024 * 1024;
  const std::uint64_t cap_est = (std::min)(approx_frame_bytes, k_max_frame_cap);
  std::uint64_t slots64 =
      (cap_est + snap.wal_scheduler_bytes_per_depth_slot - 1) / snap.wal_scheduler_bytes_per_depth_slot;
  if (slots64 == 0) slots64 = 1;
  if (snap.wal_scheduler_max_slots_per_op > 0 &&
      slots64 > static_cast<std::uint64_t>(snap.wal_scheduler_max_slots_per_op)) {
    slots64 = snap.wal_scheduler_max_slots_per_op;
  }
  if (slots64 > 1000000ull) slots64 = 1000000ull;
  return static_cast<std::uint32_t>(slots64);
}

void Engine::wal_scheduler_acquire_blocking_(std::uint32_t slots) {
  if (slots == 0 || !orch_) return;
  for (;;) {
    std::string why;
    if (orch_->scheduler().budget().try_acquire(scheduler::ResourceType::WalQueueDepth, static_cast<std::int64_t>(slots),
                                                &why)) {
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}

void Engine::wal_scheduler_release_slots_(std::uint32_t slots) {
  if (slots == 0 || !orch_) return;
  orch_->scheduler().budget().release(scheduler::ResourceType::WalQueueDepth, static_cast<std::int64_t>(slots));
}

namespace {

bool key_is_mdb_catalog(std::string_view k) {
  return k.size() >= 4 && k.compare(0, 4, "mdb$", 4) == 0;
}

class NoopOperator final : public runtime::IOperator {
 public:
  explicit NoopOperator(std::string name) : name_(std::move(name)) {}
  std::string_view name() const override { return name_; }
  bool execute(runtime::OperatorContext&) override { return true; }

 private:
  std::string name_;
};

class FlushOperator final : public runtime::IOperator {
 public:
  explicit FlushOperator(storage::StorageEngine* eng) : eng_(eng) {}
  std::string_view name() const override { return "flush_memtable"; }
  bool execute(runtime::OperatorContext&) override { return eng_->flush_memtable(nullptr); }

 private:
  storage::StorageEngine* eng_;
};

class CheckpointOperator final : public runtime::IOperator {
 public:
  explicit CheckpointOperator(storage::StorageEngine* eng) : eng_(eng) {}
  std::string_view name() const override { return "checkpoint"; }
  bool execute(runtime::OperatorContext&) override { return eng_->checkpoint(nullptr); }

 private:
  storage::StorageEngine* eng_;
};

class DrainL0CompactionOperator final : public runtime::IOperator {
 public:
  DrainL0CompactionOperator(storage::StorageEngine* eng, std::uint32_t max_rounds)
      : eng_(eng), max_rounds_(max_rounds == 0 ? 4u : max_rounds) {}
  std::string_view name() const override { return "drain_l0_compaction"; }
  bool execute(runtime::OperatorContext&) override {
    if (eng_->compaction_worker_started()) {
      return eng_->enqueue_drain_l0_compaction_and_wait(max_rounds_, nullptr, 0);
    }
    return eng_->drain_pending_l0_compactions(max_rounds_, nullptr);
  }

 private:
  storage::StorageEngine* eng_;
  std::uint32_t max_rounds_;
};

}  // namespace

Engine::Engine() = default;
Engine::~Engine() { shutdown(); }

bool Engine::startup(std::string* error_out) {
  auto snap = config_.snapshot();
  if (snap.data_dir.empty()) {
    if (error_out) *error_out = "empty data_dir";
    return false;
  }

  // `snap.data_dir` is UTF-8 (C API / FFI contract; `structdb_app` argv on modern Windows is typically UTF-8).
#if defined(_WIN32)
  const std::filesystem::path data_dir_path = std::filesystem::u8path(snap.data_dir);
#else
  const std::filesystem::path data_dir_path(snap.data_dir);
#endif
  storage_ = std::make_shared<storage::StorageEngine>(data_dir_path);
  storage_->set_exclusive_directory_lock(snap.exclusive_data_dir_lock);
  storage_->set_wal_io_backend(snap.io_backend);
  storage_->set_wal_segment_roll_max_bytes(snap.wal_segment_roll_max_bytes);
  storage_->set_undo_segment_roll_max_bytes(snap.undo_segment_roll_max_bytes);
  storage_->set_memtable_backend(snap.memtable_backend);
  if (!storage_->open(error_out, snap.storage_open_flags)) return false;
  storage_->set_wal_fsync_min_interval_ms(snap.wal_fsync_min_interval_ms);
  storage_->set_compaction_merge_min_interval_ms(snap.compaction_merge_min_interval_ms);
  storage_->set_compaction_merge_max_bytes_per_second(snap.compaction_merge_max_bytes_per_second);
  storage_->set_compaction_merge_burst_bytes(snap.compaction_merge_burst_bytes);
  storage_->set_compaction_sequential_sst_read(snap.compaction_sequential_sst_read);
  storage_->set_compaction_worker_low_priority_thread(snap.compaction_worker_low_priority_thread);
  storage_->set_compaction_dedicated_io_executor(snap.compaction_dedicated_io_executor);
  storage_->set_compaction_io_chunk_bytes(snap.compaction_io_chunk_bytes);
  storage_->set_compaction_io_pool_threads(snap.compaction_io_pool_threads);
  storage_->set_compaction_parallel_sst_reads(snap.compaction_parallel_sst_reads);
  storage_->set_wal_append_max_bytes_per_second(snap.wal_append_max_bytes_per_second);
  storage_->set_wal_append_burst_bytes(snap.wal_append_burst_bytes);
  storage_->set_wal_auto_trim_prefix_after_flush(snap.wal_auto_trim_prefix_after_flush);
  storage_->set_wal_archive_gc_after_flush(snap.wal_archive_gc_after_flush);
  storage_->set_undo_auto_truncate_after_flush(snap.undo_auto_truncate_after_flush);
  storage_->set_l0_compact_trigger_threshold(snap.l0_compact_trigger_threshold);
  storage_->set_l0_compact_max_rounds_per_flush(snap.l0_compact_max_rounds_per_flush);
  storage_->set_l1_compact_output_from_l0_merge(snap.l1_compact_output_from_l0_merge);
  storage_->set_l2_compact_output_from_l1_merge(snap.l2_compact_output_from_l1_merge);
  storage_->set_l3_compact_output_from_l2_merge(snap.l3_compact_output_from_l2_merge);
  storage_->set_l4_compact_output_from_l3_merge(snap.l4_compact_output_from_l3_merge);
  storage_->set_l0_compact_defer_after_flush(snap.l0_compact_defer_after_flush);
  storage_->set_l0_compact_max_inline_rounds_per_flush(snap.l0_compact_max_inline_rounds_per_flush);
  if (snap.enable_compaction_worker) {
    const std::size_t qd = snap.compaction_worker_queue_depth == 0 ? 64u : static_cast<std::size_t>(snap.compaction_worker_queue_depth);
    storage_->start_compaction_worker(qd);
  }

  auto budget = std::make_shared<scheduler::ResourceBudget>();
  auto sched = std::make_shared<scheduler::ExecutionScheduler>(budget);
  auto exec = std::make_shared<runtime::GraphExecutor>();
  exec->register_operator("noop", std::make_shared<NoopOperator>("noop"));
  exec->register_operator("flush_memtable", std::make_shared<FlushOperator>(storage_.get()));
  exec->register_operator("checkpoint", std::make_shared<CheckpointOperator>(storage_.get()));

  const std::uint32_t drain_max =
      snap.l0_compact_max_rounds_per_flush == 0 ? 4u : snap.l0_compact_max_rounds_per_flush;
  exec->register_operator("drain_l0_compaction",
                           std::make_shared<DrainL0CompactionOperator>(storage_.get(), drain_max));

  const bool defer_l0_compact = snap.l0_compact_defer_after_flush;
  orchestrator::PlanBuilder builder = [defer_l0_compact](std::uint64_t epoch) {
    if (defer_l0_compact) {
      return planner::ExecutionPlan::make_linear(
          epoch, std::vector<std::string>{"noop", "drain_l0_compaction"});
    }
    return planner::ExecutionPlan::make_linear(epoch, std::vector<std::string>{"noop"});
  };
  orch_ = std::make_shared<orchestrator::Orchestrator>(sched, exec, std::move(builder));
  orch_->set_before_graph_execute([this]() { sync_scheduler_budget_from_storage_pressure(); });

  services_.register_singleton(storage_);
  services_.register_singleton(orch_);

  std::string err;
  if (!orch_->run_default(&err)) {
    if (error_out) *error_out = err;
    return false;
  }
  mdb_chain_txn_active_hint_.store(false, std::memory_order_relaxed);
  embed_bypass_kv_put_during_mdb_chain_observed_.store(0, std::memory_order_relaxed);
  sync_scheduler_budget_from_storage_pressure();

  kv_put_queue_cap_ = snap.kv_put_async_queue_depth;
  if (snap.kv_put_async_queue_depth > 0) {
    {
      std::lock_guard<std::mutex> lk(kv_put_queue_mu_);
      kv_put_worker_stop_ = false;
    }
    kv_put_worker_joinable_ = true;
    kv_put_worker_thread_ = std::thread([this] { kv_put_worker_loop_(); });
  }
  infra::trace_install_from_env_once();
  return true;
}

void Engine::shutdown() {
  mdb_chain_txn_active_hint_.store(false, std::memory_order_relaxed);
  stop_kv_put_worker_join_();
  if (storage_) storage_->close();
  orch_.reset();
  storage_.reset();
  services_.clear();
}

bool Engine::kv_get(const std::string& key, std::string* value_out, std::uint64_t read_max_seq) const {
  if (!storage_) return false;
  return storage_->get(key, value_out, read_max_seq);
}

bool Engine::kv_put(const std::string& key, const std::string& value, bool fsync_wal) {
  if (!storage_) return false;
  const auto snap = config_.snapshot();
  const bool hint = mdb_chain_txn_active_hint_.load(std::memory_order_relaxed);
  if (hint && snap.mdb_chain_rollback_on_mdb_rollback && snap.mdb_persist_in_begin && key_is_mdb_catalog(key)) {
    if (snap.observe_embed_bypass_during_mdb_chain_txn) {
      const auto prev =
          embed_bypass_kv_put_during_mdb_chain_observed_.fetch_add(1, std::memory_order_relaxed);
      if (prev == 0) {
        structdb::infra::log_debug(
            "Phase24: direct Engine::kv_put on mdb$ key while MDB chain-txn hint is active (first in this engine "
            "session)");
      }
    }
    if (snap.strict_reject_direct_kv_put_during_mdb_chain_txn) {
      return false;
    }
  }
  if (snap.kv_put_async_queue_depth > 0) {
    auto task = std::make_unique<KvPutQueuedTask>();
    task->key = key;
    task->value = value;
    task->fsync_wal = fsync_wal;
    std::future<bool> fut = task->done.get_future();
    {
      std::lock_guard<std::mutex> lk(kv_put_queue_mu_);
      if (kv_put_queue_.size() >= static_cast<std::size_t>(snap.kv_put_async_queue_depth)) return false;
      kv_put_queue_.push_back(std::move(task));
    }
    kv_put_queue_cv_.notify_one();
    return fut.get();
  }
  {
    const std::uint64_t est = storage::StorageEngine::estimate_put_wal_frame_bytes(key, value);
    WalMutationBudget budget(this, est);
    const bool ok = storage_->put(key, value, fsync_wal);
    if (ok) sync_scheduler_budget_from_storage_pressure();
    return ok;
  }
}

bool Engine::kv_remove(const std::string& key, bool fsync_wal) {
  if (!storage_) return false;
  const std::uint64_t est =
      storage::StorageEngine::estimate_put_wal_frame_bytes(key, std::string(storage::versioned_kv::kTomb));
  WalMutationBudget budget(this, est);
  const bool ok = storage_->remove(key, fsync_wal);
  if (ok) sync_scheduler_budget_from_storage_pressure();
  return ok;
}

void Engine::kv_visit_prefix(std::string_view prefix,
                             const std::function<bool(std::string_view, std::string_view)>& visitor,
                             std::uint64_t read_max_seq) const {
  if (!storage_) return;
  storage_->visit_prefix(prefix, visitor, read_max_seq);
}

std::uint64_t Engine::latest_commit_seq() const {
  if (!storage_) return 0;
  return storage_->latest_commit_seq();
}

std::size_t Engine::embed_undo_stack_depth() const {
  if (!storage_) return 0;
  return storage_->undo_stack_depth();
}

bool Engine::rollback_embed_undo_until(std::size_t target_depth, std::string* error_out) {
  if (!storage_) {
    if (error_out) *error_out = "engine not started";
    return false;
  }
  return storage_->rollback_undo_frames_until_depth(target_depth, error_out);
}

void Engine::set_mdb_chain_txn_active_hint(bool active) {
  mdb_chain_txn_active_hint_.store(active, std::memory_order_relaxed);
}

std::uint64_t Engine::embed_bypass_kv_put_during_mdb_chain_observed() const {
  return embed_bypass_kv_put_during_mdb_chain_observed_.load(std::memory_order_relaxed);
}

void Engine::storage_pressure_snapshot(structdb::storage::StoragePressureSnapshot* out) const {
  if (!out) return;
  if (!storage_) {
    *out = structdb::storage::StoragePressureSnapshot{};
    return;
  }
  storage_->read_storage_pressure_snapshot(out);
  std::lock_guard<std::mutex> lk(kv_put_queue_mu_);
  out->facade_kv_put_queue_depth = static_cast<std::uint32_t>(kv_put_queue_.size());
  out->facade_kv_put_queue_cap = kv_put_queue_cap_;
}

void Engine::sync_scheduler_budget_from_storage_pressure() {
  if (!orch_ || !storage_) return;
  const auto snap = config_.snapshot();
  structdb::storage::StoragePressureSnapshot p{};
  storage_->read_storage_pressure_snapshot(&p);

  std::int64_t wal_delta = 0;
  if (snap.storage_pressure_l0_soft_start > 0 && p.l0_files >= snap.storage_pressure_l0_soft_start) {
    wal_delta -= static_cast<std::int64_t>((p.l0_files - snap.storage_pressure_l0_soft_start + 1) * 64);
  }
  if (snap.storage_pressure_wal_bytes_soft_start > 0 && p.wal_bytes >= snap.storage_pressure_wal_bytes_soft_start) {
    const std::uint64_t excess = p.wal_bytes - snap.storage_pressure_wal_bytes_soft_start;
    const std::uint64_t step = snap.storage_pressure_wal_bytes_soft_step_bytes == 0
                                   ? (64ull * 1024 * 1024)
                                   : snap.storage_pressure_wal_bytes_soft_step_bytes;
    wal_delta -= static_cast<std::int64_t>((excess / step + 1) * 64);
  }
  orch_->scheduler().budget().set_wal_queue_depth_pressure_delta(wal_delta);

  std::int64_t cs_delta = 0;
  if (snap.storage_pressure_compaction_queue_soft_pct > 0 && p.compaction_worker_queue_cap > 0) {
    const auto thr = static_cast<std::uint64_t>(p.compaction_worker_queue_cap) *
                     static_cast<std::uint64_t>(snap.storage_pressure_compaction_queue_soft_pct);
    const auto d = static_cast<std::uint64_t>(p.compaction_worker_queue_depth) * 100u;
    if (d >= thr) cs_delta -= 1;
  }
  if (snap.storage_pressure_deferred_l0_slot_tighten && p.pending_deferred_l0_compact) cs_delta -= 1;
  orch_->scheduler().budget().set_compaction_slots_pressure_delta(cs_delta);
}

void Engine::stop_kv_put_worker_join_() noexcept {
  if (!kv_put_worker_joinable_) return;
  {
    std::lock_guard<std::mutex> lk(kv_put_queue_mu_);
    kv_put_worker_stop_ = true;
  }
  kv_put_queue_cv_.notify_all();
  if (kv_put_worker_thread_.joinable()) kv_put_worker_thread_.join();
  std::lock_guard<std::mutex> lk(kv_put_queue_mu_);
  while (!kv_put_queue_.empty()) {
    auto t = std::move(kv_put_queue_.front());
    kv_put_queue_.pop_front();
    t->done.set_value(false);
  }
  kv_put_worker_joinable_ = false;
  kv_put_queue_cap_ = 0;
}

void Engine::kv_put_worker_loop_() {
  for (;;) {
    std::unique_ptr<KvPutQueuedTask> task;
    {
      std::unique_lock<std::mutex> lk(kv_put_queue_mu_);
      kv_put_queue_cv_.wait(lk, [&] { return kv_put_worker_stop_ || !kv_put_queue_.empty(); });
      if (kv_put_worker_stop_ && kv_put_queue_.empty()) return;
      if (!kv_put_queue_.empty()) {
        task = std::move(kv_put_queue_.front());
        kv_put_queue_.pop_front();
      }
    }
    if (!task) continue;
    bool ok = false;
    if (storage_) {
      const std::uint64_t est =
          storage::StorageEngine::estimate_put_wal_frame_bytes(task->key, task->value);
      WalMutationBudget budget(this, est);
      ok = storage_->put(task->key, task->value, task->fsync_wal);
      if (ok) sync_scheduler_budget_from_storage_pressure();
    }
    task->done.set_value(ok);
  }
}

bool Engine::drain_l0_compaction_queue(std::uint32_t max_rounds, std::string* error_out, std::uint32_t worker_wait_ms,
                                       std::uint8_t drain_priority) {
  if (!storage_) return false;
  if (config_.snapshot().enable_compaction_worker) {
    return storage_->enqueue_drain_l0_compaction_and_wait(max_rounds, error_out, worker_wait_ms,
                                                          static_cast<int>(drain_priority));
  }
  (void)worker_wait_ms;
  (void)drain_priority;
  return storage_->drain_pending_l0_compactions(max_rounds, error_out);
}

bool Engine::rerun_default_pipeline(std::string* error_out) {
  if (!orch_) {
    if (error_out) *error_out = "engine not started";
    return false;
  }
  return orch_->replan_and_run(error_out);
}

}  // namespace structdb::facade
