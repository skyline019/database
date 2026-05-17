#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>

#include "structdb/facade/config.hpp"
#include "structdb/facade/service_container.hpp"
#include "structdb/storage/storage_pressure.hpp"

namespace structdb::orchestrator {
class Orchestrator;
}

namespace structdb::storage {
class StorageEngine;
}

namespace structdb::facade {

class Engine {
 public:
  Engine();
  ~Engine();

  Engine(const Engine&) = delete;
  Engine& operator=(const Engine&) = delete;

  ServiceContainer& services() { return services_; }
  const ServiceContainer& services() const { return services_; }

  ConfigurableEngine& config() { return config_; }
  const ConfigurableEngine& config() const { return config_; }

  bool startup(std::string* error_out = nullptr);
  void shutdown();

  /// Phase 43: destructive PITR to `checkpoint_seq` in `checkpoint.chain` (storage must be shut down first).
  bool recover_to_checkpoint_seq(std::uint64_t checkpoint_seq, std::string* error_out = nullptr);

  orchestrator::Orchestrator* orchestrator() { return orch_.get(); }
  storage::StorageEngine* storage() { return storage_.get(); }

  /// Narrow KV read for `client/mdb` (avoids pulling `storage_engine.hpp` into embed-only paths).
  bool kv_get(const std::string& key, std::string* value_out) const {
    return kv_get(key, value_out, (std::numeric_limits<std::uint64_t>::max)());
  }
  bool kv_get(const std::string& key, std::string* value_out, std::uint64_t read_max_seq) const;

  bool kv_put(const std::string& key, const std::string& value, bool fsync_wal);
  bool kv_remove(const std::string& key, bool fsync_wal);

  void kv_visit_prefix(std::string_view prefix,
                       const std::function<bool(std::string_view key, std::string_view value)>& visitor) const {
    kv_visit_prefix(prefix, visitor, (std::numeric_limits<std::uint64_t>::max)());
  }
  void kv_visit_prefix(std::string_view prefix,
                       const std::function<bool(std::string_view key, std::string_view value)>& visitor,
                       std::uint64_t read_max_seq) const;

  std::uint64_t latest_commit_seq() const;

  /// Phase 23C: `StorageEngine::undo_stack_depth()` for MDB `BEGIN` watermark (embed versioned writes only).
  std::size_t embed_undo_stack_depth() const;

  /// Phase 23C: pop undo frames until depth equals `target_depth` (see `mdb_chain_rollback_on_mdb_rollback`).
  bool rollback_embed_undo_until(std::size_t target_depth, std::string* error_out = nullptr);

  /// Phase 24A: `mdb_runner` sets this while an MDB `BEGIN` is active **and** `mdb_chain_rollback_on_mdb_rollback` +
  /// `mdb_persist_in_begin` are both true (hint only; not a lock).
  void set_mdb_chain_txn_active_hint(bool active);
  /// Phase 24A: incremented when `observe_embed_bypass_during_mdb_chain_txn` is on and `kv_put` sees an `mdb$*`
  /// key while the hint is active (and chain+persist config). Resets on `startup`.
  std::uint64_t embed_bypass_kv_put_during_mdb_chain_observed() const;

  /// Phase 14: L0 depth, WAL/undo sizes, manifest version (from `StorageEngine` under its mutex).
  void storage_pressure_snapshot(structdb::storage::StoragePressureSnapshot* out) const;

  /// Phase 14 / 21C: apply `EngineConfigSnapshot` storage-pressure fields to scheduler `ResourceBudget` (WAL queue
  /// delta, compaction slot delta).
  void sync_scheduler_budget_from_storage_pressure();

  /// When `EngineConfigSnapshot::wal_scheduler_bytes_per_depth_slot > 0`, blocks on `WalQueueDepth` for the estimated
  /// WAL frame size; releases in destructor (used by `kv_put` / `kv_remove` / `EmbedClient::submit`).
  struct WalMutationBudget {
    Engine* eng{nullptr};
    std::uint32_t slots{0};
    explicit WalMutationBudget(Engine* e, std::uint64_t approx_wal_frame_bytes);
    WalMutationBudget(const WalMutationBudget&) = delete;
    WalMutationBudget& operator=(const WalMutationBudget&) = delete;
    ~WalMutationBudget();
  };

  /// Phase 13: drain deferred L0 compactions (see `set_l0_compact_defer_after_flush` on storage). When the compaction
  /// worker is enabled, `worker_wait_ms` is passed to `StorageEngine::enqueue_drain_l0_compaction_and_wait` (`0` =
  /// unbounded wait). `drain_priority`: higher values are scheduled before lower on the worker queue (e.g. MDB `VACUUM`).
  bool drain_l0_compaction_queue(std::uint32_t max_rounds, std::string* error_out = nullptr,
                                 std::uint32_t worker_wait_ms = 0, std::uint8_t drain_priority = 0);

  /// Phase 19: re-run the default orchestrator plan (`replan_and_run`). When `l0_compact_defer_after_flush` is on at
  /// startup, the plan is `noop` then `drain_l0_compaction`; use after batch flushes to drain deferred L0 merges.
  bool rerun_default_pipeline(std::string* error_out = nullptr);

 private:
  friend struct WalMutationBudget;
  std::uint32_t wal_scheduler_slots_for_frame_bytes_(std::uint64_t approx_frame_bytes,
                                                     const EngineConfigSnapshot& snap) const;
  void wal_scheduler_acquire_blocking_(std::uint32_t slots);
  void wal_scheduler_release_slots_(std::uint32_t slots);

  void stop_kv_put_worker_join_() noexcept;
  void kv_put_worker_loop_();

  ServiceContainer services_;
  ConfigurableEngine config_;
  std::shared_ptr<orchestrator::Orchestrator> orch_;
  std::shared_ptr<storage::StorageEngine> storage_;
  std::atomic<bool> mdb_chain_txn_active_hint_{false};
  std::atomic<std::uint64_t> embed_bypass_kv_put_during_mdb_chain_observed_{0};

  std::uint32_t kv_put_queue_cap_{0};
  mutable std::mutex kv_put_queue_mu_;
  std::condition_variable kv_put_queue_cv_;
  struct KvPutQueuedTask {
    std::string key;
    std::string value;
    bool fsync_wal{false};
    std::promise<bool> done;
  };
  std::deque<std::unique_ptr<KvPutQueuedTask>> kv_put_queue_;
  std::thread kv_put_worker_thread_;
  bool kv_put_worker_stop_{false};
  bool kv_put_worker_joinable_{false};
};

}  // namespace structdb::facade
