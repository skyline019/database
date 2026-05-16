#pragma once

#include <cstdint>
#include <mutex>
#include <string>

#include "structdb/infra/io_backend.hpp"
#include "structdb/storage/memtable_backend.hpp"

namespace structdb::facade {

struct EngineConfigSnapshot {
  std::uint64_t version{0};
  /// Default engine persistence root (WAL / SST / checkpoint under this path). See `Docs/POLICY.md` §4.0.
  std::string data_dir{"_data"};
  std::uint64_t memtable_limit_bytes{8 * 1024 * 1024};
  /// In-memory sorted table backend (`Map` = `std::map`, `SkipList` = skip list). Applied at `StorageEngine::open`. Default **SkipList**.
  structdb::storage::MemTableBackend memtable_backend{structdb::storage::MemTableBackend::SkipList};
  /// When true, engine storage may fsync more aggressively on write paths (beyond embed batch boundaries).
  /// **InnoDB analogy**: stronger durability than default; see `Docs/TXN_INNODB_MAP.md` §2 and `POLICY` §4.5. Default false.
  bool fsync_every_write{false};
  /// Passed to `StorageEngine::open` (e.g. `StorageEngine::kOpenFlagRebuildUndoStackFromLog`). Default 0.
  unsigned storage_open_flags{0};
  /// When true, `flush_memtable` runs WAL prefix trim after checkpoint write (phase 4B; default false).
  bool wal_auto_trim_prefix_after_flush{false};
  /// When true with `wal_auto_trim_prefix_after_flush`, after trim removes sealed `wal/archive/*.log` entries from
  /// catalog and disk (phase 21A; default false). See `Docs/PHASE21.md`.
  bool wal_archive_gc_after_flush{false};
  /// When true, `flush_memtable` truncates `undo.log` after successful checkpoint (phase 8 / 4C; default false). See `Docs/UNDO_LOG_4C.md`.
  bool undo_auto_truncate_after_flush{false};
  /// Phase 11: when **> 0**, after a successful `flush_memtable` checkpoint (and optional WAL/undo hooks), run up to
  /// `l0_compact_max_rounds_per_flush` rounds of `compact_merge_two_oldest_l0` while SST count **>** this value. `0` = off.
  std::uint32_t l0_compact_trigger_threshold{0};
  /// Max `compact_merge_two_oldest_l0` calls per flush when auto L0 compaction is enabled (default 4; `0` treated as 4).
  std::uint32_t l0_compact_max_rounds_per_flush{4};
  /// Phase 12: when true, `compact_merge_two_oldest_l0` emits `L1-{gen}.sst` at manifest tail (see `Docs/PHASE12.md`). Default false.
  bool l1_compact_output_from_l0_merge{false};
  /// Phase 15: allow `compact_merge_two_oldest_l1_to_l2` (see `Docs/PHASE13_PLUS_PLAN.md`). Default false.
  bool l2_compact_output_from_l1_merge{false};
  /// Phase 22A: allow `compact_merge_two_oldest_l2_to_l3` (see `Docs/PHASE22.md`). Default false.
  bool l3_compact_output_from_l2_merge{false};
  /// Phase 23B: allow `compact_merge_two_oldest_l3_to_l4` (see `Docs/PHASE23.md`). Default false.
  bool l4_compact_output_from_l3_merge{false};
  /// Phase 23A: when **> 0** and inline L0 auto-compact runs (`l0_compact_defer_after_flush` false), cap merge rounds per
  /// `flush_memtable` to `min(l0_compact_max_rounds_per_flush, this)`. `0` = no extra cap.
  std::uint32_t l0_compact_max_inline_rounds_per_flush{0};
  /// Phase 13: defer phase-11 style L0 merges until `Engine::drain_l0_compaction_queue` / storage drain API. Default false.
  bool l0_compact_defer_after_flush{false};
  /// Phase 14: when **> 0** and L0 file count **>=** this value, tighten scheduler WAL queue budget (see `Engine::sync_scheduler_budget_from_storage_pressure`). `0` = off.
  std::uint32_t storage_pressure_l0_soft_start{0};
  /// Phase 21C: when **1–100** and compaction worker is enabled, tighten **`CompactionSlots`** when
  /// `queue_depth * 100 >= queue_cap * this_percent`. `0` = off.
  std::uint32_t storage_pressure_compaction_queue_soft_pct{0};
  /// Phase 21C: when true and storage reports `pending_deferred_l0_compact`, subtract one effective compaction slot.
  bool storage_pressure_deferred_l0_slot_tighten{false};
  /// Phase 18 / 20: logical I/O backend for WAL (`WalWriter`); `IocpAsync` uses IOCP when built with `STRUCTDB_HAVE_IOCP`.
  structdb::infra::IoBackendConfig io_backend{};
  /// Phase 20A: when > 0, roll `wal.log` into `wal/archive/{seq}.log` once tail exceeds this size (set before `Engine::startup`).
  std::uint64_t wal_segment_roll_max_bytes{0};
  /// When **> 0**, enforce a minimum wall-clock interval between successful WAL fsyncs (burst smoothing). `0` = off.
  std::uint32_t wal_fsync_min_interval_ms{0};
  /// When **> 0**, enforce a minimum wall-clock interval between consecutive successful **L0** merges
  /// (`compact_merge_two_oldest_l0` / inline post-flush compaction / deferred drain). `0` = off.
  std::uint32_t compaction_merge_min_interval_ms{0};
  /// Token-bucket refill rate for **compaction merge I/O** (estimated bytes read + written per merge materialize). `0` = off.
  std::uint64_t compaction_merge_max_bytes_per_second{0};
  /// Merge byte bucket burst ceiling; `0` = `max(compaction_merge_max_bytes_per_second, 65536)`.
  std::uint64_t compaction_merge_burst_bytes{0};
  /// When true (default), SST full reads during merge materialization use a sequential-scan hint (OS-specific).
  bool compaction_sequential_sst_read{true};
  /// When true (default) with `enable_compaction_worker`, lower compaction-related background thread scheduling where
  /// supported (Windows: `THREAD_MODE_BACKGROUND_*` on the worker; Linux: `nice` + best-effort `SCHED_IDLE` on worker
  /// and on `CompactionIoExecutor` when `compaction_dedicated_io_executor` is enabled).
  bool compaction_worker_low_priority_thread{true};
  /// When true, merge materialize I/O runs on a dedicated worker thread (see `StorageEngine`).
  bool compaction_dedicated_io_executor{false};
  /// Merge materialize read/write chunk size; `0` = engine default when dedicated I/O or merge byte throttle is on.
  std::uint32_t compaction_io_chunk_bytes{0};
  /// Dedicated compaction I/O pool size. `0` = default **2** worker threads when `compaction_dedicated_io_executor` is on.
  std::uint32_t compaction_io_pool_threads{0};
  /// When true (default), merge materialize may read the two input SSTs concurrently (separate threads; WAL path unchanged).
  bool compaction_parallel_sst_reads{true};
  /// Token-bucket refill rate for WAL **frame bytes** (`4 + payload` per `append_record`). `0` = off.
  std::uint64_t wal_append_max_bytes_per_second{0};
  /// Append token-bucket burst ceiling; `0` = `max(wal_append_max_bytes_per_second, 65536)`.
  std::uint64_t wal_append_burst_bytes{0};
  /// When **> 0** and on-disk `wal_bytes` **>=** this, tighten scheduler `WalQueueDepth` ceiling (additive with L0 rule).
  std::uint64_t storage_pressure_wal_bytes_soft_start{0};
  /// Excess WAL bytes above soft start: each full **step** adds **-64** to WAL-queue delta (same unit as L0). `0` = **64 MiB** per step.
  std::uint64_t storage_pressure_wal_bytes_soft_step_bytes{0};
  /// When **> 0**, mutating `Engine::{kv_put,kv_remove}` / `EmbedClient::submit` blocks on `WalQueueDepth` with
  /// `ceil(estimated_wal_frame_bytes / this)` slots. `0` = off.
  std::uint64_t wal_scheduler_bytes_per_depth_slot{0};
  /// Caps scheduler slots acquired per mutation (`0` = no extra cap beyond an internal safety maximum).
  std::uint32_t wal_scheduler_max_slots_per_op{0};
  /// Phase 22C: when > 0, roll `undo.log` into `undo/archive/{seq}.log` (set before `Engine::startup`). See `Docs/PHASE22.md`.
  std::uint64_t undo_segment_roll_max_bytes{0};
  /// Phase 20B: run `drain_pending_l0_compactions` on a dedicated worker thread (bounded queue); default false.
  bool enable_compaction_worker{false};
  /// Max queued compaction jobs when `enable_compaction_worker` (default 64).
  std::uint32_t compaction_worker_queue_depth{64};
  /// When true (default), MDB `persist_table` runs after mutating commands while `BEGIN` is active (same embed batch
  /// path as outside a txn). `ROLLBACK` still restores only session state **unless** `mdb_chain_rollback_on_mdb_rollback`
  /// is true (phase 23C: pops `undo_stack_` back to depth captured at `BEGIN`). See `Docs/TXN_BEGIN_PERSIST_DESIGN.md`,
  /// `Docs/PHASE23.md`, and `POLICY.md` §4.3.
  bool mdb_persist_in_begin{true};
  /// Phase 23C: when true with `mdb_persist_in_begin`, MDB `ROLLBACK` calls `Engine::rollback_embed_undo_until` to the
  /// undo depth recorded at `BEGIN` (restricted model: concurrent non-MDB versioned writes are undefined). Default false.
  bool mdb_chain_rollback_on_mdb_rollback{false};
  /// Phase 24A: when true with `mdb_chain_rollback_on_mdb_rollback` + `mdb_persist_in_begin`, count `Engine::kv_put`
  /// calls whose key starts with `mdb$` while `mdb_runner` has marked an active MDB chain txn (see `Docs/PHASE24.md`).
  /// Default false.
  bool observe_embed_bypass_during_mdb_chain_txn{false};
  /// Phase 24A: when true with the same chain+persist combo and active hint, reject `Engine::kv_put` for `mdb$*` keys
  /// (strict single-writer guard). Default false.
  bool strict_reject_direct_kv_put_during_mdb_chain_txn{false};
  /// Phase 35: acquire advisory lock on `data_dir/.structdb_exclusive.lock` during `StorageEngine::open`..`close`.
  bool exclusive_data_dir_lock{false};
  /// Phase 36: optional bounded queue for `Engine::kv_put` (single worker). `0` = synchronous `storage_->put` (default).
  std::uint32_t kv_put_async_queue_depth{0};
  /// When true (default), MDB `persist_table` may emit embed batches that touch only dirty rows + row index (v2 tables).
  bool mdb_incremental_persist{true};
};

class ConfigurableEngine {
 public:
  EngineConfigSnapshot snapshot() const {
    std::lock_guard<std::mutex> lock(mu_);
    return snap_;
  }

  void update(std::uint64_t new_version, EngineConfigSnapshot s) {
    std::lock_guard<std::mutex> lock(mu_);
    snap_ = std::move(s);
    snap_.version = new_version;
  }

  std::uint64_t version() const {
    std::lock_guard<std::mutex> lock(mu_);
    return snap_.version;
  }

 private:
  mutable std::mutex mu_;
  EngineConfigSnapshot snap_{};
};

}  // namespace structdb::facade
