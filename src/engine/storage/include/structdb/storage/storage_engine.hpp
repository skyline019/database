#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "structdb/storage/compaction_io_executor.hpp"
#include "structdb/storage/byte_token_bucket.hpp"
#include "structdb/infra/long_task_progress.hpp"
#include "structdb/storage/buffer_pool.hpp"
#include "structdb/storage/checkpoint.hpp"
#include "structdb/storage/lsm_state.hpp"
#include "structdb/storage/manifest.hpp"
#include "structdb/storage/memtable_backend.hpp"
#include "structdb/storage/memtable_manager.hpp"
#include "structdb/storage/redo_log.hpp"
#include "structdb/storage/undo_log.hpp"
#include "structdb/storage/storage_pressure.hpp"
#include "structdb/storage/versioned_kv.hpp"
#include "structdb/storage/wal.hpp"
#include "structdb/storage/wal_coordinator.hpp"
#include "structdb/storage/checkpoint_undo_coordinator.hpp"
#include "structdb/storage/compaction_coordinator.hpp"
#include "structdb/storage/compaction_result.hpp"
#include "structdb/storage/recovery_open_policy.hpp"
#include "structdb/storage/wal_replay_applier.hpp"
#include "structdb/storage/recovery_coordinator.hpp"
#include "structdb/storage/storage_telemetry.hpp"
#include "structdb/infra/io_backend.hpp"

namespace structdb::storage {

class StorageEngine {
 public:
  explicit StorageEngine(std::filesystem::path data_dir);

  bool open(std::string* error_out = nullptr, unsigned open_flags = 0);
  void close();

  /// Phase 35: optional cross-process advisory lock on `data_dir/.structdb_exclusive.lock` (set before `open`).
  void set_exclusive_directory_lock(bool enable) { exclusive_directory_lock_ = enable; }

  bool put(const std::string& key, const std::string& value, bool fsync_wal) {
    return put(key, value, fsync_wal, 0);
  }
  /// When `batch_commit_seq != 0`, all versioned `mdb$` logical puts in one embed/journal batch share this seq.
  bool put(const std::string& key, const std::string& value, bool fsync_wal, std::uint64_t batch_commit_seq);
  /// Reserve one `commit_seq` for an upcoming batch (call once, then pass to each `put(..., seq)` in that batch).
  std::uint64_t reserve_commit_seq();
  bool remove(const std::string& key, bool fsync_wal);

  /// Returns logical payload (unwraps `mdbver1:` for `mdb$` keys). Tombstones and invisible versions read as miss.
  bool get(const std::string& key, std::string* value_out) const {
    return get(key, value_out, versioned_read_seq_latest());
  }
  bool get(const std::string& key, std::string* value_out, std::uint64_t read_max_seq) const;

  /// `read_max_seq == max uint64` means latest visible (no upper bound on stored commit seq).
  static std::uint64_t versioned_read_seq_latest();

  /// Monotonic high-water for new versioned puts; `data_dir/COMMIT_SEQ` is updated on `open` (after WAL replay),
  /// `close`, `checkpoint`, and flush/compaction paths — not on every `put` (WAL + in-memory `observe_*` remain authoritative).
  std::uint64_t latest_commit_seq() const { return commit_seq_hw_.load(std::memory_order_relaxed); }

  /// Visits MemTable then SSTs (newest SST first) for keys starting with `prefix`.
  /// Visitor receives **logical** values (unwrapped). Tombstones skipped.
  /// Visitor returns false to stop early.
  void visit_prefix(std::string_view prefix,
                    const std::function<bool(std::string_view key, std::string_view value)>& visitor) const {
    visit_prefix(prefix, visitor, versioned_read_seq_latest());
  }
  void visit_prefix(std::string_view prefix,
                    const std::function<bool(std::string_view key, std::string_view value)>& visitor,
                    std::uint64_t read_max_seq) const;

  /// Seal memtable to L0 SST and update manifest. Swaps active mem into `MemTableManager`'s frozen snapshot, records WAL offset,
  /// materializes sorted SST **outside** `mu_` (to `dir_/_structdb_memflush_tmp.sst` then renames to `L0-{gen}.sst`),
  /// then updates MANIFEST / `lsm_` / checkpoint under lock. Overlapping `flush_memtable` returns false
  /// (`flush_memtable already in progress`). On materialize/rename/manifest failure, frozen keys merge back into active mem.
  bool flush_memtable(std::string* error_out);

  /// Phase 9: merge the two **oldest** L0 SSTs (first two `level==0` manifest entries) into one new SST,
  /// update MANIFEST + checkpoint. Requires at least two L0 files. See `Docs/COMPACTION.md` / `PHASE12.md`.
  bool compact_merge_two_oldest_l0(std::string* error_out);

  /// Phase 15: merge the two **oldest** L1 SSTs (first two `level==1` entries in the L1 block) into one **L2** SST
  /// at manifest tail. Requires `set_l2_compact_output_from_l1_merge(true)` and at least two L1 files.
  bool compact_merge_two_oldest_l1_to_l2(std::string* error_out);

  /// Phase 22A: merge the two **oldest** L2 SSTs (first two `level==2` entries in the L2 block) into one **L3** SST
  /// at manifest tail. Requires `set_l3_compact_output_from_l2_merge(true)` and at least two L2 files.
  bool compact_merge_two_oldest_l2_to_l3(std::string* error_out);

  /// Phase 13: after `flush_memtable` with `set_l0_compact_defer_after_flush(true)`, run pending L0 merges outside
  /// the flush tail (same thread / same mutex). Up to `max_rounds` successful `compact_merge_two_oldest_l0` calls.
  bool drain_pending_l0_compactions(std::uint32_t max_rounds, std::string* error_out);

  /// Phase 20B: optional single worker + bounded queue; `enqueue_drain_l0_compaction_and_wait` runs drain on worker.
  void start_compaction_worker(std::size_t queue_max_depth);
  void stop_compaction_worker();
  /// When `wait_ms > 0`, uses `std::future::wait_for` instead of unbounded `get()` (job may still complete on worker).
  /// `drain_priority`: higher values run before lower on the compaction worker queue (FIFO within same priority).
  bool enqueue_drain_l0_compaction_and_wait(std::uint32_t max_rounds, std::string* error_out, std::uint32_t wait_ms = 0,
                                             int drain_priority = 0);
  bool compaction_worker_started() const { return compaction_worker_joinable_; }

  /// Phase 20A: when > 0, WAL may roll to `wal/archive/{seq}.log` and persist `wal.segments` v2 (must be set before `open`).
  void set_wal_segment_roll_max_bytes(std::uint64_t max_tail_bytes) { wal_segment_roll_max_bytes_ = max_tail_bytes; }

  /// Phase 22C: when > 0, `undo.log` rolls to `undo/archive/{seq}.log` and persists `undo.segments` v2 (set before `open`).
  void set_undo_segment_roll_max_bytes(std::uint64_t max_tail_bytes) { undo_segment_roll_max_bytes_ = max_tail_bytes; }

  /// Phase 20C / 18: I/O backend for WAL (`WalWriter`); must be set before `open` to take effect.
  void set_wal_io_backend(structdb::infra::IoBackendConfig cfg) { wal_io_cfg_ = cfg; }

  /// Minimum spacing between successful WAL fsyncs (`WalWriter`); `0` disables. Safe to call after `open`.
  void set_wal_fsync_min_interval_ms(std::uint32_t ms) { wal_.set_fsync_min_interval_ms(ms); }

  /// Minimum spacing between consecutive successful **L0** merges; `0` disables. Safe to call after `open`.
  void set_compaction_merge_min_interval_ms(std::uint32_t ms) { compaction_merge_min_interval_ms_ = ms; }

  /// Merge materialization byte token bucket (`0` disables). Safe to call after `open`.
  void set_compaction_merge_max_bytes_per_second(std::uint64_t bytes_per_sec) {
    compaction_merge_byte_tb_.set_max_bytes_per_second(bytes_per_sec);
  }
  void set_compaction_merge_burst_bytes(std::uint64_t burst_bytes) {
    compaction_merge_byte_tb_.set_burst_bytes(burst_bytes);
  }
  void set_compaction_sequential_sst_read(bool enable) { compaction_sequential_sst_read_ = enable; }
  void set_compaction_worker_low_priority_thread(bool enable) { compaction_worker_low_priority_thread_ = enable; }
  /// When true, merge **materialize** (full SST read + merged SST write) runs on a dedicated executor thread pool
  /// (never shares `WalWriter` / `wal.log` — SST + temp files only).
  void set_compaction_dedicated_io_executor(bool enable) { compaction_dedicated_io_executor_ = enable; }
  /// Max bytes per read/write syscall group during merge materialize; `0` = choose default when dedicated I/O or merge
  /// byte throttle is enabled (see implementation), else use single-shot I/O.
  void set_compaction_io_chunk_bytes(std::uint32_t bytes) { compaction_io_chunk_bytes_ = bytes; }
  /// Worker threads for `CompactionIoExecutor` when dedicated I/O is enabled. `0` = default **2** workers.
  void set_compaction_io_pool_threads(std::uint32_t n) { compaction_io_pool_threads_ = n; }
  /// When true (default), merge materialize may load the two input SSTs concurrently (two input files only).
  void set_compaction_parallel_sst_reads(bool enable) { compaction_parallel_sst_reads_ = enable; }

  /// WAL append byte token bucket (`WalWriter`); `0` disables. Safe to call after `open`.
  void set_wal_append_max_bytes_per_second(std::uint64_t bytes_per_sec) {
    wal_.set_append_max_bytes_per_second(bytes_per_sec);
  }
  void set_wal_append_burst_bytes(std::uint64_t burst_bytes) { wal_.set_append_burst_bytes(burst_bytes); }

  /// Upper bound for one `put` WAL frame (`4 + line`) for scheduler / tuning (conservative vs versioned encoding).
  static std::uint64_t estimate_put_wal_frame_bytes(const std::string& key, const std::string& logical_value);
  /// Upper bound for one `commit_embed_batch` WAL record (same layout as `commit_embed_batch_unlocked_`).
  static std::uint64_t estimate_commit_embed_batch_wal_frame_bytes(
      const std::vector<std::string>& dels, const std::vector<std::pair<std::string, std::string>>& puts);

  /// Phase 14: L0 depth, WAL / undo sizes, manifest version (under `mu_`).
  void read_storage_pressure_snapshot(StoragePressureSnapshot* out) const;

  /// Optional observer for compaction merge / flush long-task progress (not owned; caller clears when done).
  void set_active_long_task(structdb::infra::LongTaskReporter* reporter) { active_long_task_ = reporter; }
  structdb::infra::LongTaskReporter* active_long_task() noexcept { return active_long_task_; }
  const structdb::infra::LongTaskReporter* active_long_task() const noexcept { return active_long_task_; }

  /// Phase 16: segment count from `wal.segments` metadata (currently always **1** physical `wal.log`).
  std::uint32_t wal_log_segment_count_observed() const { return wal_segment_count_observed_; }

  /// Phase 22C: observed undo physical segments (`undo.segments` v2 sealed + active `undo.log`, or **1**).
  std::uint32_t undo_log_segment_count_observed() const { return undo_segment_count_observed_; }

  bool checkpoint(std::string* error_out);

  /// Fsync WAL after a batch of writes (used with `EmbedClient::submit(..., fsync_journal=true)`).
  bool wal_sync(std::string* error_out = nullptr);

  /// Size of `wal.log` (bytes). Embed uses `> 0` to prefer WAL crash recovery over journal replay.
  std::uint64_t wal_log_bytes_on_disk() const;

  /// Restores the previous stored value for the last versioned overwrite recorded in this process (LIFO undo stack).
  bool rollback_one_undo_frame(std::string* error_out = nullptr);

  /// Phase 23C: current `undo_stack_.size()` (under `mu_`). Used with `rollback_undo_frames_until_depth` for MDB chain rollback.
  std::size_t undo_stack_depth() const;

  /// Phase 23C: pop and apply undo frames until stack depth equals `target_depth` (no-op if already `<= target_depth`).
  /// Caller must ensure `target_depth` was captured at a safe boundary (e.g. MDB `BEGIN`); see `Docs/PHASE23.md`.
  bool rollback_undo_frames_until_depth(std::size_t target_depth, std::string* error_out = nullptr);

  /// Phase 23B: merge two oldest **L3** SSTs into one **L4** SST. Requires `set_l4_compact_output_from_l3_merge(true)`.
  bool compact_merge_two_oldest_l3_to_l4(std::string* error_out);

  /// Single WAL record for all dels + puts (embed batch); crash after record fsync yields all-or-nothing replay.
  bool commit_embed_batch(const std::vector<std::string>& dels, const std::vector<std::pair<std::string, std::string>>& puts,
                          bool fsync_wal, std::string* error_out = nullptr);

  IMemTable& memtable() { return mem_mgr_.active(); }
  const IMemTable& memtable() const { return mem_mgr_.active(); }

  /// Selects memtable implementation for the **next** `open()` (`Map` or `SkipList`). Call before `open`.
  void set_memtable_backend(MemTableBackend b) { memtable_backend_ = b; }
  MemTableBackend memtable_backend() const noexcept { return memtable_backend_; }

  Manifest& manifest() { return manifest_; }
  const Manifest& manifest() const { return manifest_; }
  BufferPool& buffer_pool() { return *pool_; }

  const LsmState& lsm_state() const { return lsm_; }

  /// When passed to `open`, scan `undo.log` and rebuild `undo_stack_` (experimental; see CHANGELOG).
  static constexpr unsigned kOpenFlagRebuildUndoStackFromLog = recovery_open_policy::kRebuildUndoStackFromLog;

  /// Bytes of `data_dir/undo.log` if present (phase 4a observability).
  std::uint64_t undo_log_bytes_on_disk() const;

  /// Phase 9: number of successful `compact_merge_two_oldest_l0` runs in this process (observability).
  std::uint64_t compaction_merge_count() const { return compaction_merge_count_.load(std::memory_order_relaxed); }

  /// Reads persisted checkpoint (phase 5: binary `checkpoint.a`/`checkpoint.b` + `checkpoint.active`, then legacy `checkpoint` text). Returns false on corruption when checkpoint files exist.
  bool read_checkpoint_state(CheckpointState* out);

  /// Phase 4B: rewrites `wal.log` to only bytes **[checkpoint.wal_offset, EOF)** and persists `wal_offset=0`.
  /// Safe only when all WAL bytes before `checkpoint.wal_offset` are fully reflected in flushed SSTs (e.g. immediately after `flush_memtable`).
  bool wal_try_trim_prefix_through_checkpoint(std::string* error_out);

  /// When true, after a successful `flush_memtable` WAL prefix trim (`wal_auto_trim_prefix_after_flush`), remove
  /// sealed `wal/archive/*.log` files and persist an empty sealed list in `wal.segments` v2 (default false). See
  /// `Docs/PHASE21.md` / `POLICY` §3.3.
  void set_wal_archive_gc_after_flush(bool enable) { wal_archive_gc_after_flush_ = enable; }

  /// Phase 8 / 4C: truncate `undo.log` to empty when `undo_stack_` is empty (no pending process-local rollback frames). See `Docs/UNDO_LOG_4C.md`.
  bool undo_try_truncate_when_stack_empty(std::string* error_out = nullptr);

  /// Phase 10: physically remove the recyclable prefix of `undo.log` (see `Docs/PHASE10.md`).
  bool undo_try_truncate_recyclable_prefix(std::string* error_out = nullptr);

  /// When true, `flush_memtable` invokes `wal_try_trim_prefix_through_checkpoint` after a successful checkpoint write (default false).
  void set_wal_auto_trim_prefix_after_flush(bool enable) { wal_auto_trim_prefix_after_flush_ = enable; }

  /// When true, `flush_memtable` truncates `undo.log` after a successful checkpoint write (default false). See `Docs/UNDO_LOG_4C.md`.
  void set_undo_auto_truncate_after_flush(bool enable) { undo_auto_truncate_after_flush_ = enable; }

  /// Phase 11: `0` disables; otherwise `flush_memtable` may run L0 merges while SST count exceeds this value. See `Docs/PHASE11.md`.
  void set_l0_compact_trigger_threshold(std::uint32_t max_l0_files_before_compact) {
    l0_compact_trigger_threshold_ = max_l0_files_before_compact;
  }
  /// Upper bound on `compact_merge_two_oldest_l0` invocations per `flush_memtable` when threshold mode is on (default 4).
  void set_l0_compact_max_rounds_per_flush(std::uint32_t max_rounds) {
    l0_compact_max_rounds_per_flush_ = max_rounds == 0 ? 4u : max_rounds;
  }

  /// Phase 23A: when **> 0** and inline L0 auto-compact runs (`l0_compact_defer_after_flush` false), cap rounds per
  /// `flush_memtable` to `min(l0_compact_max_rounds_per_flush, this)`. `0` = no extra cap (phase 11 behavior).
  void set_l0_compact_max_inline_rounds_per_flush(std::uint32_t max_rounds) {
    l0_compact_max_inline_rounds_per_flush_ = max_rounds;
  }

  /// Phase 12: when true, `compact_merge_two_oldest_l0` writes `L1-{gen}.sst` (level 1) at manifest tail instead of `L0-{gen}` at front.
  void set_l1_compact_output_from_l0_merge(bool enable) { l1_compact_output_from_l0_merge_ = enable; }

  /// Phase 15: enable `compact_merge_two_oldest_l1_to_l2` (writes `L2-{gen}.sst`). Default false.
  void set_l2_compact_output_from_l1_merge(bool enable) { l2_compact_output_from_l1_merge_ = enable; }

  /// Phase 22A: enable `compact_merge_two_oldest_l2_to_l3` (writes `L3-{gen}.sst`). Default false.
  void set_l3_compact_output_from_l2_merge(bool enable) { l3_compact_output_from_l2_merge_ = enable; }

  /// Phase 23B: enable `compact_merge_two_oldest_l3_to_l4` (writes `L4-{gen}.sst`). Default false.
  void set_l4_compact_output_from_l3_merge(bool enable) { l4_compact_output_from_l3_merge_ = enable; }

  /// Phase 13: when true with `l0_compact_trigger_threshold > 0`, `flush_memtable` only **marks** work; call
  /// `drain_pending_l0_compactions` to merge (reduces flush tail latency). Default false (inline merge, phase 11).
  void set_l0_compact_defer_after_flush(bool enable) { l0_compact_defer_after_flush_ = enable; }

 private:
  friend class WalCoordinator;
  friend class CompactionCoordinator;
  friend class CheckpointUndoCoordinator;
  friend class RecoveryCoordinator;
  friend class StorageTelemetry;
  friend class WalReplayApplier;

  bool load_commit_seq_hw_(std::string* error_out);
  bool persist_commit_seq_hw_(std::string* error_out);
  void observe_stored_commit_seq_(std::string_view stored);
  bool decode_get_visible_(std::string_view raw_stored, std::uint64_t read_max_seq, std::string* logical_out) const;
  bool mem_layers_get_raw_unlocked_(const std::string& key, std::string* raw_out) const;
  bool commit_embed_batch_unlocked_(const std::vector<std::string>& dels,
                                    const std::vector<std::pair<std::string, std::string>>& puts, bool fsync_wal,
                                    std::string* error_out);
  bool put_impl_unlocked_(const std::string& key, const std::string& value, bool fsync_wal, std::uint64_t batch_commit_seq,
                          bool record_versioned_undo);
  /// Latest non-tomb stored value in mem then SST (newest SST first); false if none.
  bool lookup_versioned_raw_for_undo_unlocked_(const std::string& key, std::string* prev_raw_out) const;
  bool append_versioned_undo_if_needed_unlocked_(const std::string& key);
  std::string logical_to_stored_for_put_unlocked_(const std::string& key, const std::string& logical,
                                                   std::uint64_t batch_commit_seq);

  struct UndoStackEntry {
    std::string key;
    std::string prev_raw;
    std::uint64_t frame_start_byte{0};
  };

  void compaction_worker_loop_();

  void report_active_long_task_(structdb::infra::LongTaskKind kind, structdb::infra::LongTaskStatus status,
                                std::uint64_t units_done, std::uint64_t units_total, std::string_view detail = {});

  struct CompactionWorkerJob {
    int drain_priority{0};
    std::uint64_t enqueue_seq{0};
    std::packaged_task<bool(std::string*)> task;
  };

  std::filesystem::path dir_;
  WalWriter wal_;
  MemTableBackend memtable_backend_{MemTableBackend::SkipList};
  MemTableManager mem_mgr_;
  Manifest manifest_;
  RedoLog redo_;
  UndoLog undo_;
  std::unique_ptr<BufferPool> pool_;
  CheckpointWriter ckpt_;
  LsmState lsm_;
  bool opened_{false};
  /// Protects mem/manifest/WAL metadata paths: readers take `std::shared_lock`, writers `std::unique_lock`.
  mutable std::shared_mutex mu_;
  std::atomic<std::uint64_t> commit_seq_hw_{0};
  /// In-memory LIFO for `rollback_one_undo_frame`. Default: built only from versioned puts in this process.
  /// Optionally repopulated from `undo.log` when `open(..., kOpenFlagRebuildUndoStackFromLog)` is used (see CHANGELOG).
  std::vector<UndoStackEntry> undo_stack_;
  bool wal_auto_trim_prefix_after_flush_{false};
  bool wal_archive_gc_after_flush_{false};
  bool undo_auto_truncate_after_flush_{false};
  std::uint32_t l0_compact_trigger_threshold_{0};
  std::uint32_t l0_compact_max_rounds_per_flush_{4};
  std::uint32_t l0_compact_max_inline_rounds_per_flush_{0};
  bool l1_compact_output_from_l0_merge_{false};
  bool l2_compact_output_from_l1_merge_{false};
  bool l3_compact_output_from_l2_merge_{false};
  bool l4_compact_output_from_l3_merge_{false};
  bool l0_compact_defer_after_flush_{false};
  bool pending_deferred_l0_compact_{false};
  std::uint32_t wal_segment_count_observed_{1};
  std::uint32_t undo_segment_count_observed_{1};
  bool undo_catalog_v2_{false};
  std::vector<std::string> undo_sealed_relative_;
  std::uint64_t undo_next_roll_seq_{1};
  std::uint64_t undo_segment_roll_max_bytes_{0};
  std::atomic<std::uint64_t> compaction_merge_count_{0};
  std::atomic<std::uint64_t> wal_append_record_calls_total_{0};
  std::atomic<std::uint64_t> wal_fsync_calls_total_{0};
  std::atomic<std::uint64_t> flush_memtable_success_total_{0};
  std::atomic<std::uint64_t> checkpoint_success_total_{0};
  std::atomic<std::uint64_t> compaction_worker_tasks_submitted_total_{0};
  std::atomic<std::uint64_t> compaction_worker_tasks_completed_total_{0};
  std::uint32_t compaction_merge_min_interval_ms_{0};
  bool has_last_l0_merge_wall_{false};
  std::chrono::steady_clock::time_point last_l0_merge_wall_{};
  std::atomic<std::uint64_t> compaction_merge_throttle_sleep_ns_total_{0};
  bool compaction_sequential_sst_read_{true};
  bool compaction_worker_low_priority_thread_{true};
  SteadyClockByteTokenBucket compaction_merge_byte_tb_{};
  std::atomic<std::uint64_t> compaction_merge_byte_throttle_sleep_ns_total_{0};

  std::unique_ptr<CompactionIoExecutor> compaction_io_executor_;
  bool compaction_dedicated_io_executor_{false};
  std::uint32_t compaction_io_chunk_bytes_{0};
  std::uint32_t compaction_io_pool_threads_{0};
  bool compaction_parallel_sst_reads_{true};

  structdb::infra::IoBackendConfig wal_io_cfg_{};
  std::uint64_t wal_segment_roll_max_bytes_{0};
  bool wal_catalog_v2_{false};
  std::vector<std::string> wal_sealed_relative_;
  std::uint64_t wal_next_roll_seq_{1};

  std::thread compaction_worker_thread_{};
  mutable std::mutex compaction_worker_mu_;
  std::condition_variable compaction_worker_cv_;
  std::atomic<std::uint64_t> compaction_worker_enqueue_seq_{0};
  std::vector<CompactionWorkerJob> compaction_tasks_;
  std::size_t compaction_queue_cap_{64};
  bool compaction_worker_stop_{false};
  bool compaction_worker_joinable_{false};

  structdb::infra::LongTaskReporter* active_long_task_{nullptr};

  bool exclusive_directory_lock_{false};
#if defined(_WIN32)
  void* exclusive_lock_handle_{nullptr};
#else
  int exclusive_lock_fd_{-1};
#endif

  WalCoordinator wal_coordinator_;
  CompactionCoordinator compaction_coordinator_;
  CheckpointUndoCoordinator checkpoint_undo_coordinator_;
  WalReplayApplier wal_replay_applier_;
  RecoveryCoordinator recovery_coordinator_;
  StorageTelemetry storage_telemetry_;
};

}  // namespace structdb::storage
