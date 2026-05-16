#pragma once

#include <cstddef>
#include <cstdint>

namespace structdb::storage {

/// Read-only storage pressure signals (phase 14) for Scheduler / budget hooks via Facade.
struct StoragePressureSnapshot {
  std::size_t l0_files{0};
  std::uint64_t wal_bytes{0};
  std::uint64_t undo_bytes{0};
  std::uint64_t manifest_version{0};
  /// Phase 21C: compaction worker queue depth (0 when worker not started).
  std::uint32_t compaction_worker_queue_depth{0};
  /// Phase 21C: configured worker queue capacity (0 when worker not started).
  std::uint32_t compaction_worker_queue_cap{0};
  /// Phase 21C: `flush_memtable` left a deferred L0 drain pending (`l0_compact_defer_after_flush`).
  bool pending_deferred_l0_compact{false};
  /// Phase 36: pending `Engine::kv_put` tasks when async queue enabled (`EngineConfigSnapshot::kv_put_async_queue_depth>0`).
  std::uint32_t facade_kv_put_queue_depth{0};
  /// Phase 36: configured async `kv_put` queue capacity (`0` when disabled).
  std::uint32_t facade_kv_put_queue_cap{0};

  /// 自进程启动以来：`WalWriter` 成功提交的 WAL 帧字节累计（每帧 `4 + record_payload_len`）。
  std::uint64_t wal_append_frame_bytes_committed_total{0};
  /// 自进程启动以来：append 字节令牌桶导致的睡眠纳秒累计（`WalWriter`）。
  std::uint64_t wal_append_throttle_sleep_ns_total{0};
  /// 自 `StorageEngine` 构造以来：L0 merge **最小间隔**策略导致的睡眠纳秒累计（`compaction_merge_min_interval_ms`）。
  std::uint64_t compaction_merge_throttle_sleep_ns_total{0};
  /// 自 `StorageEngine` 构造以来：merge **字节令牌桶**导致的睡眠纳秒累计（`compaction_merge_max_bytes_per_second`）。
  std::uint64_t compaction_merge_byte_throttle_sleep_ns_total{0};

  /// 自进程启动以来（跨 `open`/`close` 累计）：成功追加的 WAL 记录条数（每条 `WalWriter::append_record` 成功一次 +1）。
  std::uint64_t wal_append_record_calls_total{0};
  /// 自进程启动以来：WAL 刷盘次数（`append_record(..., fsync=true)` 内 fsync + 独立 `wal_.sync()`，如 `wal_sync` / 段滚动前 sync）。
  std::uint64_t wal_fsync_calls_total{0};
  /// 自进程启动以来：成功完成的 `flush_memtable` 次数。
  std::uint64_t flush_memtable_success_total{0};
  /// 自进程启动以来：成功完成的 `checkpoint` 次数。
  std::uint64_t checkpoint_success_total{0};
  /// 自进程启动以来：成功完成的 L0/L1/… **合并**次数（与 `StorageEngine::compaction_merge_count()` 一致）。
  std::uint64_t compaction_merge_success_total{0};
  /// 自 compaction worker 启动以来：入队任务数（当前仅 `enqueue_drain_l0_compaction_and_wait` 入队；worker 未启动时为 0）。
  std::uint64_t compaction_worker_tasks_submitted_total{0};
  /// 自 compaction worker 启动以来：已执行完成的任务数（与入队一一对应；内联 `drain_pending_l0_compactions` 不计入）。
  std::uint64_t compaction_worker_tasks_completed_total{0};
};

}  // namespace structdb::storage
