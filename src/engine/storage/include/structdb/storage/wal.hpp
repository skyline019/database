#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <string>
#include <string_view>

#include "structdb/infra/file_handle.hpp"
#include "structdb/infra/io_backend.hpp"
#include "structdb/infra/io_iocp.hpp"
#include "structdb/infra/io_uring_seq.hpp"
#include "structdb/storage/byte_token_bucket.hpp"

namespace structdb::storage {

/// Documents the **WAL append pipeline** for `WalWriter` (blocking `FileWriter`, `IocpSequentialFileWriter`, and
/// `IouringSequentialFileWriter`): (1) append **little-endian `uint32_t` record length**; (2) append **payload** bytes
/// contiguously at increasing file offsets (**no reordering** across a single `append_record`); (3) when `fsync` is
/// true, invoke **`sync()`** / `fdatasync` / `FlushFileBuffers` so the prefix+payload record is durable before
/// returning. See `Docs/PHASE21.md` §21B and `structdb/infra/io_backend.hpp`.
///
/// **Compaction channel**: LSM merge materialization reads/writes only SST and temp files via `FileReader`/`FileWriter`
/// on dedicated pool threads — it does **not** call `WalWriter::append_record` or share WAL file descriptors.
struct WalPipeline {};

class WalWriter {
 public:
  bool open(const std::filesystem::path& dir, const infra::IoBackendConfig& io_config = {});
  void close();
  bool append_record(const void* data, std::size_t len, bool fsync);
  /// Flush data written to the WAL file to stable storage (when WAL is open).
  bool sync();
  const std::filesystem::path& path() const { return path_; }

  /// When **> 0**, enforces a minimum wall-clock spacing between successful fsyncs (`append_record(..., true)` and
  /// `sync()`), via `sleep_until` on a steady clock. `0` disables (default). Does not reorder writes.
  void set_fsync_min_interval_ms(std::uint32_t ms);

  /// Token-bucket refill rate for **on-disk WAL frame bytes** (`uint32_t` length prefix + payload per `append_record`).
  /// `0` disables (default). Use only from `StorageEngine` while holding **`mu_` exclusive (write) lock**; no internal locking.
  void set_append_max_bytes_per_second(std::uint64_t bytes_per_sec);
  /// Max accumulated burst capacity for the append token bucket. `0` = `max(bytes_per_sec, 65536)`.
  void set_append_burst_bytes(std::uint64_t burst_bytes);

  /// Cumulative nanoseconds slept while waiting for the append byte bucket (observability).
  std::uint64_t append_throttle_sleep_ns_total() const {
    return append_throttle_sleep_ns_total_.load(std::memory_order_relaxed);
  }
  /// Sum of successful `append_record` frame sizes (`4 + len`) since this `WalWriter` was constructed.
  std::uint64_t append_frame_bytes_committed_total() const {
    return append_frame_bytes_committed_total_.load(std::memory_order_relaxed);
  }

 private:
  enum class BackendKind { File, Iocp, IoUring };

  void throttle_before_fsync_if_configured_();
  void mark_after_successful_fsync_();
  void note_append_frame_committed_(std::uint64_t frame_bytes);

  infra::IoBackendConfig io_cfg_{};
  infra::FileWriter file_;
  infra::IocpSequentialFileWriter iocp_;
  infra::IouringSequentialFileWriter uring_;
  BackendKind backend_{BackendKind::File};
  std::filesystem::path path_;
  std::uint32_t fsync_min_interval_ms_{0};
  bool has_last_successful_fsync_{false};
  std::chrono::steady_clock::time_point last_successful_fsync_{};

  SteadyClockByteTokenBucket append_byte_tb_{};
  std::atomic<std::uint64_t> append_throttle_sleep_ns_total_{0};
  std::atomic<std::uint64_t> append_frame_bytes_committed_total_{0};
};

/// Length-prefixed WAL records (`append_record`). Replays `[start_offset, EOF)`; incomplete trailing frame ignored.
bool wal_replay_from_offset(const std::filesystem::path& wal_path, std::uint64_t start_offset,
                            const std::function<bool(std::string_view line, std::string* err)>& on_record,
                            std::string* error_out);

}  // namespace structdb::storage
