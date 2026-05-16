#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace structdb::infra {

/// Long-running work category (GUI / MDB / compaction / export share the same progress envelope).
enum class LongTaskKind : std::uint8_t {
  Unknown = 0,
  MdbScript,
  CompactionMerge,
  CompactionFlush,
  Export,
  Bench,
};

/// Lifecycle state for cooperative cancel and UI display.
enum class LongTaskStatus : std::uint8_t {
  Running = 0,
  Throttled,
  Cancelling,
  Cancelled,
  Completed,
  Failed,
};

/// Single progress sample (JSON field names align with GUI `long-task-progress` payload).
struct LongTaskProgressSnapshot {
  LongTaskKind kind{LongTaskKind::Unknown};
  LongTaskStatus status{LongTaskStatus::Running};
  std::uint64_t units_done{0};
  std::uint64_t units_total{0};  /// `0` = unknown / indeterminate denominator
  std::uint64_t bytes_done{0};
  std::uint64_t bytes_total{0};  /// `0` = unknown
  std::string detail;
  std::string task_id;
};

const char* long_task_kind_cstr(LongTaskKind k) noexcept;
const char* long_task_status_cstr(LongTaskStatus s) noexcept;
/// camelCase strings for GUI / Tauri (`mdbScript`, `running`, …).
const char* long_task_kind_api_string(LongTaskKind k) noexcept;
const char* long_task_status_api_string(LongTaskStatus s) noexcept;

/// Thread-safe cooperative cancel flag shared by producer and consumer.
class LongTaskCancelToken {
 public:
  LongTaskCancelToken();
  void request_cancel();
  bool cancel_requested() const;
  std::shared_ptr<std::atomic<bool>> shared_atomic() const;

 private:
  std::shared_ptr<std::atomic<bool>> flag_;
};

using LongTaskProgressCallback = std::function<void(const LongTaskProgressSnapshot&)>;

/// Reports `LongTaskProgressSnapshot` and polls `LongTaskCancelToken` (not owned by callbacks).
class LongTaskReporter {
 public:
  LongTaskReporter();
  explicit LongTaskReporter(LongTaskKind kind);

  void set_kind(LongTaskKind kind);
  LongTaskKind kind() const { return kind_; }

  void set_task_id(std::string id);
  void set_detail(std::string detail);

  void set_progress_callback(LongTaskProgressCallback cb);
  void bind_cancel_token(std::shared_ptr<LongTaskCancelToken> token);
  std::shared_ptr<LongTaskCancelToken> cancel_token() const;

  bool cancel_requested() const;
  /// When cancel is set, emits `Cancelling` once and returns true.
  bool poll_cancel_and_report_cancelling();

  void report(LongTaskStatus status, std::uint64_t units_done, std::uint64_t units_total = 0);
  void report_bytes(LongTaskStatus status, std::uint64_t bytes_done, std::uint64_t bytes_total = 0);
  void emit_snapshot(LongTaskProgressSnapshot snap);

 private:
  void fill_and_emit_(LongTaskProgressSnapshot snap);

  LongTaskKind kind_{LongTaskKind::Unknown};
  std::string task_id_;
  std::string detail_;
  std::uint64_t units_done_{0};
  std::uint64_t units_total_{0};
  std::uint64_t bytes_done_{0};
  std::uint64_t bytes_total_{0};
  std::shared_ptr<LongTaskCancelToken> cancel_;
  LongTaskProgressCallback on_progress_;
  bool cancelling_reported_{false};
};

}  // namespace structdb::infra
