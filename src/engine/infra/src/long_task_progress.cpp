#include "structdb/infra/long_task_progress.hpp"

namespace structdb::infra {

const char* long_task_kind_cstr(LongTaskKind k) noexcept {
  switch (k) {
    case LongTaskKind::MdbScript:
      return "mdb_script";
    case LongTaskKind::CompactionMerge:
      return "compaction_merge";
    case LongTaskKind::CompactionFlush:
      return "compaction_flush";
    case LongTaskKind::Export:
      return "export";
    case LongTaskKind::Bench:
      return "bench";
    case LongTaskKind::Unknown:
    default:
      return "unknown";
  }
}

const char* long_task_status_cstr(LongTaskStatus s) noexcept {
  switch (s) {
    case LongTaskStatus::Running:
      return "running";
    case LongTaskStatus::Throttled:
      return "throttled";
    case LongTaskStatus::Cancelling:
      return "cancelling";
    case LongTaskStatus::Cancelled:
      return "cancelled";
    case LongTaskStatus::Completed:
      return "completed";
    case LongTaskStatus::Failed:
      return "failed";
    default:
      return "unknown";
  }
}

const char* long_task_kind_api_string(LongTaskKind k) noexcept {
  switch (k) {
    case LongTaskKind::MdbScript:
      return "mdbScript";
    case LongTaskKind::CompactionMerge:
      return "compactionMerge";
    case LongTaskKind::CompactionFlush:
      return "compactionFlush";
    case LongTaskKind::Export:
      return "export";
    case LongTaskKind::Bench:
      return "bench";
    case LongTaskKind::Unknown:
    default:
      return "unknown";
  }
}

const char* long_task_status_api_string(LongTaskStatus s) noexcept { return long_task_status_cstr(s); }

LongTaskCancelToken::LongTaskCancelToken() : flag_(std::make_shared<std::atomic<bool>>(false)) {}

void LongTaskCancelToken::request_cancel() { flag_->store(true, std::memory_order_release); }

bool LongTaskCancelToken::cancel_requested() const { return flag_->load(std::memory_order_acquire); }

std::shared_ptr<std::atomic<bool>> LongTaskCancelToken::shared_atomic() const { return flag_; }

LongTaskReporter::LongTaskReporter() : cancel_(std::make_shared<LongTaskCancelToken>()) {}

LongTaskReporter::LongTaskReporter(LongTaskKind kind) : kind_(kind), cancel_(std::make_shared<LongTaskCancelToken>()) {}

void LongTaskReporter::set_kind(LongTaskKind kind) { kind_ = kind; }

void LongTaskReporter::set_task_id(std::string id) { task_id_ = std::move(id); }

void LongTaskReporter::set_detail(std::string detail) { detail_ = std::move(detail); }

void LongTaskReporter::set_progress_callback(LongTaskProgressCallback cb) { on_progress_ = std::move(cb); }

void LongTaskReporter::bind_cancel_token(std::shared_ptr<LongTaskCancelToken> token) {
  if (token) cancel_ = std::move(token);
}

std::shared_ptr<LongTaskCancelToken> LongTaskReporter::cancel_token() const { return cancel_; }

bool LongTaskReporter::cancel_requested() const { return cancel_ && cancel_->cancel_requested(); }

bool LongTaskReporter::poll_cancel_and_report_cancelling() {
  if (!cancel_requested()) return false;
  if (!cancelling_reported_) {
    cancelling_reported_ = true;
    report(LongTaskStatus::Cancelling, units_done_, units_total_);
  }
  return true;
}

void LongTaskReporter::report(LongTaskStatus status, std::uint64_t units_done, std::uint64_t units_total) {
  units_done_ = units_done;
  if (units_total > 0) units_total_ = units_total;
  LongTaskProgressSnapshot snap;
  snap.kind = kind_;
  snap.status = status;
  snap.units_done = units_done_;
  snap.units_total = units_total_;
  snap.bytes_done = bytes_done_;
  snap.bytes_total = bytes_total_;
  fill_and_emit_(std::move(snap));
}

void LongTaskReporter::report_bytes(LongTaskStatus status, std::uint64_t bytes_done, std::uint64_t bytes_total) {
  bytes_done_ = bytes_done;
  if (bytes_total > 0) bytes_total_ = bytes_total;
  LongTaskProgressSnapshot snap;
  snap.kind = kind_;
  snap.status = status;
  snap.units_done = units_done_;
  snap.units_total = units_total_;
  snap.bytes_done = bytes_done_;
  snap.bytes_total = bytes_total_;
  fill_and_emit_(std::move(snap));
}

void LongTaskReporter::emit_snapshot(LongTaskProgressSnapshot snap) {
  if (snap.units_done > 0 || snap.units_total > 0) {
    units_done_ = snap.units_done;
    if (snap.units_total > 0) units_total_ = snap.units_total;
  }
  if (snap.bytes_done > 0 || snap.bytes_total > 0) {
    bytes_done_ = snap.bytes_done;
    if (snap.bytes_total > 0) bytes_total_ = snap.bytes_total;
  }
  fill_and_emit_(std::move(snap));
}

void LongTaskReporter::fill_and_emit_(LongTaskProgressSnapshot snap) {
  if (snap.kind == LongTaskKind::Unknown) snap.kind = kind_;
  if (snap.task_id.empty()) snap.task_id = task_id_;
  if (snap.detail.empty()) snap.detail = detail_;
  if (on_progress_) on_progress_(snap);
}

}  // namespace structdb::infra
