#include "structdb/storage/wal.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <thread>
#include <vector>

namespace structdb::storage {

namespace {

bool read_u32_le(const std::uint8_t*& p, const std::uint8_t* end, std::uint32_t* out) {
  if (static_cast<std::size_t>(end - p) < 4) return false;
  std::uint32_t v = 0;
  for (int i = 0; i < 4; ++i) v |= static_cast<std::uint32_t>(*p++) << (8 * i);
  *out = v;
  return true;
}

}  // namespace

bool WalWriter::open(const std::filesystem::path& dir, const infra::IoBackendConfig& io_config) {
  close();
  io_cfg_ = io_config;
  std::filesystem::create_directories(dir);
  path_ = dir / "wal.log";
#if defined(__linux__) && defined(STRUCTDB_HAVE_IO_URING)
  if (io_cfg_.kind == infra::IoBackendKind::IoUringAsync) {
    if (uring_.open(path_, true)) {
      backend_ = BackendKind::IoUring;
      return true;
    }
  }
#endif
#if defined(_WIN32) && defined(STRUCTDB_HAVE_IOCP)
  if (io_cfg_.kind == infra::IoBackendKind::IocpAsync) {
    if (iocp_.open(path_, true)) {
      backend_ = BackendKind::Iocp;
      return true;
    }
  }
#else
  (void)iocp_;
#endif
  backend_ = BackendKind::File;
  return file_.open(path_, true);
}

void WalWriter::close() {
  iocp_.close();
  uring_.close();
  file_.close();
  backend_ = BackendKind::File;
}

void WalWriter::set_fsync_min_interval_ms(std::uint32_t ms) { fsync_min_interval_ms_ = ms; }

void WalWriter::set_append_max_bytes_per_second(std::uint64_t bytes_per_sec) {
  append_byte_tb_.set_max_bytes_per_second(bytes_per_sec);
}

void WalWriter::set_append_burst_bytes(std::uint64_t burst_bytes) {
  append_byte_tb_.set_burst_bytes(burst_bytes);
}

void WalWriter::note_append_frame_committed_(std::uint64_t frame_bytes) {
  append_frame_bytes_committed_total_.fetch_add(frame_bytes, std::memory_order_relaxed);
}

void WalWriter::throttle_before_fsync_if_configured_() {
  if (fsync_min_interval_ms_ == 0) return;
  if (!has_last_successful_fsync_) return;
  const auto gap = std::chrono::milliseconds(static_cast<int>(fsync_min_interval_ms_));
  const auto now = std::chrono::steady_clock::now();
  const auto next_ok = last_successful_fsync_ + gap;
  if (now < next_ok) std::this_thread::sleep_until(next_ok);
}

void WalWriter::mark_after_successful_fsync_() {
  last_successful_fsync_ = std::chrono::steady_clock::now();
  has_last_successful_fsync_ = true;
}

bool WalWriter::sync() {
  throttle_before_fsync_if_configured_();
  bool ok = false;
  switch (backend_) {
    case BackendKind::Iocp:
      ok = iocp_.sync();
      break;
    case BackendKind::IoUring:
      ok = uring_.sync();
      break;
    case BackendKind::File:
    default:
      if (!file_.is_open()) return false;
      ok = file_.sync();
      break;
  }
  if (ok) mark_after_successful_fsync_();
  return ok;
}

bool WalWriter::append_record(const void* data, std::size_t len, bool fsync) {
  const std::uint64_t frame = 4ull + static_cast<std::uint64_t>(len);
  append_byte_tb_.throttle(frame, &append_throttle_sleep_ns_total_);
  bool ok = false;
  struct RefundGuard {
    WalWriter* w;
    std::uint64_t f;
    bool* o;
    ~RefundGuard() {
      if (w && !*o) w->append_byte_tb_.refund(f);
    }
  } guard{this, frame, &ok};
  std::uint32_t le = static_cast<std::uint32_t>(len);
  switch (backend_) {
    case BackendKind::Iocp: {
      if (!iocp_.is_open()) return false;
      if (!iocp_.write_all(&le, sizeof(le))) return false;
      if (!iocp_.write_all(data, len)) return false;
      if (fsync) {
        throttle_before_fsync_if_configured_();
        if (!iocp_.sync()) return false;
        mark_after_successful_fsync_();
      }
      break;
    }
    case BackendKind::IoUring: {
      if (!uring_.is_open()) return false;
      if (!uring_.write_all(&le, sizeof(le))) return false;
      if (!uring_.write_all(data, len)) return false;
      if (fsync) {
        throttle_before_fsync_if_configured_();
        if (!uring_.sync()) return false;
        mark_after_successful_fsync_();
      }
      break;
    }
    case BackendKind::File:
    default:
      if (!file_.is_open()) return false;
      if (!file_.write_all(&le, sizeof(le))) return false;
      if (!file_.write_all(data, len)) return false;
      if (fsync) {
        throttle_before_fsync_if_configured_();
        if (!file_.sync()) return false;
        mark_after_successful_fsync_();
      }
      break;
  }
  ok = true;
  note_append_frame_committed_(frame);
  return true;
}

bool wal_replay_from_offset(const std::filesystem::path& wal_path, std::uint64_t start_offset,
                            const std::function<bool(std::string_view line, std::string* err)>& on_record,
                            std::string* error_out) {
  if (!std::filesystem::exists(wal_path)) return true;
  infra::FileReader reader(wal_path);
  if (!reader.is_open()) {
    if (error_out) *error_out = "wal replay: open";
    return false;
  }
  std::vector<std::uint8_t> buf;
  if (!reader.read_all(buf)) {
    if (error_out) *error_out = "wal replay: read";
    return false;
  }
  if (start_offset > buf.size()) {
    if (error_out) *error_out = "wal replay: bad start offset";
    return false;
  }
  const std::uint8_t* p = buf.data() + start_offset;
  const std::uint8_t* end = buf.data() + buf.size();
  while (p < end) {
    std::uint32_t reclen = 0;
    if (!read_u32_le(p, end, &reclen)) break;
    if (reclen > static_cast<std::uint32_t>(end - p)) {
      /* incomplete record (crash mid-frame) */
      break;
    }
    const std::string_view line(reinterpret_cast<const char*>(p), reclen);
    p += reclen;
    std::string err;
    if (!on_record(line, &err)) {
      if (error_out && !err.empty()) *error_out = std::move(err);
      return false;
    }
  }
  return true;
}

}  // namespace structdb::storage
