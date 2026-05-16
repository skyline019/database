#include "structdb/storage/checkpoint_undo_coordinator.hpp"

#include "structdb/storage/checkpoint.hpp"
#include "structdb/storage/storage_engine.hpp"
#include "structdb/storage/wal_coordinator.hpp"

#include "structdb/infra/file_handle.hpp"
#include "structdb/storage/undo_log.hpp"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <string>
#include <vector>

#include "storage_engine_detail.hpp"

namespace sed = structdb::storage::storage_engine_detail;

namespace structdb::storage {

bool CheckpointUndoCoordinator::persist_undo_segments_for_flush_unlocked(std::string* err) {
  if (engine_.undo_catalog_v2_) {
    return sed::persist_undo_segments_metadata_v2(engine_.dir_, engine_.undo_next_roll_seq_, engine_.undo_sealed_relative_,
                                                    err);
  }
  return true;
}

std::uint64_t CheckpointUndoCoordinator::undo_logical_stream_total_bytes_unlocked() const {
  std::uint64_t o = 0;
  for (const auto& rel : engine_.undo_sealed_relative_) {
    o += sed::file_size_u64_or_zero(engine_.dir_ / rel);
  }
  o += sed::file_size_u64_or_zero(engine_.dir_ / "undo.log");
  return o;
}

bool CheckpointUndoCoordinator::undo_consume_logical_prefix_unlocked(std::uint64_t prefix_bytes, std::string* error_out) {
  if (prefix_bytes == 0) return true;
  engine_.undo_.close();
  auto reopen_undo = [&]() -> bool {
    if (!engine_.undo_.open(engine_.dir_)) {
      if (error_out) *error_out = "undo prefix: reopen active";
      return false;
    }
    return true;
  };
  std::uint64_t remaining = prefix_bytes;
  while (remaining > 0 && !engine_.undo_sealed_relative_.empty()) {
    const std::string rel = engine_.undo_sealed_relative_.front();
    const auto full = engine_.dir_ / rel;
    const std::uint64_t sz = sed::file_size_u64_or_zero(full);
    if (remaining >= sz) {
      std::error_code ec;
      std::filesystem::remove(full, ec);
      if (ec) {
        if (error_out) *error_out = std::string("undo prefix: remove sealed: ") + ec.message();
        (void)reopen_undo();
        return false;
      }
      engine_.undo_sealed_relative_.erase(engine_.undo_sealed_relative_.begin());
      remaining -= sz;
      if (engine_.undo_catalog_v2_) {
        if (!sed::persist_undo_segments_metadata_v2(engine_.dir_, engine_.undo_next_roll_seq_, engine_.undo_sealed_relative_,
                                                    error_out)) {
          (void)reopen_undo();
          return false;
        }
      }
    } else {
      if (!UndoLog::truncate_prefix_at_path(full, remaining, error_out)) {
        (void)reopen_undo();
        return false;
      }
      remaining = 0;
      break;
    }
  }
  if (remaining > 0) {
    if (!UndoLog::truncate_prefix_at_path(engine_.dir_ / "undo.log", remaining, error_out)) {
      (void)reopen_undo();
      return false;
    }
  }
  return reopen_undo();
}

bool CheckpointUndoCoordinator::undo_roll_to_new_segment_unlocked(std::string* error_out) {
  if (engine_.undo_segment_roll_max_bytes_ == 0) return true;
  engine_.undo_catalog_v2_ = true;
  std::error_code ec;
  std::filesystem::create_directories(engine_.dir_ / "undo" / "archive", ec);
  (void)ec;
  const std::uint64_t id = engine_.undo_next_roll_seq_++;
  const std::string rel = std::string("undo/archive/") + std::to_string(id) + ".log";
  engine_.undo_.close();
  const auto from = engine_.dir_ / "undo.log";
  const auto to = engine_.dir_ / rel;
  std::filesystem::rename(from, to, ec);
  if (ec) {
    --engine_.undo_next_roll_seq_;
    if (!engine_.undo_.open(engine_.dir_)) {
      if (error_out) *error_out = "undo roll: reopen after rename failure";
      return false;
    }
    if (error_out) *error_out = std::string("undo roll: rename: ") + ec.message();
    return false;
  }
  engine_.undo_sealed_relative_.push_back(rel);
  if (!sed::persist_undo_segments_metadata_v2(engine_.dir_, engine_.undo_next_roll_seq_, engine_.undo_sealed_relative_,
                                              error_out)) {
    engine_.undo_sealed_relative_.pop_back();
    --engine_.undo_next_roll_seq_;
    std::filesystem::rename(to, from, ec);
    if (!engine_.undo_.open(engine_.dir_)) {
      if (error_out && error_out->empty()) *error_out = "undo roll: reopen after v2 persist failure";
      return false;
    }
    if (error_out && error_out->empty()) *error_out = "undo roll: persist v2";
    return false;
  }
  if (!engine_.undo_.open(engine_.dir_)) {
    if (error_out) *error_out = "undo roll: open new undo.log";
    return false;
  }
  engine_.undo_segment_count_observed_ = static_cast<std::uint32_t>(engine_.undo_sealed_relative_.size() + 1u);
  return true;
}

bool CheckpointUndoCoordinator::undo_maybe_roll_after_append_unlocked(std::string* error_out) {
  if (engine_.undo_segment_roll_max_bytes_ == 0) return true;
  if (!engine_.undo_.sync()) {
    if (error_out) *error_out = "undo roll: sync before size check";
    return false;
  }
  if (sed::file_size_u64_or_zero(engine_.dir_ / "undo.log") < engine_.undo_segment_roll_max_bytes_) return true;
  return undo_roll_to_new_segment_unlocked(error_out);
}

bool CheckpointUndoCoordinator::load_undo_segments_catalog_for_open(std::string* error_out) {
  engine_.undo_catalog_v2_ = false;
  engine_.undo_sealed_relative_.clear();
  engine_.undo_next_roll_seq_ = 1;
  const auto p = engine_.dir_ / "undo.segments";
  if (!std::filesystem::exists(p)) return true;
  std::ifstream in(p);
  if (!in) {
    if (error_out) *error_out = "undo.segments open";
    return false;
  }
  std::string vline;
  if (!std::getline(in, vline)) {
    if (error_out) *error_out = "undo.segments empty";
    return false;
  }
  if (!vline.empty() && vline.back() == '\r') vline.pop_back();
  unsigned long ver = 0;
  try {
    ver = std::stoul(vline);
  } catch (...) {
    if (error_out) *error_out = "undo.segments: bad version line";
    return false;
  }
  if (ver == 1u) return true;
  if (ver != 2u) {
    if (error_out) *error_out = "undo.segments: unsupported format (need v1 or v2)";
    return false;
  }
  std::string line2, line3;
  if (!std::getline(in, line2) || !std::getline(in, line3)) {
    if (error_out) *error_out = "undo.segments v2: truncated header";
    return false;
  }
  if (!line2.empty() && line2.back() == '\r') line2.pop_back();
  if (!line3.empty() && line3.back() == '\r') line3.pop_back();
  std::uint64_t next_roll = 1;
  std::size_t nsealed = 0;
  try {
    next_roll = static_cast<std::uint64_t>(std::stoull(line2));
    nsealed = static_cast<std::size_t>(std::stoul(line3));
  } catch (...) {
    if (error_out) *error_out = "undo.segments v2: parse next_roll / count";
    return false;
  }
  if (nsealed > 65536u) {
    if (error_out) *error_out = "undo.segments v2: sealed count too large";
    return false;
  }
  std::vector<std::string> sealed;
  sealed.reserve(nsealed);
  for (std::size_t i = 0; i < nsealed; ++i) {
    std::string rel;
    if (!std::getline(in, rel)) {
      if (error_out) *error_out = "undo.segments v2: missing sealed path line";
      return false;
    }
    if (!rel.empty() && rel.back() == '\r') rel.pop_back();
    if (!sed::undo_segment_rel_path_safe(rel)) {
      if (error_out) *error_out = "undo.segments v2: illegal sealed path";
      return false;
    }
    const auto full = engine_.dir_ / rel;
    if (!std::filesystem::exists(full)) {
      if (error_out) *error_out = "undo.segments v2: sealed segment file missing";
      return false;
    }
    sealed.push_back(std::move(rel));
  }
  engine_.undo_catalog_v2_ = true;
  engine_.undo_next_roll_seq_ = next_roll == 0 ? 1 : next_roll;
  engine_.undo_sealed_relative_ = std::move(sealed);
  return true;
}

std::uint64_t CheckpointUndoCoordinator::compute_undo_recyclable_prefix_bytes_unlocked() const {
  const std::uint64_t sz = undo_logical_stream_total_bytes_unlocked();
  if (engine_.undo_stack_.empty()) return sz;
  return engine_.undo_stack_.front().frame_start_byte;
}

void CheckpointUndoCoordinator::fill_checkpoint_undo_safe_prefix_unlocked(CheckpointState* ck) const {
  if (!ck) return;
  const std::uint64_t sz = undo_logical_stream_total_bytes_unlocked();
  std::uint64_t p = compute_undo_recyclable_prefix_bytes_unlocked();
  if (p > sz) p = sz;
  ck->undo_log_safe_prefix_bytes = p;
}

bool CheckpointUndoCoordinator::undo_truncate_recyclable_prefix_unlocked(std::string* error_out) {
  const std::uint64_t p = compute_undo_recyclable_prefix_bytes_unlocked();
  if (p == 0) return true;
  if (!undo_consume_logical_prefix_unlocked(p, error_out)) return false;
  for (auto& e : engine_.undo_stack_) {
    if (e.frame_start_byte < p) {
      if (error_out) *error_out = "undo truncate_prefix stack offset";
      return false;
    }
    e.frame_start_byte -= p;
  }
  engine_.undo_segment_count_observed_ =
      engine_.undo_catalog_v2_ ? static_cast<std::uint32_t>(engine_.undo_sealed_relative_.size() + 1u) : 1u;
  return true;
}

bool CheckpointUndoCoordinator::rebuild_undo_stack_from_undo_log_unlocked(std::string* error_out) {
  engine_.undo_stack_.clear();
  std::vector<std::uint8_t> buf;
  for (const auto& rel : engine_.undo_sealed_relative_) {
    const auto pth = engine_.dir_ / rel;
    if (!std::filesystem::exists(pth)) {
      if (error_out) *error_out = "undo rebuild: sealed segment missing";
      return false;
    }
    infra::FileReader r(pth);
    if (!r.is_open()) {
      if (error_out) *error_out = "undo rebuild: sealed open";
      return false;
    }
    std::vector<std::uint8_t> chunk;
    if (!r.read_all(chunk)) {
      if (error_out) *error_out = "undo rebuild: sealed read";
      return false;
    }
    buf.insert(buf.end(), chunk.begin(), chunk.end());
  }
  const auto active = engine_.dir_ / "undo.log";
  if (std::filesystem::exists(active)) {
    infra::FileReader r2(active);
    if (!r2.is_open()) {
      if (error_out) *error_out = "undo rebuild: active open";
      return false;
    }
    std::vector<std::uint8_t> tail;
    if (!r2.read_all(tail)) {
      if (error_out) *error_out = "undo rebuild: active read";
      return false;
    }
    buf.insert(buf.end(), tail.begin(), tail.end());
  }
  if (buf.empty()) return true;
  const std::uint8_t* p = buf.data();
  const std::uint8_t* end = buf.data() + buf.size();
  while (p < end) {
    const std::uint64_t frame_start = static_cast<std::uint64_t>(p - buf.data());
    std::uint32_t outer = 0;
    if (!sed::read_u32_le(p, end, &outer)) break;
    if (outer > static_cast<std::uint32_t>(end - p)) {
      if (error_out) *error_out = "undo rebuild: truncated outer";
      return false;
    }
    const std::uint8_t* q = p;
    const std::uint8_t* qend = p + outer;
    p = qend;
    constexpr unsigned char kMagic[] = {'S', 'T', 'R', 'D', 'B', 'U', 'V', '1'};
    if (static_cast<std::size_t>(qend - q) < sizeof(kMagic)) {
      if (error_out) *error_out = "undo rebuild: short inner";
      return false;
    }
    if (std::memcmp(q, kMagic, sizeof(kMagic)) != 0) {
      if (error_out) *error_out = "undo rebuild: bad magic";
      return false;
    }
    q += sizeof(kMagic);
    std::uint32_t klen = 0;
    std::uint32_t vlen = 0;
    if (!sed::read_u32_le(q, qend, &klen)) {
      if (error_out) *error_out = "undo rebuild: klen";
      return false;
    }
    if (static_cast<std::size_t>(qend - q) < klen) {
      if (error_out) *error_out = "undo rebuild: key";
      return false;
    }
    std::string key(reinterpret_cast<const char*>(q), klen);
    q += klen;
    if (!sed::read_u32_le(q, qend, &vlen)) {
      if (error_out) *error_out = "undo rebuild: vlen";
      return false;
    }
    if (static_cast<std::size_t>(qend - q) < vlen) {
      if (error_out) *error_out = "undo rebuild: val";
      return false;
    }
    std::string val(reinterpret_cast<const char*>(q), vlen);
    q += vlen;
    if (q != qend) {
      if (error_out) *error_out = "undo rebuild: inner trailing";
      return false;
    }
    engine_.undo_stack_.push_back(StorageEngine::UndoStackEntry{std::move(key), std::move(val), frame_start});
  }
  return true;
}

namespace {
constexpr bool kAppendRedoMirrorWal = false;
}  // namespace

bool CheckpointUndoCoordinator::rollback_one_undo_frame_unlocked(std::string* error_out) {
  if (engine_.undo_stack_.empty()) return false;
  const auto pr = engine_.undo_stack_.back();
  engine_.undo_stack_.pop_back();
  const std::string line = pr.key + "=" + pr.prev_raw + "\n";
  if (!engine_.wal_.append_record(line.data(), line.size(), false)) {
    engine_.undo_stack_.push_back(pr);
    if (error_out) *error_out = "wal append (rollback)";
    return false;
  }
  engine_.wal_coordinator_.observe_append_unlocked(false);
  if (!engine_.wal_coordinator_.maybe_roll_after_append_unlocked()) {
    engine_.undo_stack_.push_back(pr);
    if (error_out) *error_out = "wal roll (rollback)";
    return false;
  }
  if (kAppendRedoMirrorWal) {
    const std::string redo_rec = std::string("PUT ") + line;
    if (!engine_.redo_.append(redo_rec.data(), redo_rec.size(), false)) {
      engine_.undo_stack_.push_back(pr);
      if (error_out) *error_out = "redo append (rollback)";
      return false;
    }
  }
  engine_.mem_mgr_.active().put(pr.key, pr.prev_raw);
  engine_.observe_stored_commit_seq_(pr.prev_raw);
  return true;
}

}  // namespace structdb::storage
