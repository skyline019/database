#include "structdb/storage/wal_coordinator.hpp"

#include "structdb/storage/storage_engine.hpp"

#include "structdb/infra/file_handle.hpp"
#include "structdb/storage/wal.hpp"

#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <string>
#include <vector>

#include "storage_engine_detail.hpp"

namespace sed = structdb::storage::storage_engine_detail;

namespace structdb::storage {

bool WalCoordinator::persist_segments_for_flush_unlocked(std::string* err) {
  if (engine_.wal_catalog_v2_) {
    return sed::persist_wal_segments_metadata_v2(engine_.dir_, engine_.wal_next_roll_seq_, engine_.wal_sealed_relative_,
                                                   err);
  }
  return sed::persist_wal_segments_metadata_v1(engine_.dir_, 1u, err);
}

bool WalCoordinator::load_segments_catalog_for_open(std::string* error_out) {
  engine_.wal_catalog_v2_ = false;
  engine_.wal_sealed_relative_.clear();
  engine_.wal_next_roll_seq_ = 1;
  const auto p = engine_.dir_ / "wal.segments";
  if (!std::filesystem::exists(p)) return true;
  std::ifstream in(p);
  if (!in) {
    if (error_out) *error_out = "wal.segments open";
    return false;
  }
  std::string vline;
  if (!std::getline(in, vline)) {
    if (error_out) *error_out = "wal.segments empty";
    return false;
  }
  if (!vline.empty() && vline.back() == '\r') vline.pop_back();
  unsigned long ver = 0;
  try {
    ver = std::stoul(vline);
  } catch (...) {
    if (error_out) *error_out = "wal.segments: bad version line";
    return false;
  }
  if (ver == 1u) return true;
  if (ver != 2u) {
    if (error_out) *error_out = "wal.segments: unsupported format (need v1 or v2)";
    return false;
  }
  std::string line2, line3;
  if (!std::getline(in, line2) || !std::getline(in, line3)) {
    if (error_out) *error_out = "wal.segments v2: truncated header";
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
    if (error_out) *error_out = "wal.segments v2: parse next_roll / count";
    return false;
  }
  if (nsealed > 65536u) {
    if (error_out) *error_out = "wal.segments v2: sealed count too large";
    return false;
  }
  std::vector<std::string> sealed;
  sealed.reserve(nsealed);
  for (std::size_t i = 0; i < nsealed; ++i) {
    std::string rel;
    if (!std::getline(in, rel)) {
      if (error_out) *error_out = "wal.segments v2: missing sealed path line";
      return false;
    }
    if (!rel.empty() && rel.back() == '\r') rel.pop_back();
    if (!sed::wal_segment_rel_path_safe(rel)) {
      if (error_out) *error_out = "wal.segments v2: illegal sealed path";
      return false;
    }
    const auto full = engine_.dir_ / rel;
    if (!std::filesystem::exists(full)) {
      if (error_out) *error_out = "wal.segments v2: sealed segment file missing";
      return false;
    }
    sealed.push_back(std::move(rel));
  }
  engine_.wal_catalog_v2_ = true;
  engine_.wal_next_roll_seq_ = next_roll == 0 ? 1 : next_roll;
  engine_.wal_sealed_relative_ = std::move(sealed);
  return true;
}

void WalCoordinator::observe_append_unlocked(bool record_fsync) {
  engine_.wal_append_record_calls_total_.fetch_add(1, std::memory_order_relaxed);
  if (record_fsync) engine_.wal_fsync_calls_total_.fetch_add(1, std::memory_order_relaxed);
}

void WalCoordinator::observe_fsync_unlocked() {
  engine_.wal_fsync_calls_total_.fetch_add(1, std::memory_order_relaxed);
}

bool WalCoordinator::roll_to_new_segment_unlocked(std::string* error_out) {
  if (engine_.wal_segment_roll_max_bytes_ == 0) return true;
  if (!engine_.wal_.sync()) {
    if (error_out) *error_out = "wal roll: sync";
    return false;
  }
  observe_fsync_unlocked();
  engine_.wal_.close();
  std::error_code ec;
  std::filesystem::create_directories(engine_.dir_ / "wal" / "archive", ec);
  (void)ec;
  const std::uint64_t id = engine_.wal_next_roll_seq_++;
  std::ostringstream rel_os;
  rel_os << "wal/archive/" << std::setw(6) << std::setfill('0') << id << ".log";
  const std::string rel = rel_os.str();
  const auto from = engine_.dir_ / "wal.log";
  const auto to = engine_.dir_ / rel;
  std::filesystem::rename(from, to, ec);
  if (ec) {
    --engine_.wal_next_roll_seq_;
    if (!engine_.wal_.open(engine_.dir_, engine_.wal_io_cfg_)) {
      if (error_out) *error_out = "wal roll: reopen after rename failure";
      return false;
    }
    if (error_out) *error_out = "wal roll: rename wal.log";
    return false;
  }
  engine_.wal_sealed_relative_.push_back(rel);
  engine_.wal_catalog_v2_ = true;
  if (!sed::persist_wal_segments_metadata_v2(engine_.dir_, engine_.wal_next_roll_seq_, engine_.wal_sealed_relative_,
                                               error_out)) {
    std::filesystem::rename(to, from, ec);
    engine_.wal_sealed_relative_.pop_back();
    --engine_.wal_next_roll_seq_;
    if (!engine_.wal_.open(engine_.dir_, engine_.wal_io_cfg_)) {
      if (error_out && error_out->empty()) *error_out = "wal roll: reopen after v2 persist failure";
      return false;
    }
    if (error_out && error_out->empty()) *error_out = "wal roll: persist v2";
    return false;
  }
  if (!engine_.wal_.open(engine_.dir_, engine_.wal_io_cfg_)) {
    if (error_out) *error_out = "wal roll: open new wal.log";
    return false;
  }
  engine_.wal_segment_count_observed_ = static_cast<std::uint32_t>(engine_.wal_sealed_relative_.size() + 1u);
  return true;
}

bool WalCoordinator::maybe_roll_after_append_unlocked() {
  if (engine_.wal_segment_roll_max_bytes_ == 0) return true;
  if (sed::file_size_u64_or_zero(engine_.wal_.path()) < engine_.wal_segment_roll_max_bytes_) return true;
  std::string err;
  return roll_to_new_segment_unlocked(&err);
}

bool WalCoordinator::try_trim_prefix_through_checkpoint_unlocked(std::string* error_out) {
  CheckpointState ck{};
  std::string ckr;
  if (!engine_.ckpt_.read_latest(engine_.dir_, &ck, &ckr)) {
    if (error_out) *error_out = ckr.empty() ? "wal trim: missing or unreadable checkpoint" : ckr;
    return false;
  }
  if (ck.wal_offset == 0) return true;
  const auto wal_path = engine_.wal_.path();
  infra::FileReader reader(wal_path);
  if (!reader.is_open()) {
    if (error_out) *error_out = "wal trim: wal read open";
    return false;
  }
  std::vector<std::uint8_t> buf;
  if (!reader.read_all(buf)) {
    if (error_out) *error_out = "wal trim: wal read";
    return false;
  }
  if (ck.wal_offset > buf.size()) {
    if (error_out) *error_out = "wal trim: checkpoint wal_offset past EOF";
    return false;
  }
  engine_.wal_.close();
  {
    infra::FileWriter w(wal_path, false);
    if (!w.is_open()) {
      if (!engine_.wal_.open(engine_.dir_, engine_.wal_io_cfg_)) {
        if (error_out) *error_out = "wal trim: wal reopen after failed rewrite";
        return false;
      }
      if (error_out) *error_out = "wal trim: wal rewrite open";
      return false;
    }
    const std::uint8_t* tail = buf.data() + static_cast<std::size_t>(ck.wal_offset);
    const std::size_t tail_len = buf.size() - static_cast<std::size_t>(ck.wal_offset);
    if (tail_len > 0 && !w.write_all(tail, tail_len)) {
      w.close();
      if (!engine_.wal_.open(engine_.dir_, engine_.wal_io_cfg_)) {
        if (error_out) *error_out = "wal trim: wal reopen after write fail";
        return false;
      }
      if (error_out) *error_out = "wal trim: write tail";
      return false;
    }
    if (!w.sync()) {
      w.close();
      if (!engine_.wal_.open(engine_.dir_, engine_.wal_io_cfg_)) {
        if (error_out) *error_out = "wal trim: wal reopen after sync fail";
        return false;
      }
      if (error_out) *error_out = "wal trim: sync";
      return false;
    }
  }
  if (!engine_.wal_.open(engine_.dir_, engine_.wal_io_cfg_)) {
    if (error_out) *error_out = "wal trim: wal reopen";
    return false;
  }
  ck.wal_offset = 0;
  engine_.checkpoint_undo_coordinator_.fill_checkpoint_undo_safe_prefix_unlocked(&ck);
  if (!engine_.ckpt_.write_rotating(engine_.dir_, ck, error_out)) {
    if (error_out && error_out->empty()) *error_out = "wal trim: checkpoint update";
    return false;
  }
  return true;
}

bool WalCoordinator::gc_sealed_archives_unlocked(std::string* error_out) {
  if (!engine_.wal_catalog_v2_ || engine_.wal_sealed_relative_.empty()) return true;
  const auto saved = engine_.wal_sealed_relative_;
  engine_.wal_sealed_relative_.clear();
  if (!sed::persist_wal_segments_metadata_v2(engine_.dir_, engine_.wal_next_roll_seq_, engine_.wal_sealed_relative_,
                                             error_out)) {
    engine_.wal_sealed_relative_ = saved;
    if (error_out && error_out->empty()) *error_out = "wal archive gc: persist wal.segments";
    return false;
  }
  for (const auto& rel : saved) {
    const auto full = engine_.dir_ / rel;
    std::error_code ec;
    std::filesystem::remove(full, ec);
    if (ec) {
      engine_.wal_sealed_relative_ = saved;
      if (!sed::persist_wal_segments_metadata_v2(engine_.dir_, engine_.wal_next_roll_seq_, engine_.wal_sealed_relative_,
                                                 error_out)) {
        if (error_out) *error_out = std::string("wal archive gc: restore catalog after remove failure: ") + ec.message();
        return false;
      }
      if (error_out) *error_out = std::string("wal archive gc: remove ") + rel + ": " + ec.message();
      return false;
    }
  }
  engine_.wal_segment_count_observed_ = 1u;
  return true;
}

}  // namespace structdb::storage
