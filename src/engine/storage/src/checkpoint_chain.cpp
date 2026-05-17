#include "structdb/storage/checkpoint_chain.hpp"

#include <fstream>
#include <sstream>
#include <system_error>

#include "structdb/infra/file_handle.hpp"
#include "structdb/storage/checkpoint.hpp"

namespace structdb::storage {

namespace {

constexpr const char* kChainName = "checkpoint.chain";

std::filesystem::path chain_path(const std::filesystem::path& data_dir) { return data_dir / kChainName; }

bool parse_line(const std::string& line, CheckpointChainEntry* e) {
  if (!e || line.empty()) return false;
  std::istringstream iss(line);
  iss >> e->checkpoint_seq >> e->wal_offset >> e->redo_offset >> e->manifest_version >> e->mdb_catalog_epoch >>
      e->undo_log_safe_prefix_bytes >> e->written_unix_ns;
  return iss && e->checkpoint_seq > 0;
}

std::string format_line(const CheckpointChainEntry& e) {
  std::ostringstream o;
  o << e.checkpoint_seq << ' ' << e.wal_offset << ' ' << e.redo_offset << ' ' << e.manifest_version << ' '
    << e.mdb_catalog_epoch << ' ' << e.undo_log_safe_prefix_bytes << ' ' << e.written_unix_ns;
  return o.str();
}

CheckpointChainEntry from_state(const CheckpointState& st, std::uint64_t written_unix_ns) {
  CheckpointChainEntry e;
  e.checkpoint_seq = st.checkpoint_seq;
  e.wal_offset = st.wal_offset;
  e.redo_offset = st.redo_offset;
  e.manifest_version = st.manifest_version;
  e.mdb_catalog_epoch = st.mdb_catalog_epoch;
  e.undo_log_safe_prefix_bytes = st.undo_log_safe_prefix_bytes;
  e.written_unix_ns = written_unix_ns;
  return e;
}

CheckpointState to_state(const CheckpointChainEntry& e) {
  CheckpointState st;
  st.checkpoint_seq = e.checkpoint_seq;
  st.wal_offset = e.wal_offset;
  st.redo_offset = e.redo_offset;
  st.manifest_version = e.manifest_version;
  st.mdb_catalog_epoch = e.mdb_catalog_epoch;
  st.undo_log_safe_prefix_bytes = e.undo_log_safe_prefix_bytes;
  return st;
}

}  // namespace

bool checkpoint_chain_append(const std::filesystem::path& data_dir, const CheckpointState& st,
                             std::uint64_t written_unix_ns, std::string* error_out) {
  if (st.checkpoint_seq == 0) {
    if (error_out) *error_out = "checkpoint.chain: seq must be > 0";
    return false;
  }
  const CheckpointChainEntry ent = from_state(st, written_unix_ns);
  std::vector<CheckpointChainEntry> existing;
  (void)checkpoint_chain_read_all(data_dir, &existing, nullptr);
  if (!existing.empty() && existing.back().checkpoint_seq >= ent.checkpoint_seq) return true;

  std::filesystem::create_directories(data_dir);
  const std::string line = format_line(ent) + "\n";
  infra::FileWriter w(chain_path(data_dir), true);
  if (!w.is_open()) {
    if (error_out) *error_out = "checkpoint.chain: open";
    return false;
  }
  if (!w.write_all(line.data(), line.size()) || !w.sync()) {
    if (error_out) *error_out = "checkpoint.chain: append";
    return false;
  }
  return true;
}

bool checkpoint_chain_read_all(const std::filesystem::path& data_dir, std::vector<CheckpointChainEntry>* out,
                               std::string* error_out) {
  if (!out) {
    if (error_out) *error_out = "null out";
    return false;
  }
  out->clear();
  const auto p = chain_path(data_dir);
  std::ifstream in(p, std::ios::binary);
  if (!in) return true;
  std::string line;
  while (std::getline(in, line)) {
    if (line.empty()) continue;
    CheckpointChainEntry e;
    if (!parse_line(line, &e)) {
      if (error_out) *error_out = "checkpoint.chain: bad line";
      return false;
    }
    out->push_back(std::move(e));
  }
  return true;
}

bool checkpoint_chain_find(const std::filesystem::path& data_dir, std::uint64_t checkpoint_seq,
                           CheckpointChainEntry* out, std::string* error_out) {
  if (!out) {
    if (error_out) *error_out = "null out";
    return false;
  }
  std::vector<CheckpointChainEntry> all;
  if (!checkpoint_chain_read_all(data_dir, &all, error_out)) return false;
  for (const auto& e : all) {
    if (e.checkpoint_seq == checkpoint_seq) {
      *out = e;
      return true;
    }
  }
  if (error_out) *error_out = "checkpoint.chain: seq not found";
  return false;
}

bool checkpoint_chain_validate(const std::filesystem::path& data_dir, bool strict, std::string* error_out) {
  std::vector<CheckpointChainEntry> entries;
  if (!checkpoint_chain_read_all(data_dir, &entries, error_out)) return false;
  if (entries.empty()) return true;

  for (std::size_t i = 1; i < entries.size(); ++i) {
    if (entries[i].checkpoint_seq <= entries[i - 1].checkpoint_seq) {
      if (error_out) *error_out = "checkpoint.chain: seq not strictly increasing";
      return false;
    }
  }

  const auto& last = entries.back();
  const std::filesystem::path wal_path = data_dir / "wal.log";
  if (std::filesystem::exists(wal_path)) {
    std::error_code ec;
    const auto wal_sz = std::filesystem::file_size(wal_path, ec);
    if (!ec && wal_sz < last.wal_offset) {
      if (error_out) {
        *error_out = "checkpoint.chain: wal.log smaller than chain tail wal_offset";
      }
      return false;
    }
  }

  CheckpointWriter ckpt;
  CheckpointState latest{};
  if (!ckpt.read_latest(data_dir, &latest, nullptr)) return true;
  if (latest.checkpoint_seq != 0 && last.checkpoint_seq != latest.checkpoint_seq) {
    const std::string msg = "checkpoint.chain: tail seq " + std::to_string(last.checkpoint_seq) +
                            " != read_latest seq " + std::to_string(latest.checkpoint_seq);
    if (strict) {
      if (error_out) *error_out = msg;
      return false;
    }
    if (error_out) *error_out = msg;
    return true;
  }
  if (latest.wal_offset != 0 && last.wal_offset != latest.wal_offset) {
    const std::string msg = "checkpoint.chain: tail wal_offset " + std::to_string(last.wal_offset) +
                            " != read_latest wal_offset " + std::to_string(latest.wal_offset);
    if (strict) {
      if (error_out) *error_out = msg;
      return false;
    }
    if (error_out) *error_out = msg;
    return true;
  }
  return true;
}

bool recover_data_dir_to_checkpoint_seq(const std::filesystem::path& data_dir, std::uint64_t target_checkpoint_seq,
                                        std::string* error_out) {
  CheckpointChainEntry target;
  if (!checkpoint_chain_find(data_dir, target_checkpoint_seq, &target, error_out)) return false;

  CheckpointWriter ckpt;
  CheckpointState latest{};
  if (!ckpt.read_latest(data_dir, &latest, error_out)) return false;
  if (target_checkpoint_seq > latest.checkpoint_seq) {
    if (error_out) *error_out = "recover: target seq in future";
    return false;
  }

  const std::filesystem::path wal_path = data_dir / "wal.log";
  if (std::filesystem::exists(wal_path)) {
    std::error_code ec;
    std::filesystem::resize_file(wal_path, static_cast<std::uintmax_t>(target.wal_offset), ec);
    if (ec) {
      if (error_out) *error_out = "recover: wal resize: " + ec.message();
      return false;
    }
  }

  const CheckpointState st = to_state(target);
  if (!ckpt.write_recovery_checkpoint(data_dir, st, target.written_unix_ns, error_out)) return false;

  std::vector<CheckpointChainEntry> all;
  if (!checkpoint_chain_read_all(data_dir, &all, error_out)) return false;
  std::ostringstream rebuilt;
  for (const auto& e : all) {
    if (e.checkpoint_seq <= target_checkpoint_seq) rebuilt << format_line(e) << '\n';
  }
  {
    infra::FileWriter w(chain_path(data_dir), false);
    if (!w.is_open()) {
      if (error_out) *error_out = "recover: chain rewrite";
      return false;
    }
    const std::string body = rebuilt.str();
    if (!body.empty()) {
      if (!w.write_all(body.data(), body.size())) {
        if (error_out) *error_out = "recover: chain write";
        return false;
      }
    }
    if (!w.sync()) {
      if (error_out) *error_out = "recover: chain sync";
      return false;
    }
  }
  return true;
}

}  // namespace structdb::storage
