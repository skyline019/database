#include "structdb/client/embed_client.hpp"

#include <charconv>
#include <chrono>
#include <ctime>
#include <cstring>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#if defined(_WIN32)
#include <process.h>
#else
#include <unistd.h>
#endif

#include "structdb/facade/engine.hpp"
#include "structdb/infra/tracer.hpp"
#include "structdb/storage/storage_engine.hpp"

#include <algorithm>
#include <limits>

namespace structdb::client {

namespace {

constexpr char kSep = '\t';
constexpr const char kSessionLogFilename[] = "session_log.txt";
constexpr const char kSessionLogArchPrefix[] = "session_log.arch.";
constexpr std::uintmax_t kSessionLogMaxBytes = 2u * 1024u * 1024u;
constexpr int kMaxSessionLogArchives = 12;

std::string path_u8_for_err(const std::filesystem::path& p) {
  try {
    return std::string(p.u8string());
  } catch (...) {
    return "(path)";
  }
}

bool migrate_legacy_embed_artifacts_if_needed_(const std::filesystem::path& session_dir,
                                                const std::filesystem::path& artifacts_dir, std::string* error_out) {
  namespace fs = std::filesystem;
  std::error_code ec;
  const fs::path legacy_journal = session_dir / "session.journal";
  const fs::path new_journal = artifacts_dir / "session.journal";
  const fs::path legacy_txn = session_dir / "session.txn";
  const fs::path new_txn = artifacts_dir / "session.txn";
  if (fs::exists(new_journal, ec)) return true;
  bool need_migrate = false;
  if (fs::exists(legacy_journal, ec)) need_migrate = true;
  if (!fs::exists(new_txn, ec) && fs::exists(legacy_txn, ec)) need_migrate = true;
  if (!need_migrate) return true;

  fs::create_directories(artifacts_dir, ec);
  if (ec) {
    if (error_out) {
      *error_out = "EmbedClient::open: cannot create session artifacts directory (path=" +
                   path_u8_for_err(artifacts_dir) + ")";
    }
    return false;
  }

  constexpr const char* kMoveNames[] = {"session.journal", "session.ckpt", "session.txn", kSessionLogFilename};
  for (const char* name : kMoveNames) {
    const fs::path src = session_dir / name;
    const fs::path dst = artifacts_dir / name;
    if (!fs::exists(src, ec) || ec) continue;
    fs::rename(src, dst, ec);
    if (ec) {
      if (error_out) {
        *error_out = "EmbedClient::open: migrate legacy session file failed (path=" + path_u8_for_err(src) +
                     " err=" + ec.message() + ")";
      }
      return false;
    }
  }

  for (const auto& ent : fs::directory_iterator(session_dir, ec)) {
    if (ec) break;
    const auto name = ent.path().filename().string();
    if (name.size() > std::strlen(kSessionLogArchPrefix) &&
        name.compare(0, std::strlen(kSessionLogArchPrefix), kSessionLogArchPrefix) == 0) {
      const fs::path dst = artifacts_dir / name;
      fs::rename(ent.path(), dst, ec);
      if (ec) {
        if (error_out) {
          *error_out = "EmbedClient::open: migrate legacy session log archive failed (path=" +
                       path_u8_for_err(ent.path()) + ")";
        }
        return false;
      }
    }
  }
  return true;
}

std::string utc_iso8601_z() {
  using namespace std::chrono;
  const auto now = system_clock::now();
  const std::time_t t = system_clock::to_time_t(now);
#if defined(_WIN32)
  std::tm tm_buf{};
  if (gmtime_s(&tm_buf, &t) != 0) return "invalid-time";
  const std::tm* const tm = &tm_buf;
#else
  std::tm tm_buf{};
  const std::tm* const tm = gmtime_r(&t, &tm_buf);
  if (!tm) return "invalid-time";
#endif
  char buf[32];
  if (std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", tm) == 0) return "invalid-time";
  return std::string(buf);
}

std::string utc_compact_for_filename() {
  using namespace std::chrono;
  const auto now = system_clock::now();
  const std::time_t t = system_clock::to_time_t(now);
#if defined(_WIN32)
  std::tm tm_buf{};
  if (gmtime_s(&tm_buf, &t) != 0) return "invalid";
  const std::tm* const tm = &tm_buf;
#else
  std::tm tm_buf{};
  const std::tm* const tm = gmtime_r(&t, &tm_buf);
  if (!tm) return "invalid";
#endif
  char buf[32];
  if (std::strftime(buf, sizeof(buf), "%Y%m%dT%H%M%SZ", tm) == 0) return "invalid";
  return std::string(buf);
}

unsigned long embed_current_pid() {
#if defined(_WIN32)
  return static_cast<unsigned long>(_getpid());
#else
  return static_cast<unsigned long>(getpid());
#endif
}

void append_session_log_line_best_effort(const std::filesystem::path& log_path, const std::string& line) {
  try {
    std::ofstream out(log_path, std::ios::app | std::ios::binary);
    if (!out) return;
    out.write(line.data(), static_cast<std::streamsize>(line.size()));
    out.put('\n');
    out.flush();
  } catch (...) {
  }
}

void prune_old_session_archives(const std::filesystem::path& dir) {
  std::error_code ec;
  std::vector<std::filesystem::directory_entry> archs;
  for (const auto& ent : std::filesystem::directory_iterator(dir, ec)) {
    if (ec) return;
    const auto name = ent.path().filename().string();
    if (name.size() > std::strlen(kSessionLogArchPrefix) &&
        name.compare(0, std::strlen(kSessionLogArchPrefix), kSessionLogArchPrefix) == 0) {
      archs.push_back(ent);
    }
  }
  std::sort(archs.begin(), archs.end(), [](const auto& a, const auto& b) {
    return a.path().filename().string() < b.path().filename().string();
  });
  while (static_cast<int>(archs.size()) > kMaxSessionLogArchives) {
    std::filesystem::remove(archs.front().path(), ec);
    archs.erase(archs.begin());
  }
}

void rotate_session_log_if_needed(const std::filesystem::path& dir, const std::filesystem::path& log_path) {
  std::error_code ec;
  if (!std::filesystem::exists(log_path, ec) || ec) return;
  const auto sz = std::filesystem::file_size(log_path, ec);
  if (ec || sz <= kSessionLogMaxBytes) return;
  const std::filesystem::path arch = dir / (std::string(kSessionLogArchPrefix) + utc_compact_for_filename() + ".txt");
  std::filesystem::rename(log_path, arch, ec);
  if (ec) {
    try {
      std::ofstream trunc(log_path, std::ios::trunc | std::ios::binary);
      trunc << "# session_log truncated: rename to archive failed (" << arch.filename().string() << ")\n";
    } catch (...) {
    }
    return;
  }
  prune_old_session_archives(dir);
}

bool journal_field_has_illegal_chars(std::string_view s) {
  return s.find_first_of("\t\n\r") != std::string_view::npos;
}

bool batch_journal_safe(const CommandBatch& batch) {
  if (journal_field_has_illegal_chars(batch.idempotency_token)) return false;
  if (journal_field_has_illegal_chars(batch.client_session_id)) return false;
  for (const auto& kv : batch.puts) {
    if (journal_field_has_illegal_chars(kv.first)) return false;
    if (journal_field_has_illegal_chars(kv.second)) return false;
  }
  for (const auto& k : batch.dels) {
    if (journal_field_has_illegal_chars(k)) return false;
  }
  return true;
}

bool parse_u64(std::string_view s, std::uint64_t* out) {
  if (!out || s.empty()) return false;
  std::uint64_t v = 0;
  const auto* first = s.data();
  const auto* last = s.data() + s.size();
  const auto res = std::from_chars(first, last, v);
  if (res.ec != std::errc{} || res.ptr != last) return false;
  *out = v;
  return true;
}

bool parse_usize(std::string_view s, std::size_t* out) {
  std::uint64_t v = 0;
  if (!parse_u64(s, &v)) return false;
  if (v > static_cast<std::uint64_t>((std::numeric_limits<std::size_t>::max)())) return false;
  *out = static_cast<std::size_t>(v);
  return true;
}

void strip_trailing_cr(std::string* line) {
  if (!line->empty() && line->back() == '\r') line->pop_back();
}

std::string encode_batch(std::uint64_t seq, const CommandBatch& batch) {
  std::ostringstream out;
  out << seq << kSep << batch.idempotency_token << kSep << batch.puts.size();
  for (const auto& kv : batch.puts) {
    out << kSep << kv.first << kSep << kv.second;
  }
  if (!batch.dels.empty()) {
    out << kSep << "D" << kSep << batch.dels.size();
    for (const auto& k : batch.dels) {
      out << kSep << k;
    }
  }
  out << kSep << batch.client_session_id << kSep << batch.term;
  return out.str();
}

bool apply_fields_after_ack(const std::vector<std::string>& fields, storage::StorageEngine* st,
                            std::uint64_t* max_seq_out, std::string* error_out) {
  if (fields.size() < 3) {
    if (error_out) *error_out = "journal: short line";
    return false;
  }
  std::uint64_t seq = 0;
  std::size_t n = 0;
  if (!parse_u64(fields[0], &seq)) {
    if (error_out) *error_out = "journal: bad seq";
    return false;
  }
  if (!parse_usize(fields[2], &n)) {
    if (error_out) *error_out = "journal: bad put count";
    return false;
  }
  std::size_t pos = 3 + 2 * n;
  if (pos < fields.size() && fields[pos] == "D") {
    std::size_t nd = 0;
    if (pos + 1 >= fields.size()) {
      if (error_out) *error_out = "journal: del count";
      return false;
    }
    if (!parse_usize(fields[pos + 1], &nd)) {
      if (error_out) *error_out = "journal: bad del count";
      return false;
    }
    const std::size_t need = pos + 2 + nd + 2;
    if (fields.size() != need) {
      if (error_out) *error_out = "journal: field count (dels)";
      return false;
    }
    const std::uint64_t batch_commit_seq = n > 0 ? st->reserve_commit_seq() : 0;
    for (std::size_t i = 0; i < nd; ++i) {
      const auto& dk = fields[pos + 2 + i];
      if (!st->remove(dk, false)) {
        if (error_out) *error_out = "journal: del during recovery";
        return false;
      }
    }
    for (std::size_t i = 0; i < n; ++i) {
      const auto& k = fields[3 + 2 * i];
      const auto& v = fields[4 + 2 * i];
      if (!st->put(k, v, false, batch_commit_seq)) {
        if (error_out) *error_out = "journal: put during recovery";
        return false;
      }
    }
    if (max_seq_out) *max_seq_out = std::max(*max_seq_out, seq);
    return true;
  }
  if (fields.size() != pos && fields.size() != pos + 2) {
    if (error_out) *error_out = "journal: field count";
    return false;
  }
  const std::uint64_t batch_commit_seq = n > 0 ? st->reserve_commit_seq() : 0;
  for (std::size_t i = 0; i < n; ++i) {
    const auto& k = fields[3 + 2 * i];
    const auto& v = fields[4 + 2 * i];
    if (!st->put(k, v, false, batch_commit_seq)) {
      if (error_out) *error_out = "journal: put during recovery";
      return false;
    }
  }
  if (max_seq_out) *max_seq_out = std::max(*max_seq_out, seq);
  return true;
}

}  // namespace

bool EmbedClient::split_journal_fields(const std::string& line, std::vector<std::string>* fields) {
  if (!fields) return false;
  fields->clear();
  std::size_t start = 0;
  while (start <= line.size()) {
    const auto pos = line.find(kSep, start);
    if (pos == std::string::npos) {
      fields->emplace_back(line.substr(start));
      break;
    }
    fields->emplace_back(line.substr(start, pos - start));
    start = pos + 1;
  }
  return true;
}

EmbedClient::EmbedClient(facade::Engine& engine) : engine_(engine) {}

void EmbedClient::refresh_read_snapshot() {
  if (engine_.storage()) read_snapshot_seq_ = engine_.latest_commit_seq();
}

bool EmbedClient::open(const std::filesystem::path& session_dir, std::string* error_out) {
  journal_w_.close();
  if (!engine_.storage()) {
    if (error_out) {
      *error_out = "EmbedClient::open: engine storage is null (ensure Engine::startup succeeded before opening embed)";
    }
    return false;
  }
  dir_ = session_dir;
  std::filesystem::create_directories(dir_);
  const std::filesystem::path artifacts_dir = dir_ / kEmbedSessionArtifactsDir;
  std::filesystem::create_directories(artifacts_dir);
  if (!migrate_legacy_embed_artifacts_if_needed_(dir_, artifacts_dir, error_out)) return false;

  journal_path_ = artifacts_dir / "session.journal";
  ckpt_path_ = artifacts_dir / "session.ckpt";
  session_log_path_ = artifacts_dir / kSessionLogFilename;
  last_engine_checkpoint_manifest_ = 0;
  last_engine_checkpoint_seq_ = 0;

  if (std::filesystem::exists(ckpt_path_)) {
    std::ifstream in(ckpt_path_);
    if (!in) {
      if (error_out) {
        *error_out = "EmbedClient::open: cannot open session checkpoint for read (path=" + path_u8_for_err(ckpt_path_) +
                     ")";
      }
      return false;
    }
    std::string line1;
    if (!std::getline(in, line1)) {
      if (error_out) {
        *error_out = "EmbedClient::open: session checkpoint file is empty or unreadable (path=" +
                     path_u8_for_err(ckpt_path_) + ")";
      }
      return false;
    }
    strip_trailing_cr(&line1);
    std::uint64_t ack = 0;
    if (!parse_u64(line1, &ack)) {
      if (error_out) {
        *error_out = "EmbedClient::open: first line of session.ckpt is not a valid u64 last_ack (path=" +
                     path_u8_for_err(ckpt_path_) + ", got=" + line1 + ")";
      }
      return false;
    }
    last_ack_ = ack;
    std::string line2;
    if (std::getline(in, line2)) {
      strip_trailing_cr(&line2);
      if (!line2.empty()) {
        std::uint64_t mv = 0;
        if (!parse_u64(line2, &mv)) {
          if (error_out) {
            *error_out = "EmbedClient::open: second line of session.ckpt is not a valid u64 manifest version (path=" +
                         path_u8_for_err(ckpt_path_) + ", got=" + line2 + ")";
          }
          return false;
        }
        last_engine_checkpoint_manifest_ = mv;
      }
    }
    std::string line3;
    if (std::getline(in, line3)) {
      strip_trailing_cr(&line3);
      if (!line3.empty()) {
        std::uint64_t cks = 0;
        if (!parse_u64(line3, &cks)) {
          if (error_out) {
            *error_out = "EmbedClient::open: third line of session.ckpt is not a valid u64 checkpoint_seq (path=" +
                         path_u8_for_err(ckpt_path_) + ", got=" + line3 + ")";
          }
          return false;
        }
        last_engine_checkpoint_seq_ = cks;
      }
    }
  }

  {
    std::lock_guard<std::mutex> lock(idem_mu_);
    idem_completed_.clear();
  }

  if (!parse_journal_for_recovery(error_out)) return false;

  refresh_read_snapshot();

  rotate_session_log_if_needed(artifacts_dir, session_log_path_);
  {
    std::uint64_t wal_b = 0;
    if (auto* st = engine_.storage()) wal_b = st->wal_log_bytes_on_disk();
    std::ostringstream line;
    line << "SESSION_OPEN\t" << utc_iso8601_z() << "\tpid=" << embed_current_pid() << "\tlast_ack=" << last_ack_
         << "\tnext_seq=" << next_seq_ << "\twal_bytes=" << wal_b;
    append_session_log_line_best_effort(session_log_path_, line.str());
  }

  opened_ = true;
  return true;
}

void EmbedClient::close() {
  journal_w_.close();
  if (opened_ && !session_log_path_.empty()) {
    std::ostringstream line;
    line << "SESSION_CLOSE\t" << utc_iso8601_z() << "\tpid=" << embed_current_pid();
    append_session_log_line_best_effort(session_log_path_, line.str());
  }
  opened_ = false;
}

bool EmbedClient::append_journal_line(std::uint64_t seq, const CommandBatch& batch, bool fsync) {
  const auto line = encode_batch(seq, batch);
  std::string block;
  block.reserve(line.size() + 1);
  block.append(line);
  block.push_back('\n');
  if (!journal_w_.is_open()) {
    if (!journal_w_.open(journal_path_, true)) return false;
  }
  if (!journal_w_.write_all(block.data(), block.size())) return false;
  if (fsync && !journal_w_.sync()) return false;
  return true;
}

bool EmbedClient::submit(const CommandBatch& batch, bool fsync_journal, std::string* error_out) {
  infra::SpanGuard trace_submit("embed.submit", 0);
  if (!opened_) {
    if (error_out) *error_out = "EmbedClient::submit: session not open (call EmbedClient::open first)";
    return false;
  }
  if (!batch_journal_safe(batch)) {
    if (error_out) *error_out = "journal: batch contains tab/newline in field (unsafe)";
    return false;
  }
  std::lock_guard<std::mutex> slk(submit_mu_);
  if (!batch.idempotency_token.empty()) {
    std::lock_guard<std::mutex> lock(idem_mu_);
    if (idem_completed_.count(batch.idempotency_token)) return true;
  }
  auto* st = engine_.storage();
  if (!st) {
    if (error_out) *error_out = "EmbedClient::submit: engine storage is null";
    return false;
  }
  const std::uint64_t seq = next_seq_++;
  if (!batch.dels.empty() || !batch.puts.empty()) {
    const std::uint64_t est =
        structdb::storage::StorageEngine::estimate_commit_embed_batch_wal_frame_bytes(batch.dels, batch.puts);
    structdb::facade::Engine::WalMutationBudget wal_budget(&engine_, est);
    if (!st->commit_embed_batch(batch.dels, batch.puts, fsync_journal, error_out)) {
      return false;
    }
    engine_.sync_scheduler_budget_from_storage_pressure();
  } else if (fsync_journal && !st->wal_sync(error_out)) {
    return false;
  }
  if (!append_journal_line(seq, batch, fsync_journal)) {
    if (error_out) {
      *error_out = "EmbedClient::submit: append to session.journal failed after storage commit (path=" +
                   path_u8_for_err(journal_path_) + ", seq=" + std::to_string(seq) + ")";
    }
    return false;
  }
  last_ack_ = seq;
  if (!batch.idempotency_token.empty()) {
    std::lock_guard<std::mutex> idlk(idem_mu_);
    idem_completed_.insert(batch.idempotency_token);
  }
  refresh_read_snapshot();
  return true;
}

bool EmbedClient::save_checkpoint(std::string* error_out) {
  if (!opened_) {
    if (error_out) *error_out = "EmbedClient::save_checkpoint: session not open (call EmbedClient::open first)";
    return false;
  }
  auto* st = engine_.storage();
  if (!st) {
    if (error_out) *error_out = "EmbedClient::save_checkpoint: engine storage is null";
    return false;
  }
  if (!st->checkpoint(error_out)) return false;
  last_engine_checkpoint_manifest_ = st->manifest().version();
  last_engine_checkpoint_seq_ = 0;
  {
    structdb::storage::CheckpointState ecs{};
    if (st->read_checkpoint_state(&ecs)) {
      last_engine_checkpoint_seq_ = ecs.checkpoint_seq;
    }
  }
  std::ofstream out(ckpt_path_, std::ios::trunc);
  if (!out) {
    if (error_out) {
      *error_out = "EmbedClient::save_checkpoint: cannot open session.ckpt for write (path=" + path_u8_for_err(ckpt_path_) +
                   ")";
    }
    return false;
  }
  out << last_ack_ << "\n" << last_engine_checkpoint_manifest_ << "\n" << last_engine_checkpoint_seq_ << "\n";
  return static_cast<bool>(out);
}

bool EmbedClient::recover(std::string* error_out) {
  if (!opened_) {
    if (error_out) *error_out = "EmbedClient::recover: session not open (call EmbedClient::open first)";
    return false;
  }
  return parse_journal_for_recovery(error_out);
}

bool EmbedClient::parse_journal_for_recovery(std::string* error_out) {
  auto* st = engine_.storage();
  if (!st) {
    if (error_out) *error_out = "EmbedClient::parse_journal_for_recovery: engine storage is null";
    return false;
  }
  if (!std::filesystem::exists(journal_path_)) return true;
  std::ifstream in(journal_path_);
  if (!in) {
    if (error_out) {
      *error_out = "EmbedClient::parse_journal_for_recovery: cannot open session.journal for read (path=" +
                   path_u8_for_err(journal_path_) + ")";
    }
    return false;
  }
  const bool prefer_wal_replayed = st->wal_log_bytes_on_disk() > 0;
  std::string line;
  std::uint64_t journal_max_seq = 0;
  std::uint64_t max_applied_seq = 0;
  while (std::getline(in, line)) {
    strip_trailing_cr(&line);
    if (line.empty()) continue;
    std::vector<std::string> fields;
    if (!split_journal_fields(line, &fields)) continue;
    if (fields.size() < 3) {
      if (error_out) {
        *error_out = "EmbedClient::parse_journal_for_recovery: line has fewer than 3 tab-separated fields (path=" +
                     path_u8_for_err(journal_path_) + ", line_prefix=" + line.substr(0, 160) + ")";
      }
      return false;
    }
    std::uint64_t seq = 0;
    if (!parse_u64(fields[0], &seq)) {
      if (error_out) {
        *error_out = "EmbedClient::parse_journal_for_recovery: first field is not a valid u64 seq (path=" +
                     path_u8_for_err(journal_path_) + ", field0=\"" + fields[0] + "\")";
      }
      return false;
    }
    journal_max_seq = std::max(journal_max_seq, seq);
    const std::string& token = fields[1];
    if (!token.empty()) {
      std::lock_guard<std::mutex> lock(idem_mu_);
      idem_completed_.insert(token);
    }
    if (prefer_wal_replayed) {
      continue;
    }
    if (seq <= last_ack_) continue;
    if (!apply_fields_after_ack(fields, st, &max_applied_seq, error_out)) return false;
  }
  last_ack_ = std::max(last_ack_, journal_max_seq);
  next_seq_ = std::max(next_seq_, journal_max_seq + 1);
  (void)max_applied_seq;
  return true;
}

}  // namespace structdb::client
