#include <algorithm>
#include <chrono>
#include <numeric>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include <cctype>

#include "structdb/client/embed_client.hpp"
#include "structdb/client/mdb_runner.hpp"
#include "structdb/facade/config.hpp"
#include "structdb/facade/engine.hpp"
#include "structdb/infra/logging.hpp"
#include "structdb/storage/checkpoint_chain.hpp"

#if defined(_MSC_VER)
#  include "structdb/infra/leak_detector.hpp"
#endif

namespace {

struct QueryBenchCase {
  const char* name;
  std::string line;
};

enum class QueryBenchProfile { Standard, Analytics, All };

QueryBenchProfile parse_query_bench_profile(std::string_view s) {
  if (s == "analytics") return QueryBenchProfile::Analytics;
  if (s == "all" || s == "full") return QueryBenchProfile::All;
  return QueryBenchProfile::Standard;
}

double steady_ms_since(const std::chrono::steady_clock::time_point& t0) {
  const auto t1 = std::chrono::steady_clock::now();
  return std::chrono::duration<double, std::milli>(t1 - t0).count();
}

double percentile_ms(std::vector<double> samples, double p) {
  if (samples.empty()) return 0.0;
  std::sort(samples.begin(), samples.end());
  const double rank = p * static_cast<double>(samples.size() - 1);
  const std::size_t lo = static_cast<std::size_t>(rank);
  const std::size_t hi = std::min(lo + 1, samples.size() - 1);
  const double frac = rank - static_cast<double>(lo);
  return samples[lo] * (1.0 - frac) + samples[hi] * frac;
}

bool run_timed_mdb_line(structdb::facade::Engine& engine, structdb::client::EmbedClient& client,
                        structdb::client::mdb::MdbInteractiveSession& session, std::string_view line,
                        double* out_ms, std::string* err_out) {
  std::vector<std::string> log;
  const auto t0 = std::chrono::steady_clock::now();
  const auto r =
      structdb::client::mdb::mdb_repl_execute_line(engine, client, session, line, &log, false, false, err_out);
  *out_ms = steady_ms_since(t0);
  return r.ok;
}

void print_oltp_micro_json(std::uint64_t rows, const std::vector<double>& insert_ms,
                           const std::vector<double>& update_ms) {
  const double insert_total =
      std::accumulate(insert_ms.begin(), insert_ms.end(), 0.0);
  const double update_total = std::accumulate(update_ms.begin(), update_ms.end(), 0.0);
  const double insert_p50 = percentile_ms(insert_ms, 0.50);
  const double insert_p99 = percentile_ms(insert_ms, 0.99);
  const double update_p50 = percentile_ms(update_ms, 0.50);
  const double update_p99 = percentile_ms(update_ms, 0.99);
  const double insert_tps =
      insert_total > 0 ? static_cast<double>(rows) / (insert_total / 1000.0) : 0.0;
  const double update_tps =
      update_total > 0 ? static_cast<double>(rows) / (update_total / 1000.0) : 0.0;
  std::cout << "[OLTP_MICRO_JSON]{"
            << "\"rows\":" << rows << ","
            << "\"insert_p50_ms\":" << insert_p50 << ","
            << "\"insert_p99_ms\":" << insert_p99 << ","
            << "\"insert_tps\":" << insert_tps << ","
            << "\"update_p50_ms\":" << update_p50 << ","
            << "\"update_p99_ms\":" << update_p99 << ","
            << "\"update_tps\":" << update_tps << ","
            << "\"insert_total_ms\":" << insert_total << ","
            << "\"update_total_ms\":" << update_total << "}\n";
}

int run_oltp_persist_micro(structdb::facade::Engine& engine, const std::filesystem::path& session_dir,
                           std::uint64_t rows) {
  if (rows == 0) rows = 1000;
  std::string err;
  structdb::client::EmbedClient client(engine);
  if (!client.open(session_dir, &err)) {
    std::cerr << "embed open failed: " << err << "\n";
    return 1;
  }
  structdb::client::mdb::MdbInteractiveSession session;
  double ms = 0;
  const char* setup[] = {"CREATE TABLE(oltp)", "USE(oltp)", "DEFATTR(id:int,val:int)"};
  for (const char* line : setup) {
    if (!run_timed_mdb_line(engine, client, session, line, &ms, &err)) {
      std::cerr << "setup failed (" << line << "): " << err << "\n";
      client.close();
      return 1;
    }
  }

  std::vector<double> insert_ms;
  std::vector<double> update_ms;
  insert_ms.reserve(static_cast<std::size_t>(rows));
  update_ms.reserve(static_cast<std::size_t>(rows));

  for (std::uint64_t i = 1; i <= rows; ++i) {
    const std::string ins =
        "INSERT(" + std::to_string(i) + "," + std::to_string(i) + "," + std::to_string(i) + ")";
    if (!run_timed_mdb_line(engine, client, session, ins, &ms, &err)) {
      std::cerr << "INSERT failed at id=" << i << ": " << err << "\n";
      client.close();
      return 1;
    }
    insert_ms.push_back(ms);
  }
  for (std::uint64_t i = 1; i <= rows; ++i) {
    const std::string upd = "UPDATE(" + std::to_string(i) + "," + std::to_string(i) + "," +
                            std::to_string(i + 1000000) + ")";
    if (!run_timed_mdb_line(engine, client, session, upd, &ms, &err)) {
      std::cerr << "UPDATE failed at id=" << i << ": " << err << "\n";
      client.close();
      return 1;
    }
    update_ms.push_back(ms);
  }

  print_oltp_micro_json(rows, insert_ms, update_ms);
  client.close();
  return 0;
}

int run_query_bench(structdb::facade::Engine& engine, const std::filesystem::path& session_dir,
                    std::string_view table, std::uint64_t row_count, int page_size, int warmup_iters, int bench_iters,
                    bool bench_varied, QueryBenchProfile profile) {
  std::string err;
  structdb::client::EmbedClient client(engine);
  if (!client.open(session_dir, &err)) {
    std::cerr << "embed open failed: " << err << "\n";
    return 1;
  }
  structdb::client::mdb::MdbInteractiveSession session;
  const std::string use_line = std::string("USE(") + std::string(table) + ")";
  double ms = 0;
  if (!run_timed_mdb_line(engine, client, session, use_line, &ms, &err)) {
    std::cerr << "USE failed: " << err << "\n";
    client.close();
    return 1;
  }
  const std::uint64_t hit_id = row_count > 0 ? row_count / 2 : 1;
  const std::uint64_t miss_id = row_count + 1;
  const int page_size_clamped = page_size > 0 ? page_size : 100;
  const int mid_page = row_count > 0 ? static_cast<int>((row_count / 2) / static_cast<std::uint64_t>(page_size_clamped)) + 1
                                     : 1;
  const int last_page =
      row_count > 0 ? static_cast<int>((row_count - 1) / static_cast<std::uint64_t>(page_size_clamped)) + 1 : 1;

  std::vector<QueryBenchCase> cases;
  const bool run_standard =
      profile == QueryBenchProfile::Standard || profile == QueryBenchProfile::All;
  const bool run_analytics =
      profile == QueryBenchProfile::Analytics || profile == QueryBenchProfile::All;

  if (run_standard) {
    cases.push_back({"count", "COUNT"});
    cases.push_back({"explain_where_hit", "EXPLAIN WHERE(id,=," + std::to_string(hit_id) + ")"});
    cases.push_back({"where_hit", "WHERE(id,=," + std::to_string(hit_id) + ")"});
    cases.push_back({"where_miss", "WHERE(id,=," + std::to_string(miss_id) + ")"});
    cases.push_back({"page_json_first", "PAGE_JSON(1," + std::to_string(page_size_clamped) + ",id,asc)"});
    cases.push_back({"page_json_mid", "PAGE_JSON(" + std::to_string(mid_page) + "," +
                                         std::to_string(page_size_clamped) + ",id,asc)"});
    cases.push_back({"page_json_last", "PAGE_JSON(" + std::to_string(last_page) + "," +
                                        std::to_string(page_size_clamped) + ",id,asc)"});
    cases.push_back(
        {"page_json_after_mid",
         "PAGE_JSON(AFTER," + std::to_string(hit_id > 50 ? hit_id - 50 : 0) + "," +
             std::to_string(page_size_clamped) + ",id,asc)"});
    cases.push_back({"page_json_cols_id", "PAGE_JSON(1," + std::to_string(page_size_clamped) + ",id,asc,COLS,id)"});
    cases.push_back(
        {"page_json_after_stream",
         "PAGE_JSON(AFTER," + std::to_string(hit_id > 50 ? hit_id - 50 : 0) + "," +
             std::to_string(page_size_clamped) + ",id,asc,STREAM)"});
    cases.push_back(
        {"page_json_ids_only",
         "PAGE_JSON(AFTER," + std::to_string(hit_id > 50 ? hit_id - 50 : 0) + "," +
             std::to_string(page_size_clamped) + ",id,asc,IDS_ONLY)"});
    if (bench_varied) {
      const std::string tag_hit = "t" + std::to_string(hit_id);
      cases.push_back({"explain_tag_hit", "EXPLAIN WHERE(tag,=," + tag_hit + ")"});
      cases.push_back({"where_tag_hit", "WHERE(tag,=," + tag_hit + ")"});
    }
  }

  if (run_analytics) {
    const int dept_hit = row_count > 0 ? static_cast<int>(hit_id % 100) : 0;
    const std::string k_hit = "t" + std::to_string(hit_id % 50);
    cases.push_back({"group_by_dept_count", "GROUP BY (dept) COUNT"});
    cases.push_back({"group_by_dept_sum", "GROUP BY (dept) SUM(val)"});
    cases.push_back({"sum_val", "SUM(val)"});
    cases.push_back({"qbal_val", "QBAL(val,0)"});
    cases.push_back({"explain_dept_hit", "EXPLAIN WHERE(dept,=," + std::to_string(dept_hit) + ")"});
    cases.push_back({"where_dept_hit", "WHERE(dept,=," + std::to_string(dept_hit) + ")"});
    cases.push_back({"explain_k_hit", "EXPLAIN WHERE(k,=," + k_hit + ")"});
    cases.push_back({"where_k_hit", "WHERE(k,=," + k_hit + ")"});
    cases.push_back(
        {"page_json_sort_val_desc", "PAGE_JSON(1," + std::to_string(page_size_clamped) + ",val,desc)"});
    cases.push_back(
        {"page_json_sort_dept_asc", "PAGE_JSON(1," + std::to_string(page_size_clamped) + ",dept,asc)"});
    cases.push_back({"page_json_mid_val_desc", "PAGE_JSON(" + std::to_string(mid_page) + "," +
                                                  std::to_string(page_size_clamped) + ",val,desc)"});
    // Capped STATS/IDS for PR gate (~ms); full STATS is optional soak (scan_index_ik_stats_full).
    cases.push_back({"scan_index_ik", "SCAN INDEX(ik,5000,STATS)"});
    cases.push_back({"scan_index_ik_ids", "SCAN INDEX(ik,5000,IDS)"});
    cases.push_back({"scan_index_ik_stats_full", "SCAN INDEX(ik,STATS)"});
  }

  const char* profile_tag = "standard";
  if (profile == QueryBenchProfile::Analytics) profile_tag = "analytics";
  if (profile == QueryBenchProfile::All) profile_tag = "all";
  std::cout << "[QUERY_BENCH] phase=setup table=" << table << " profile=" << profile_tag
            << " row_count_hint=" << row_count << " cases=" << cases.size() << " ok=1\n";

  auto run_phase = [&](const char* phase, int iters) {
    for (const auto& c : cases) {
      for (int i = 0; i < iters; ++i) {
        double qms = 0;
        const bool ok = run_timed_mdb_line(engine, client, session, c.line, &qms, &err);
        std::cout << "[QUERY_BENCH] phase=" << phase << " name=" << c.name << " iter=" << i << " ms=" << qms
                  << " ok=" << (ok ? 1 : 0);
        if (!ok) std::cout << " err=" << err;
        std::cout << "\n";
        if (!ok) return false;
      }
    }
    return true;
  };

  if (!run_phase("warmup", warmup_iters > 0 ? warmup_iters : 0)) {
    client.close();
    return 1;
  }
  if (!run_phase("bench", bench_iters > 0 ? bench_iters : 1)) {
    client.close();
    return 1;
  }
  client.close();
  return 0;
}

int run_backup_bundle(const std::string& data_dir, const std::string& session_dir,
                      const std::filesystem::path& out_root) {
  namespace fs = std::filesystem;
  const fs::path data = data_dir;
  const fs::path sess = session_dir;
  if (!fs::exists(data)) {
    std::cerr << "backup: data_dir not found: " << data << "\n";
    return 1;
  }
  if (!fs::exists(sess)) {
    std::cerr << "backup: session_dir not found: " << sess << "\n";
    return 1;
  }
  const fs::path out_data = out_root / "data_dir";
  const fs::path out_sess = out_root / "session_dir";
  std::error_code ec;
  fs::create_directories(out_root, ec);
  auto copy_tree = [&](const fs::path& src, const fs::path& dst) -> bool {
    if (!fs::exists(src)) return true;
    fs::create_directories(dst, ec);
    for (fs::recursive_directory_iterator it(src, fs::directory_options::skip_permission_denied);
         it != fs::recursive_directory_iterator(); ++it) {
      const auto rel = fs::relative(it->path(), src, ec);
      if (ec) return false;
      const fs::path target = dst / rel;
      if (it->is_directory()) {
        fs::create_directories(target, ec);
      } else if (it->is_regular_file()) {
        fs::create_directories(target.parent_path(), ec);
        fs::copy_file(it->path(), target, fs::copy_options::overwrite_existing, ec);
        if (ec) {
          std::cerr << "backup copy failed: " << it->path() << " -> " << target << ": " << ec.message() << "\n";
          return false;
        }
      }
    }
    return true;
  };
  if (!copy_tree(data, out_data) || !copy_tree(sess, out_sess)) return 1;
  std::vector<structdb::storage::CheckpointChainEntry> chain_entries;
  std::string chain_err;
  if (structdb::storage::checkpoint_chain_read_all(data, &chain_entries, &chain_err) && !chain_entries.empty()) {
    const auto last_seq = chain_entries.back().checkpoint_seq;
    std::ofstream manifest(out_root / "backup_manifest.json", std::ios::trunc);
    if (manifest) {
      manifest << "{\"last_checkpoint_seq\":" << last_seq << "}\n";
    }
  }
  std::cout << "[BACKUP_BUNDLE] ok out=" << out_root.string() << " data_dir=" << out_data.string()
            << " session_dir=" << out_sess.string() << "\n";
  return 0;
}

int run_recover_to_checkpoint_seq(const std::string& data_dir, std::uint64_t checkpoint_seq) {
  structdb::facade::Engine engine;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = data_dir;
  snap.version = 1;
  engine.config().update(1, snap);
  std::string err;
  if (!engine.recover_to_checkpoint_seq(checkpoint_seq, &err)) {
    std::cerr << "recover failed: " << err << "\n";
    return 1;
  }
  std::cout << "[RECOVER_TO_CHECKPOINT_SEQ] ok seq=" << checkpoint_seq << " data_dir=" << data_dir << "\n";
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  structdb::infra::install_spdlog_default();

#if defined(_MSC_VER) && defined(_DEBUG)
  structdb::infra::LeakDetector::install_msvc_crt_leak_report();
#endif

  // Default matches `EngineConfigSnapshot::data_dir` (`_data`); override with `--data-dir`.
  std::string data_dir = "_data";
  std::string run_mdb_path;
  std::string session_dir;
  bool repl = false;
  int page_no = 0;
  int page_size = 0;
  std::string page_order = "id";
  bool page_desc = false;
  bool page_json = false;
  std::string page_table;
  bool mdb_bulk_import = false;
  bool mdb_stream_log = false;
  bool mdb_persist_coalesce = false;
  bool embed_journal_skip_until_commit = false;
  bool memtable_bulk_put = false;
  std::uint64_t storage_embed_batch_max_frame_bytes = 0;
  std::uint32_t mdb_persist_chunk_max_puts = 0;
  bool query_bench = false;
  std::uint64_t query_bench_row_count = 100000;
  int query_bench_page_size = 100;
  int query_bench_warmup = 1;
  int query_bench_iters = 5;
  bool query_bench_varied = false;
  std::string query_bench_profile = "standard";
  bool oltp_persist_micro = false;
  std::uint64_t oltp_rows = 1000;
  bool backup_bundle = false;
  std::string backup_out;
  bool recover_checkpoint = false;
  std::uint64_t recover_checkpoint_seq = 0;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--data-dir" && i + 1 < argc) {
      data_dir = argv[++i];
    } else if (a == "--mdb-bulk-import") {
      mdb_bulk_import = true;
    } else if (a == "--mdb-stream-log") {
      mdb_stream_log = true;
    } else if (a == "--mdb-persist-coalesce") {
      mdb_persist_coalesce = true;
    } else if (a == "--embed-journal-skip-until-commit") {
      embed_journal_skip_until_commit = true;
    } else if (a == "--memtable-bulk-put") {
      memtable_bulk_put = true;
    } else if (a == "--storage-embed-batch-max-frame-bytes" && i + 1 < argc) {
      storage_embed_batch_max_frame_bytes = static_cast<std::uint64_t>(std::stoull(argv[++i]));
    } else if (a == "--mdb-persist-chunk-max-puts" && i + 1 < argc) {
      mdb_persist_chunk_max_puts = static_cast<std::uint32_t>(std::stoul(argv[++i]));
    } else if (a == "--run-mdb" && i + 1 < argc) {
      run_mdb_path = argv[++i];
    } else if (a == "--session-dir" && i + 1 < argc) {
      session_dir = argv[++i];
    } else if (a == "--repl") {
      repl = true;
    } else if (a == "--page" && i + 2 < argc) {
      page_no = std::stoi(argv[++i]);
      page_size = std::stoi(argv[++i]);
    } else if (a == "--order" && i + 1 < argc) {
      page_order = argv[++i];
    } else if (a == "--desc") {
      page_desc = true;
    } else if (a == "--page-json") {
      page_json = true;
    } else if (a == "--table" && i + 1 < argc) {
      page_table = argv[++i];
    } else if (a == "--query-bench") {
      query_bench = true;
    } else if (a == "--bench-row-count" && i + 1 < argc) {
      query_bench_row_count = static_cast<std::uint64_t>(std::stoull(argv[++i]));
    } else if (a == "--bench-page-size" && i + 1 < argc) {
      query_bench_page_size = std::stoi(argv[++i]);
    } else if (a == "--bench-warmup" && i + 1 < argc) {
      query_bench_warmup = std::stoi(argv[++i]);
    } else if (a == "--bench-iters" && i + 1 < argc) {
      query_bench_iters = std::stoi(argv[++i]);
    } else if (a == "--bench-varied") {
      query_bench_varied = true;
    } else if (a == "--bench-profile" && i + 1 < argc) {
      query_bench_profile = argv[++i];
    } else if (a == "--oltp-persist-micro") {
      oltp_persist_micro = true;
    } else if (a == "--oltp-rows" && i + 1 < argc) {
      oltp_rows = static_cast<std::uint64_t>(std::stoull(argv[++i]));
    } else if (a == "--backup-bundle" && i + 1 < argc) {
      backup_bundle = true;
      backup_out = argv[++i];
    } else if (a == "--recover-to-checkpoint-seq" && i + 1 < argc) {
      recover_checkpoint = true;
      recover_checkpoint_seq = static_cast<std::uint64_t>(std::stoull(argv[++i]));
    }
  }

  if (recover_checkpoint) {
    if (recover_checkpoint_seq == 0) {
      std::cerr << "--recover-to-checkpoint-seq requires seq > 0\n";
      return 1;
    }
    return run_recover_to_checkpoint_seq(data_dir, recover_checkpoint_seq);
  }

  if (backup_bundle) {
    if (backup_out.empty()) {
      std::cerr << "--backup-bundle requires <out_dir>\n";
      return 1;
    }
    return run_backup_bundle(data_dir, session_dir.empty()
                                                 ? (std::filesystem::path(data_dir) / "embed_session").string()
                                                 : session_dir,
                             std::filesystem::path(backup_out));
  }

  if (session_dir.empty()) {
    session_dir = (std::filesystem::path(data_dir) / "embed_session").string();
  }

  structdb::facade::Engine engine;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = data_dir;
  snap.version = 1;
  snap.mdb_bulk_import_mode = mdb_bulk_import;
  snap.mdb_persist_coalesce = mdb_persist_coalesce;
  snap.embed_journal_skip_until_commit = embed_journal_skip_until_commit;
  snap.memtable_bulk_put_enabled = memtable_bulk_put;
  if (storage_embed_batch_max_frame_bytes > 0) {
    snap.storage_embed_batch_max_frame_bytes = storage_embed_batch_max_frame_bytes;
  }
  if (mdb_persist_chunk_max_puts > 0) {
    snap.mdb_persist_chunk_max_puts = mdb_persist_chunk_max_puts;
  }
  engine.config().update(1, snap);

  std::string err;
  if (!engine.startup(&err)) {
    std::cerr << "startup failed: " << err << "\n";
    return 1;
  }

  if (query_bench) {
    if (page_table.empty()) {
      std::cerr << "--query-bench requires --table <name>\n";
      engine.shutdown();
      return 1;
    }
    const int rc = run_query_bench(engine, session_dir, page_table, query_bench_row_count, query_bench_page_size,
                                   query_bench_warmup, query_bench_iters, query_bench_varied,
                                   parse_query_bench_profile(query_bench_profile));
    engine.shutdown();
    return rc;
  }

  if (oltp_persist_micro) {
    const int rc = run_oltp_persist_micro(engine, session_dir, oltp_rows);
    engine.shutdown();
    return rc;
  }

  if (repl) {
    structdb::client::EmbedClient client(engine);
    if (!client.open(session_dir, &err)) {
      std::cerr << "embed open failed: " << err << "\n";
      engine.shutdown();
      return 1;
    }
    structdb::client::mdb::MdbInteractiveSession session;
    std::cout << "StructDB REPL (empty line or EXIT to exit). Example: USE(mytable)\n";
    std::string line;
    while (std::getline(std::cin, line)) {
      if (!line.empty() && line.back() == '\r') line.pop_back();
      auto not_space = [](unsigned char c) { return !std::isspace(c); };
      while (!line.empty() && !not_space(static_cast<unsigned char>(line.front()))) line.erase(line.begin());
      while (!line.empty() && !not_space(static_cast<unsigned char>(line.back()))) line.pop_back();
      if (line.empty()) break;
      std::vector<std::string> log;
      std::string e2;
      const auto r = structdb::client::mdb::mdb_repl_execute_line(engine, client, session, line, &log, false, false, &e2);
      for (const auto& outl : log) {
        std::cout << outl << "\n";
      }
      if (r.repl_exit_requested) {
        break;
      }
      if (!r.ok) {
        std::cerr << "error: " << r.last_error << "\n";
      }
    }
    client.close();
    engine.shutdown();
    return 0;
  }

  if (page_json && page_no > 0 && page_size > 0 && !page_table.empty()) {
    structdb::client::EmbedClient client(engine);
    if (!client.open(session_dir, &err)) {
      std::cerr << "embed open failed: " << err << "\n";
      engine.shutdown();
      return 1;
    }
    structdb::client::mdb::MdbInteractiveSession session;
    std::vector<std::string> log;
    std::string e2;
    {
      const std::string use_line = std::string("USE(") + page_table + ")";
      const auto r1 = structdb::client::mdb::mdb_repl_execute_line(engine, client, session, use_line, &log, false, false,
                                                                   &e2);
      if (!r1.ok) {
        std::cerr << "USE failed: " << r1.last_error << "\n";
        for (const auto& line : log) {
          std::cout << line << "\n";
        }
        client.close();
        engine.shutdown();
        return 1;
      }
    }
    std::ostringstream pj;
    pj << "PAGE_JSON(" << page_no << "," << page_size << "," << page_order << "," << (page_desc ? "desc" : "asc")
       << ")";
    const auto r2 = structdb::client::mdb::mdb_repl_execute_line(engine, client, session, pj.str(), &log, false, false,
                                                                 &e2);
    for (const auto& line : log) {
      std::cout << line << "\n";
    }
    if (!r2.ok) {
      std::cerr << "PAGE_JSON failed: " << r2.last_error << "\n";
      client.close();
      engine.shutdown();
      return 1;
    }
    client.close();
    engine.shutdown();
    return 0;
  }

  if (!run_mdb_path.empty()) {
    structdb::client::EmbedClient client(engine);
    if (!client.open(session_dir, &err)) {
      std::cerr << "embed open failed: " << err << "\n";
      engine.shutdown();
      return 1;
    }
    std::vector<std::string> log;
    structdb::client::mdb::MdbRunOptions opt;
    opt.script_path = run_mdb_path;
    opt.log_sink = &log;
    opt.bulk_import_mode = mdb_bulk_import;
    opt.persist_coalesce = mdb_persist_coalesce;
    opt.stream_log_lines = mdb_stream_log;
    const auto r = structdb::client::mdb::run_mdb_script(engine, client, opt);
    if (!mdb_stream_log) {
      for (const auto& line : log) {
        std::cout << line << "\n";
      }
    }
    if (!r.ok) {
      std::cerr << "mdb script failed at line " << r.last_line_no << ": " << r.last_error << "\n";
      client.close();
      engine.shutdown();
      return 1;
    }
    std::cout << "mdb script finished OK\n";
    client.close();
    engine.shutdown();
    return 0;
  }

  std::cout << "StructDB engine ready (data_dir=" << data_dir << ")\n";
  engine.shutdown();
  return 0;
}
