#include <filesystem>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include <cctype>

#include "structdb/client/embed_client.hpp"
#include "structdb/client/mdb_runner.hpp"
#include "structdb/facade/config.hpp"
#include "structdb/facade/engine.hpp"
#include "structdb/infra/logging.hpp"

#if defined(_MSC_VER)
#  include "structdb/infra/leak_detector.hpp"
#endif

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
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--data-dir" && i + 1 < argc) {
      data_dir = argv[++i];
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
    }
  }

  if (session_dir.empty()) {
    session_dir = (std::filesystem::path(data_dir) / "embed_session").string();
  }

  structdb::facade::Engine engine;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = data_dir;
  snap.version = 1;
  engine.config().update(1, snap);

  std::string err;
  if (!engine.startup(&err)) {
    std::cerr << "startup failed: " << err << "\n";
    return 1;
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
    const auto r = structdb::client::mdb::run_mdb_script(engine, client, opt);
    for (const auto& line : log) {
      std::cout << line << "\n";
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
