#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <string_view>

#include "structdb/client/embed_client.hpp"
#include "structdb/client/mdb_command_parser.hpp"
#include "structdb/client/mdb_runner.hpp"
#include "structdb/facade/engine.hpp"
#include "structdb/infra/long_task_progress.hpp"
#include "structdb/storage/mdb_keyspace.hpp"
#include "structdb/storage/storage_engine.hpp"

#include "test_artifact_env.hpp"

namespace fs = std::filesystem;

namespace {

fs::path temp_dir(const char* name) {
  return structdb::testing::test_artifact_run_root() / "structdb_mdb_tests" / name;
}

void write_script(const fs::path& p, std::string_view text) {
  std::ofstream out(p, std::ios::binary);
  ASSERT_TRUE(out.is_open());
  out << text;
}

bool parse_u64_after_key(std::string_view s, std::string_view key, std::uint64_t* out) {
  const auto p = s.find(key);
  if (p == std::string_view::npos) return false;
  std::size_t i = p + key.size();
  std::uint64_t v = 0;
  bool any = false;
  while (i < s.size() && s[i] >= '0' && s[i] <= '9') {
    any = true;
    v = v * 10u + static_cast<std::uint64_t>(s[i] - '0');
    ++i;
  }
  if (!any) return false;
  *out = v;
  return true;
}

std::string require_single_log_line(const std::vector<std::string>& log, std::string_view needle) {
  std::string hit;
  for (const auto& s : log) {
    if (s.find(needle) == std::string::npos) continue;
    if (!hit.empty()) {
      ADD_FAILURE() << "multiple log lines contain \"" << needle << "\"";
      return hit;
    }
    hit = s;
  }
  EXPECT_FALSE(hit.empty()) << "no log line contains \"" << needle << "\"";
  return hit;
}

/// 事务链严格测试：每条用例均从空目录起，经 **CREATE TABLE → USE → DEFATTR → LIST TABLES → COUNT(rows=0)** 再进入事务/快照断言。
struct TxnChainStrictRepl {
  fs::path root;
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap{};
  std::string err;
  structdb::client::EmbedClient client;
  structdb::client::mdb::MdbInteractiveSession session;
  std::vector<std::string> log;

  explicit TxnChainStrictRepl(const char* temp_subdir) : root(temp_dir(temp_subdir)), client(eng) {
    fs::remove_all(root);
    fs::create_directories(root);
    snap.data_dir = (root / "_data").string();
    eng.config().update(1, snap);
  }

  bool start() { return eng.startup(&err) && client.open(root / "session", &err); }

  structdb::client::mdb::MdbRunResult repl(std::string_view line) {
    return structdb::client::mdb::mdb_repl_execute_line(eng, client, session, line, &log, false, false, &err);
  }

  bool strict_new_table_from_create(std::string_view table_token, std::string_view defattr_command) {
    const std::string t(table_token);
    const std::string ct = "CREATE TABLE(" + t + ")";
    const std::string ut = "USE(" + t + ")";
    if (!repl(ct).ok) {
      ADD_FAILURE() << "CREATE TABLE failed: " << err;
      return false;
    }
    if (!repl(ut).ok) {
      ADD_FAILURE() << "USE failed: " << err;
      return false;
    }
    if (!repl(defattr_command).ok) {
      ADD_FAILURE() << "DEFATTR failed: " << err;
      return false;
    }
    log.clear();
    if (!repl("LIST TABLES").ok) {
      ADD_FAILURE() << "LIST TABLES failed: " << err;
      return false;
    }
    const std::string needle = "[TABLE] " + t;
    bool found = false;
    for (const auto& s : log) {
      if (s.find(needle) != std::string::npos) found = true;
    }
    if (!found) {
      ADD_FAILURE() << "LIST TABLES must include " << needle;
      return false;
    }
    log.clear();
    if (!repl("COUNT").ok) {
      ADD_FAILURE() << "COUNT failed: " << err;
      return false;
    }
    const std::string c = require_single_log_line(log, "[COUNT]");
    if (c.find("rows=0") == std::string::npos) {
      ADD_FAILURE() << "COUNT after fresh DDL expected rows=0, got: " << c;
      return false;
    }
    return true;
  }

  void close() {
    client.close();
    eng.shutdown();
  }
};

void assert_mdb_script_log_has_strict_ddl_prefix(const std::vector<std::string>& log,
                                                 std::string_view table_token) {
  std::ostringstream j;
  for (const auto& s : log) j << s << '\n';
  const std::string all = j.str();
  const std::string tab = std::string("[TABLE] ") + std::string(table_token);
  EXPECT_NE(all.find(tab), std::string::npos) << "script must list table " << tab;
  EXPECT_NE(all.find("[COUNT] rows=0"), std::string::npos) << "script must COUNT empty table after DEFATTR";
}

std::string join_logs(const std::vector<std::string>& log) {
  std::ostringstream j;
  for (const auto& s : log) j << s << '\n';
  return j.str();
}

/// Under per-run artifact root so paths never contain `)` (breaks IMPORTDIR `(...)` parsing on Windows).
fs::path phase25_cmd_test_root(const char* case_name) {
  return fs::absolute(structdb::testing::test_artifact_run_root() / "structdb_mdb_phase25_cmd" / case_name);
}

}  // namespace

TEST(Mdb, LowercaseCommandsAccepted) {
  const auto root = temp_dir("mdb_lower");
  fs::remove_all(root);
  fs::create_directories(root);

  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;

  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "session", &err)) << err;

  const fs::path script = root / "case_lower.mdb";
  write_script(script,
               "create table(tl)\n"
               "use(tl)\n"
               "defattr(name:string,age:int)\n"
               "insert(1,Alice,20)\n"
               "count\n"
               "page(1,10,id,desc)\n"
               "page_json(1,10,id,asc)\n");

  std::vector<std::string> log;
  structdb::client::mdb::MdbRunOptions opt;
  opt.script_path = script;
  opt.log_sink = &log;
  const auto r = structdb::client::mdb::run_mdb_script(eng, client, opt);
  ASSERT_TRUE(r.ok) << r.last_error;

  std::ostringstream joined;
  for (const auto& s : log) joined << s << '\n';
  const std::string log_text = joined.str();
  EXPECT_EQ(log_text.find("[ERR] unknown command"), std::string::npos);
  EXPECT_NE(log_text.find("[COUNT]"), std::string::npos);
  EXPECT_NE(log_text.find("[PAGE] table="), std::string::npos);
  EXPECT_NE(log_text.find("[PAGE_JSON]{"), std::string::npos);
  EXPECT_NE(log_text.find("\"headers\":"), std::string::npos);

  client.close();
  eng.shutdown();
}

TEST(Mdb, ScriptLongTaskReportsProgressAndHonoursCancel) {
  const auto root = temp_dir("mdb_long_task");
  fs::remove_all(root);
  fs::create_directories(root);

  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;

  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "session", &err)) << err;

  const fs::path script = root / "long_task.mdb";
  write_script(script,
               "CREATE TABLE(lt)\n"
               "USE(lt)\n"
               "DEFATTR(name:string)\n"
               "COUNT\n");

  structdb::infra::LongTaskReporter reporter(structdb::infra::LongTaskKind::MdbScript);
  std::vector<structdb::infra::LongTaskProgressSnapshot> samples;
  reporter.set_progress_callback([&](const structdb::infra::LongTaskProgressSnapshot& s) {
    samples.push_back(s);
    if (s.units_done == 1) reporter.cancel_token()->request_cancel();
  });

  structdb::client::mdb::MdbRunOptions opt;
  opt.script_path = script;
  opt.long_task = &reporter;
  const auto r = structdb::client::mdb::run_mdb_script(eng, client, opt);
  EXPECT_FALSE(r.ok);
  EXPECT_TRUE(r.cancelled);
  EXPECT_EQ(r.last_error, "cancelled");
  ASSERT_GE(samples.size(), 2u);
  EXPECT_EQ(samples.back().status, structdb::infra::LongTaskStatus::Cancelled);

  client.close();
  eng.shutdown();
}

TEST(Mdb, UpdateTypeMismatchStopsScript) {
  const auto root = temp_dir("mdb_stop");
  fs::remove_all(root);
  fs::create_directories(root);

  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;

  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "session", &err)) << err;

  const fs::path script = root / "case.mdb";
  write_script(script,
               "CREATE TABLE(tu)\n"
               "USE(tu)\n"
               "DEFATTR(name:string,age:int)\n"
               "INSERT(1,Alice,20)\n"
               "UPDATE(1,Alice,a)\n"
               "UPDATE(1,Alice,30)\n");

  std::vector<std::string> log;
  structdb::client::mdb::MdbRunOptions opt;
  opt.script_path = script;
  opt.log_sink = &log;
  const auto r = structdb::client::mdb::run_mdb_script(eng, client, opt);
  ASSERT_FALSE(r.ok);

  namespace mk = structdb::storage::mdb_keyspace;
  std::string blob;
  ASSERT_TRUE(eng.kv_get(mk::row_key("tu", "1"), &blob));
  std::string dec;
  ASSERT_TRUE(structdb::client::mdb::mdb_decode_stored_snapshot(blob, &dec));
  EXPECT_NE(dec.find("20"), std::string::npos);
  EXPECT_EQ(dec.find("30"), std::string::npos);

  client.close();
  eng.shutdown();
}

TEST(Mdb, DelattrRemovesColumn) {
  const auto root = temp_dir("mdb_delattr");
  fs::remove_all(root);
  fs::create_directories(root);

  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;

  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "session", &err)) << err;

  const fs::path script = root / "case_delattr.mdb";
  write_script(script,
               "CREATE TABLE(tda)\n"
               "USE(tda)\n"
               "DEFATTR(name:string,age:int)\n"
               "INSERT(1,Alice,20)\n"
               "INSERT(2,Bob,30)\n"
               "DELATTR(age)\n");

  std::vector<std::string> log;
  structdb::client::mdb::MdbRunOptions opt;
  opt.script_path = script;
  opt.log_sink = &log;
  const auto r = structdb::client::mdb::run_mdb_script(eng, client, opt);
  ASSERT_TRUE(r.ok) << r.last_error;

  std::ostringstream joined;
  for (const auto& s : log) joined << s << '\n';
  EXPECT_NE(joined.str().find("[DELATTR] ok: key=age"), std::string::npos);

  namespace mk = structdb::storage::mdb_keyspace;
  std::string schv;
  ASSERT_TRUE(eng.kv_get(std::string(mk::schema_key("tda")), &schv));
  std::string dec;
  ASSERT_TRUE(structdb::client::mdb::mdb_decode_stored_snapshot(schv, &dec));
  EXPECT_EQ(dec.find("age:int"), std::string::npos);

  client.close();
  eng.shutdown();
}

TEST(Mdb, WhereUpdateWhereTxn) {
  const auto root = temp_dir("mdb_where_txn");
  fs::remove_all(root);
  fs::create_directories(root);

  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;

  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "session", &err)) << err;

  const fs::path script = root / "w.mdb";
  write_script(script,
               "CREATE TABLE(wt)\n"
               "USE(wt)\n"
               "DEFATTR(name:string,age:int)\n"
               "INSERT(1,Alice,10)\n"
               "INSERT(2,Bob,20)\n"
               "BEGIN\n"
               "WHERE(age, >, 15)\n"
               "UPDATEWHERE(age, 99, WHERE, id, =, 2)\n"
               "COMMIT\n"
               "COUNT(age, =, 99)\n");

  std::vector<std::string> log;
  structdb::client::mdb::MdbRunOptions opt;
  opt.script_path = script;
  opt.log_sink = &log;
  const auto r = structdb::client::mdb::run_mdb_script(eng, client, opt);
  ASSERT_TRUE(r.ok) << r.last_error;

  std::ostringstream joined;
  for (const auto& s : log) joined << s << '\n';
  const std::string t = joined.str();
  EXPECT_NE(t.find("matched 1 / 2 rows"), std::string::npos) << t;
  EXPECT_NE(t.find("updated 1 rows"), std::string::npos) << t;
  EXPECT_NE(t.find("[COMMIT] ok"), std::string::npos) << t;

  client.close();
  eng.shutdown();
}

TEST(Mdb, JournalRecoverRestoresTable) {
  const auto root = temp_dir("mdb_rec");
  fs::remove_all(root);
  fs::create_directories(root);

  const fs::path script = root / "init.mdb";
  write_script(script,
               "CREATE TABLE(tr)\n"
               "USE(tr)\n"
               "DEFATTR(name:string)\n"
               "INSERT(1,Zed)\n");

  {
    structdb::facade::Engine eng;
    structdb::facade::EngineConfigSnapshot snap;
    snap.data_dir = (root / "_data").string();
    eng.config().update(1, snap);
    std::string err;
    ASSERT_TRUE(eng.startup(&err)) << err;
    structdb::client::EmbedClient client(eng);
    ASSERT_TRUE(client.open(root / "session", &err)) << err;
    structdb::client::mdb::MdbRunOptions opt;
    opt.script_path = script;
    ASSERT_TRUE(structdb::client::mdb::run_mdb_script(eng, client, opt).ok);
    client.close();
    eng.shutdown();
  }

  structdb::facade::Engine eng2;
  structdb::facade::EngineConfigSnapshot snap2;
  snap2.data_dir = (root / "_data").string();
  eng2.config().update(1, snap2);
  std::string err;
  ASSERT_TRUE(eng2.startup(&err)) << err;
  structdb::client::EmbedClient c2(eng2);
  ASSERT_TRUE(c2.open(root / "session", &err)) << err;

  namespace mk = structdb::storage::mdb_keyspace;
  std::string blob;
  ASSERT_TRUE(eng2.kv_get(mk::row_key("tr", "1"), &blob));
  std::string dec;
  ASSERT_TRUE(structdb::client::mdb::mdb_decode_stored_snapshot(blob, &dec));
  EXPECT_NE(dec.find("Zed"), std::string::npos);

  c2.close();
  eng2.shutdown();
}

TEST(Mdb, ReplSessionPersistsTxnAcrossLines) {
  const auto root = temp_dir("mdb_repl_txn");
  fs::remove_all(root);
  fs::create_directories(root);

  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "session", &err)) << err;

  structdb::client::mdb::MdbInteractiveSession session;
  std::vector<std::string> log;
  auto run = [&](const char* line) {
    return structdb::client::mdb::mdb_repl_execute_line(eng, client, session, line, &log, false, false, &err);
  };

  ASSERT_TRUE(run("CREATE TABLE(rt)").ok);
  ASSERT_TRUE(run("USE(rt)").ok);
  ASSERT_TRUE(run("DEFATTR(name:string)").ok);
  ASSERT_TRUE(run("BEGIN").ok);
  ASSERT_TRUE(run("INSERT(1,Hi)").ok);
  ASSERT_TRUE(run("COMMIT").ok);

  const fs::path script = root / "equiv.mdb";
  write_script(script,
               "CREATE TABLE(rt2)\n"
               "USE(rt2)\n"
               "DEFATTR(name:string)\n"
               "BEGIN\n"
               "INSERT(1,Hi)\n"
               "COMMIT\n");
  structdb::client::mdb::MdbRunOptions opt;
  opt.script_path = script;
  ASSERT_TRUE(structdb::client::mdb::run_mdb_script(eng, client, opt).ok);

  client.close();
  eng.shutdown();
}

TEST(Mdb, BulkInsertRenattrExportJson) {
  const auto root = temp_dir("mdb_bulk_ren_export");
  fs::remove_all(root);
  fs::create_directories(root);
  const fs::path json_path = root / "t.json";

  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "session", &err)) << err;

  std::ostringstream script;
  script << "CREATE TABLE(bx)\n"
         << "USE(bx)\n"
         << "DEFATTR(tag:string)\n"
         << "BULKINSERT(1,A|2,B)\n"
         << "RENATTR(tag,label)\n"
         << "EXPORT JSON " << json_path.generic_string() << "\n";

  const fs::path script_path = root / "run.mdb";
  write_script(script_path, script.str());

  std::vector<std::string> log;
  structdb::client::mdb::MdbRunOptions opt;
  opt.script_path = script_path;
  opt.log_sink = &log;
  ASSERT_TRUE(structdb::client::mdb::run_mdb_script(eng, client, opt).ok) << err;

  std::ifstream in(json_path);
  ASSERT_TRUE(in);
  const std::string json((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
  EXPECT_NE(json.find("\"id\": \"1\""), std::string::npos);
  EXPECT_NE(json.find("\"label\": \"A\""), std::string::npos);

  client.close();
  eng.shutdown();
}

TEST(Mdb, EngineKvReadSeqVisibility) {
  const auto root = temp_dir("mdb_kv_seq");
  fs::remove_all(root);
  fs::create_directories(root);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;

  const std::string k = "mdb$v2$cat$seqt";
  ASSERT_TRUE(eng.kv_put(k, "v1", false));
  const std::uint64_t mid = eng.latest_commit_seq();
  ASSERT_TRUE(eng.kv_put(k, "v2", false));

  std::string got;
  ASSERT_FALSE(eng.kv_get(k, &got, mid));
  ASSERT_TRUE(eng.kv_get(k, &got));
  EXPECT_EQ(got, "v2");

  eng.shutdown();
}

TEST(Mdb, TxnChainStorageReadSeqHelperAndShowSnapshot) {
  TxnChainStrictRepl H("mdb_txn_chain_seq");
  ASSERT_TRUE(H.start()) << H.err;
  ASSERT_TRUE(H.strict_new_table_from_create("tc", "DEFATTR(name:string)"));
  ASSERT_TRUE(H.repl("TXNISOLATION snapshot").ok);
  ASSERT_TRUE(H.repl("BEGIN").ok);

  H.log.clear();
  ASSERT_TRUE(H.repl("SHOW TXN").ok);
  const std::string show_txn_line = require_single_log_line(H.log, "[SHOW TXN]");
  std::uint64_t snap_seq = 0;
  ASSERT_TRUE(parse_u64_after_key(show_txn_line, "snap_seq=", &snap_seq));

  ASSERT_TRUE(H.repl("INSERT(1,InTxn)").ok);
  H.log.clear();
  ASSERT_TRUE(H.repl("COUNT").ok);
  EXPECT_NE(require_single_log_line(H.log, "[COUNT]").find("rows=1"), std::string::npos);

  const std::string bump_k = "mdb$v2$cat$txnchain_bump";
  ASSERT_TRUE(H.eng.kv_put(bump_k, "v", false));
  const std::uint64_t after_put = H.eng.latest_commit_seq();
  ASSERT_GT(after_put, snap_seq);

  H.log.clear();
  ASSERT_TRUE(H.repl("SHOW SNAPSHOT").ok);
  const std::string show_snap_line = require_single_log_line(H.log, "[SHOW SNAPSHOT]");
  std::uint64_t txn_storage = 0;
  ASSERT_TRUE(parse_u64_after_key(show_snap_line, "txn_storage_read_seq=", &txn_storage));
  EXPECT_EQ(txn_storage, snap_seq);
  EXPECT_EQ(structdb::client::mdb::mdb_storage_read_seq_for_script(H.eng, H.client, true, false, snap_seq),
             snap_seq);

  H.log.clear();
  ASSERT_TRUE(H.repl("COUNT").ok);
  EXPECT_NE(require_single_log_line(H.log, "[COUNT]").find("rows=1"), std::string::npos);

  ASSERT_TRUE(H.repl("COMMIT").ok);
  H.close();
}

TEST(Mdb, TxnChainTxnIsolationRejectedDuringOpenTxn) {
  TxnChainStrictRepl H("mdb_txn_iso_reject");
  ASSERT_TRUE(H.start()) << H.err;
  ASSERT_TRUE(H.strict_new_table_from_create("ti", "DEFATTR(x:string)"));
  ASSERT_TRUE(H.repl("BEGIN").ok);
  const auto r = H.repl("TXNISOLATION read_committed");
  EXPECT_FALSE(r.ok);
  EXPECT_NE(r.last_error.find("TXNISOLATION"), std::string::npos);
  ASSERT_TRUE(H.repl("ROLLBACK").ok);
  ASSERT_TRUE(H.repl("TXNISOLATION read_committed").ok);
  H.close();
}

TEST(Mdb, TxnChainReadCommittedShowSnapshotTracksLatest) {
  TxnChainStrictRepl H("mdb_txn_rc_snap");
  ASSERT_TRUE(H.start()) << H.err;
  ASSERT_TRUE(H.strict_new_table_from_create("trc", "DEFATTR(k:string)"));
  ASSERT_TRUE(H.repl("TXNISOLATION read_committed").ok);
  ASSERT_TRUE(H.repl("BEGIN").ok);

  ASSERT_TRUE(H.repl("INSERT(1,RcRow)").ok);
  H.log.clear();
  ASSERT_TRUE(H.repl("COUNT").ok);
  EXPECT_NE(require_single_log_line(H.log, "[COUNT]").find("rows=1"), std::string::npos);

  H.log.clear();
  ASSERT_TRUE(H.repl("SHOW SNAPSHOT").ok);
  const std::string s1 = require_single_log_line(H.log, "[SHOW SNAPSHOT]");
  std::uint64_t ts1 = 0;
  std::uint64_t el1 = 0;
  ASSERT_TRUE(parse_u64_after_key(s1, "txn_storage_read_seq=", &ts1));
  ASSERT_TRUE(parse_u64_after_key(s1, "engine_latest_commit_seq=", &el1));
  EXPECT_EQ(ts1, el1);

  ASSERT_TRUE(H.eng.kv_put("mdb$v2$cat$txnchain_rc_bump", "z", false));

  H.log.clear();
  ASSERT_TRUE(H.repl("SHOW SNAPSHOT").ok);
  const std::string s2 = require_single_log_line(H.log, "[SHOW SNAPSHOT]");
  std::uint64_t ts2 = 0;
  std::uint64_t el2 = 0;
  ASSERT_TRUE(parse_u64_after_key(s2, "txn_storage_read_seq=", &ts2));
  ASSERT_TRUE(parse_u64_after_key(s2, "engine_latest_commit_seq=", &el2));
  EXPECT_EQ(ts2, el2);
  EXPECT_GT(ts2, ts1);
  EXPECT_EQ(structdb::client::mdb::mdb_storage_read_seq_for_script(H.eng, H.client, true, true, 0), ts2);

  ASSERT_TRUE(H.repl("ROLLBACK").ok);
  H.close();
}

TEST(Mdb, TxnBeginPersistStorageSurvivesRollback) {
  namespace mk = structdb::storage::mdb_keyspace;
  const auto root = temp_dir("mdb_txn_persist_rb");
  fs::remove_all(root);
  fs::create_directories(root);

  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  ASSERT_TRUE(snap.mdb_persist_in_begin);
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "sess", &err)) << err;
  structdb::client::mdb::MdbInteractiveSession session;
  std::vector<std::string> log;

  auto run = [&](const char* line) {
    return structdb::client::mdb::mdb_repl_execute_line(eng, client, session, line, &log, false, false, nullptr);
  };
  ASSERT_TRUE(run("CREATE TABLE(tp)").ok);
  ASSERT_TRUE(run("USE(tp)").ok);
  ASSERT_TRUE(run("DEFATTR(k:string)").ok);
  ASSERT_TRUE(run("BEGIN").ok);
  ASSERT_TRUE(run("INSERT(1,RbRow)").ok);
  log.clear();
  ASSERT_TRUE(run("COUNT").ok);
  EXPECT_NE(require_single_log_line(log, "[COUNT]").find("rows=1"), std::string::npos);
  ASSERT_TRUE(run("ROLLBACK").ok);
  log.clear();
  ASSERT_TRUE(run("COUNT").ok);
  EXPECT_NE(require_single_log_line(log, "[COUNT]").find("rows=0"), std::string::npos);

  std::string v;
  ASSERT_TRUE(eng.kv_get(mk::row_key("tp", "1"), &v));
  std::string dec;
  ASSERT_TRUE(structdb::client::mdb::mdb_decode_stored_snapshot(v, &dec));
  EXPECT_NE(dec.find("RbRow"), std::string::npos);

  client.close();
  eng.shutdown();
}

TEST(Mdb, TxnBeginPersistDisabledNoStorageDuringTxn) {
  namespace mk = structdb::storage::mdb_keyspace;
  const auto root = temp_dir("mdb_txn_persist_off");
  fs::remove_all(root);
  fs::create_directories(root);

  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  snap.mdb_persist_in_begin = false;
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "sess", &err)) << err;
  structdb::client::mdb::MdbInteractiveSession session;
  std::vector<std::string> log;

  auto run = [&](const char* line) {
    return structdb::client::mdb::mdb_repl_execute_line(eng, client, session, line, &log, false, false, nullptr);
  };
  ASSERT_TRUE(run("CREATE TABLE(tq)").ok);
  ASSERT_TRUE(run("USE(tq)").ok);
  ASSERT_TRUE(run("DEFATTR(k:string)").ok);
  ASSERT_TRUE(run("BEGIN").ok);
  ASSERT_TRUE(run("INSERT(1,OffRow)").ok);
  ASSERT_TRUE(run("ROLLBACK").ok);

  std::string v;
  EXPECT_FALSE(eng.kv_get(mk::row_key("tq", "1"), &v));

  client.close();
  eng.shutdown();
}

TEST(Mdb, TxnBeginPersistChainRollbackPopsStorageWhenEnabled) {
  namespace mk = structdb::storage::mdb_keyspace;
  const auto root = temp_dir("mdb_txn_persist_chain_rb");
  fs::remove_all(root);
  fs::create_directories(root);

  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  snap.mdb_chain_rollback_on_mdb_rollback = true;
  ASSERT_TRUE(snap.mdb_persist_in_begin);
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  ASSERT_TRUE(eng.config().snapshot().mdb_chain_rollback_on_mdb_rollback);
  ASSERT_TRUE(eng.config().snapshot().mdb_persist_in_begin);
  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "sess", &err)) << err;
  structdb::client::mdb::MdbInteractiveSession session;
  std::vector<std::string> log;

  auto run = [&](const char* line) {
    return structdb::client::mdb::mdb_repl_execute_line(eng, client, session, line, &log, false, false, nullptr);
  };
  ASSERT_TRUE(run("CREATE TABLE(tc)").ok);
  ASSERT_TRUE(run("USE(tc)").ok);
  ASSERT_TRUE(run("DEFATTR(k:string)").ok);
  ASSERT_TRUE(run("BEGIN").ok);
  const std::size_t undo_d0 = eng.embed_undo_stack_depth();
  ASSERT_TRUE(run("INSERT(1,ChainRow)").ok);
  EXPECT_GT(eng.embed_undo_stack_depth(), undo_d0) << "versioned persist should grow undo_stack_";
  log.clear();
  ASSERT_TRUE(run("COUNT").ok);
  EXPECT_NE(require_single_log_line(log, "[COUNT]").find("rows=1"), std::string::npos);
  ASSERT_TRUE(run("ROLLBACK").ok);
  EXPECT_EQ(eng.embed_undo_stack_depth(), undo_d0) << "chain ROLLBACK should pop undo back to BEGIN depth";
  log.clear();
  ASSERT_TRUE(run("COUNT").ok);
  EXPECT_NE(require_single_log_line(log, "[COUNT]").find("rows=0"), std::string::npos);

  std::string v;
  EXPECT_FALSE(eng.kv_get(mk::row_key("tc", "1"), &v))
      << "chain rollback should remove persisted row when mdb_chain_rollback_on_mdb_rollback is true";

  client.close();
  eng.shutdown();
}

TEST(Mdb, TxnChainScriptSnapshotShowSnapshotConsistent) {
  const auto root = temp_dir("mdb_txn_script_snap");
  fs::remove_all(root);
  fs::create_directories(root);

  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "session", &err)) << err;

  const fs::path script = root / "txn_snap.mdb";
  write_script(script,
               "CREATE TABLE(tss)\n"
               "USE(tss)\n"
               "DEFATTR(a:string)\n"
               "LIST TABLES\n"
               "COUNT\n"
               "TXNISOLATION snapshot\n"
               "BEGIN\n"
               "SHOW TXN\n"
               "SHOW SNAPSHOT\n"
               "COMMIT\n");

  std::vector<std::string> log;
  structdb::client::mdb::MdbRunOptions opt;
  opt.script_path = script;
  opt.log_sink = &log;
  ASSERT_TRUE(structdb::client::mdb::run_mdb_script(eng, client, opt).ok) << err;
  assert_mdb_script_log_has_strict_ddl_prefix(log, "tss");

  const std::string txn_line = require_single_log_line(log, "[SHOW TXN]");
  const std::string snap_line = require_single_log_line(log, "[SHOW SNAPSHOT]");
  std::uint64_t snap_seq = 0;
  std::uint64_t txn_storage = 0;
  ASSERT_TRUE(parse_u64_after_key(txn_line, "snap_seq=", &snap_seq));
  ASSERT_TRUE(parse_u64_after_key(snap_line, "txn_storage_read_seq=", &txn_storage));
  EXPECT_EQ(txn_storage, snap_seq);

  client.close();
  eng.shutdown();
}

TEST(Mdb, TxnChainShowSnapshotNoTxnOmitsTxnStorage) {
  TxnChainStrictRepl H("mdb_txn_snap_no_txn");
  ASSERT_TRUE(H.start()) << H.err;
  ASSERT_TRUE(H.strict_new_table_from_create("tn", "DEFATTR(z:string)"));
  H.log.clear();
  ASSERT_TRUE(H.repl("SHOW SNAPSHOT").ok);
  const std::string line = require_single_log_line(H.log, "[SHOW SNAPSHOT]");
  EXPECT_EQ(line.find("txn_storage_read_seq="), std::string::npos);
  H.close();
}

TEST(Mdb, TxnChainAfterCommitShowSnapshotOmitsTxnStorage) {
  TxnChainStrictRepl H("mdb_txn_snap_after_commit");
  ASSERT_TRUE(H.start()) << H.err;
  ASSERT_TRUE(H.strict_new_table_from_create("tac", "DEFATTR(z:string)"));
  ASSERT_TRUE(H.repl("BEGIN").ok);
  ASSERT_TRUE(H.repl("COMMIT").ok);
  H.log.clear();
  ASSERT_TRUE(H.repl("SHOW SNAPSHOT").ok);
  const std::string line = require_single_log_line(H.log, "[SHOW SNAPSHOT]");
  EXPECT_EQ(line.find("txn_storage_read_seq="), std::string::npos);
  H.close();
}

TEST(Mdb, TxnChainScriptTxnIsolationDuringTxnFails) {
  const auto root = temp_dir("mdb_txn_script_iso_fail");
  fs::remove_all(root);
  fs::create_directories(root);

  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "session", &err)) << err;

  const fs::path script = root / "txn_iso_fail.mdb";
  write_script(script,
               "CREATE TABLE(tsf)\n"
               "USE(tsf)\n"
               "DEFATTR(a:string)\n"
               "LIST TABLES\n"
               "COUNT\n"
               "BEGIN\n"
               "TXNISOLATION snapshot\n"
               "COMMIT\n");

  std::vector<std::string> log;
  structdb::client::mdb::MdbRunOptions opt;
  opt.script_path = script;
  opt.log_sink = &log;
  const auto r = structdb::client::mdb::run_mdb_script(eng, client, opt);
  EXPECT_FALSE(r.ok);
  EXPECT_NE(r.last_error.find("TXNISOLATION"), std::string::npos);
  assert_mdb_script_log_has_strict_ddl_prefix(log, "tsf");

  client.close();
  eng.shutdown();
}

TEST(Mdb, TxnChainHelperNonTxnMatchesEmbedReadSnapshot) {
  TxnChainStrictRepl H("mdb_txn_helper_off");
  ASSERT_TRUE(H.start()) << H.err;
  ASSERT_TRUE(H.strict_new_table_from_create("th", "DEFATTR(z:string)"));
  const std::uint64_t embed_snap = H.client.read_snapshot_seq();
  EXPECT_EQ(structdb::client::mdb::mdb_storage_read_seq_for_script(H.eng, H.client, false, false, 0), embed_snap);
  H.close();
}

TEST(Mdb, TxnChainCommitWithoutTxnLogsNoActive) {
  TxnChainStrictRepl H("mdb_txn_commit_no_txn");
  ASSERT_TRUE(H.start()) << H.err;
  ASSERT_TRUE(H.strict_new_table_from_create("tcx", "DEFATTR(x:string)"));
  H.log.clear();
  ASSERT_TRUE(H.repl("COMMIT").ok);
  EXPECT_NE(join_logs(H.log).find("[COMMIT] no active txn"), std::string::npos);
  H.close();
}

TEST(Mdb, TxnChainRollbackWithoutTxnLogsNoActive) {
  TxnChainStrictRepl H("mdb_txn_rb_no_txn");
  ASSERT_TRUE(H.start()) << H.err;
  ASSERT_TRUE(H.strict_new_table_from_create("trx", "DEFATTR(x:string)"));
  H.log.clear();
  ASSERT_TRUE(H.repl("ROLLBACK").ok);
  EXPECT_NE(join_logs(H.log).find("[ROLLBACK] no active txn"), std::string::npos);
  H.close();
}

TEST(Mdb, TxnChainSavepointWithoutTxnFails) {
  TxnChainStrictRepl H("mdb_txn_sp_no_txn");
  ASSERT_TRUE(H.start()) << H.err;
  ASSERT_TRUE(H.strict_new_table_from_create("tsp0", "DEFATTR(x:string)"));
  const auto r = H.repl("SAVEPOINT sp1");
  EXPECT_FALSE(r.ok);
  EXPECT_NE(r.last_error.find("SAVEPOINT"), std::string::npos);
  H.close();
}

TEST(Mdb, TxnChainRollbackToSavepointWithoutTxnFails) {
  TxnChainStrictRepl H("mdb_txn_rts_no_txn");
  ASSERT_TRUE(H.start()) << H.err;
  ASSERT_TRUE(H.strict_new_table_from_create("trts", "DEFATTR(x:string)"));
  const auto r = H.repl("ROLLBACK TO SAVEPOINT sp");
  EXPECT_FALSE(r.ok);
  EXPECT_NE(r.last_error.find("ROLLBACK TO SAVEPOINT"), std::string::npos);
  H.close();
}

TEST(Mdb, TxnChainRollbackToSavepointUnknownName) {
  TxnChainStrictRepl H("mdb_txn_rts_unknown");
  ASSERT_TRUE(H.start()) << H.err;
  ASSERT_TRUE(H.strict_new_table_from_create("tuk", "DEFATTR(x:string)"));
  ASSERT_TRUE(H.repl("BEGIN").ok);
  ASSERT_TRUE(H.repl("SAVEPOINT s1").ok);
  const auto r = H.repl("ROLLBACK TO SAVEPOINT ghost");
  EXPECT_FALSE(r.ok);
  EXPECT_NE(r.last_error.find("unknown savepoint"), std::string::npos);
  ASSERT_TRUE(H.repl("ROLLBACK").ok);
  H.close();
}

TEST(Mdb, TxnChainRollbackToSavepointBareLineUnknownVerb) {
  TxnChainStrictRepl H("mdb_txn_rts_empty");
  ASSERT_TRUE(H.start()) << H.err;
  ASSERT_TRUE(H.strict_new_table_from_create("tmt", "DEFATTR(x:string)"));
  ASSERT_TRUE(H.repl("BEGIN").ok);
  // 行首 trim 会去掉尾部空白，无法形成 parser 所需的 `rollback to savepoint ` 前缀 + 空 tail；当前实现表现为 **parse 失败**。
  const auto r = H.repl("ROLLBACK TO SAVEPOINT");
  EXPECT_FALSE(r.ok);
  EXPECT_NE(r.last_error.find("unknown verb"), std::string::npos);
  ASSERT_TRUE(H.repl("ROLLBACK").ok);
  H.close();
}

TEST(Mdb, TxnChainDoubleBeginPreservesSnapSeq) {
  TxnChainStrictRepl H("mdb_txn_double_begin");
  ASSERT_TRUE(H.start()) << H.err;
  ASSERT_TRUE(H.strict_new_table_from_create("tdb", "DEFATTR(x:string)"));
  ASSERT_TRUE(H.repl("TXNISOLATION snapshot").ok);
  ASSERT_TRUE(H.repl("BEGIN").ok);
  H.log.clear();
  ASSERT_TRUE(H.repl("SHOW TXN").ok);
  std::uint64_t snap1 = 0;
  ASSERT_TRUE(parse_u64_after_key(require_single_log_line(H.log, "[SHOW TXN]"), "snap_seq=", &snap1));
  ASSERT_TRUE(H.repl("BEGIN").ok);
  H.log.clear();
  ASSERT_TRUE(H.repl("SHOW TXN").ok);
  std::uint64_t snap2 = 0;
  ASSERT_TRUE(parse_u64_after_key(require_single_log_line(H.log, "[SHOW TXN]"), "snap_seq=", &snap2));
  EXPECT_EQ(snap1, snap2);
  ASSERT_TRUE(H.repl("ROLLBACK").ok);
  H.close();
}

TEST(Mdb, TxnChainTxnIsolationReadCommittedWithSpaces) {
  TxnChainStrictRepl H("mdb_txn_iso_spaces");
  ASSERT_TRUE(H.start()) << H.err;
  ASSERT_TRUE(H.strict_new_table_from_create("tis", "DEFATTR(x:string)"));
  ASSERT_TRUE(H.repl("TXNISOLATION read committed").ok);
  ASSERT_TRUE(H.repl("BEGIN").ok);
  H.log.clear();
  ASSERT_TRUE(H.repl("SHOW TXN").ok);
  const std::string line = require_single_log_line(H.log, "[SHOW TXN]");
  EXPECT_NE(line.find("read_committed=yes"), std::string::npos);
  ASSERT_TRUE(H.repl("ROLLBACK").ok);
  H.close();
}

TEST(Mdb, TxnChainTxnIsolationUnknownTailUsesSnapshot) {
  TxnChainStrictRepl H("mdb_txn_iso_unknown");
  ASSERT_TRUE(H.start()) << H.err;
  ASSERT_TRUE(H.strict_new_table_from_create("tiu", "DEFATTR(x:string)"));
  ASSERT_TRUE(H.repl("TXNISOLATION garbage_mode_xyz").ok);
  ASSERT_TRUE(H.repl("BEGIN").ok);
  H.log.clear();
  ASSERT_TRUE(H.repl("SHOW TXN").ok);
  const std::string line = require_single_log_line(H.log, "[SHOW TXN]");
  EXPECT_NE(line.find("read_committed=no"), std::string::npos);
  ASSERT_TRUE(H.repl("ROLLBACK").ok);
  H.close();
}

TEST(Mdb, TxnChainAddAttrAfterDefattrAppendsColumn) {
  TxnChainStrictRepl H("mdb_txn_addattr");
  ASSERT_TRUE(H.start()) << H.err;
  ASSERT_TRUE(H.strict_new_table_from_create("tadd", "DEFATTR(a:string)"));
  ASSERT_TRUE(H.repl("INSERT(1,x)").ok);
  ASSERT_TRUE(H.repl("ADDATTR(b:int)").ok);
  ASSERT_TRUE(H.repl("INSERT(2,y,7)").ok);
  H.log.clear();
  ASSERT_TRUE(H.repl("COUNT").ok);
  EXPECT_NE(require_single_log_line(H.log, "[COUNT]").find("rows=2"), std::string::npos);
  H.close();
}

/// `ADDATTR` 要求先有 `DEFATTR`；仅 `CREATE`+`USE` 的空 schema 表应拒绝。
TEST(Mdb, AddAttrStrictEmptySchemaFails) {
  TxnChainStrictRepl H("mdb_addattr_strict_empty");
  ASSERT_TRUE(H.start()) << H.err;
  ASSERT_TRUE(H.repl("CREATE TABLE(tz0)").ok);
  ASSERT_TRUE(H.repl("USE(tz0)").ok);
  const auto r = H.repl("ADDATTR(x:int)");
  ASSERT_FALSE(r.ok);
  EXPECT_NE(r.last_error.find("schema empty"), std::string::npos) << r.last_error;
  H.close();
}

TEST(Mdb, AddAttrStrictDuplicateColumnFails) {
  TxnChainStrictRepl H("mdb_addattr_strict_dup");
  ASSERT_TRUE(H.start()) << H.err;
  ASSERT_TRUE(H.strict_new_table_from_create("tz1", "DEFATTR(a:string)"));
  ASSERT_TRUE(H.repl("ADDATTR(b:int)").ok);
  const auto r = H.repl("ADDATTR(a:string)");
  ASSERT_FALSE(r.ok);
  EXPECT_NE(r.last_error.find("duplicate"), std::string::npos) << r.last_error;
  H.close();
}

TEST(Mdb, AddAttrStrictMultiPairFails) {
  TxnChainStrictRepl H("mdb_addattr_strict_multi");
  ASSERT_TRUE(H.start()) << H.err;
  ASSERT_TRUE(H.strict_new_table_from_create("tz2", "DEFATTR(a:string)"));
  const auto r = H.repl("ADDATTR(b:int,c:int)");
  ASSERT_FALSE(r.ok);
  EXPECT_NE(r.last_error.find("exactly one"), std::string::npos) << r.last_error;
  H.close();
}

/// `ADDATTR(idx,name:type)` 在 schema 指定下标插入列，行向量同步插入默认值。
TEST(Mdb, AddAttrInsertsAtIndexMiddle) {
  TxnChainStrictRepl H("mdb_addattr_pos_mid");
  ASSERT_TRUE(H.start()) << H.err;
  ASSERT_TRUE(H.strict_new_table_from_create("tp", "DEFATTR(a:string,b:string)"));
  ASSERT_TRUE(H.repl("INSERT(1,xa,xb)").ok);
  ASSERT_TRUE(H.repl("ADDATTR(1,m:int)").ok);
  H.log.clear();
  ASSERT_TRUE(H.repl("PAGE_JSON(1,10,a,asc)").ok);
  const std::string j = join_logs(H.log);
  EXPECT_NE(j.find("\"name\":\"a\""), std::string::npos) << j;
  EXPECT_NE(j.find("\"name\":\"m\""), std::string::npos) << j;
  EXPECT_NE(j.find("\"name\":\"b\""), std::string::npos) << j;
  EXPECT_LT(j.find("\"name\":\"a\""), j.find("\"name\":\"m\"")) << j;
  EXPECT_LT(j.find("\"name\":\"m\""), j.find("\"name\":\"b\"")) << j;
  EXPECT_NE(j.find("xa"), std::string::npos) << j;
  EXPECT_NE(j.find("xb"), std::string::npos) << j;
  H.close();
}

TEST(Mdb, AddAttrIndexOutOfRangeFails) {
  TxnChainStrictRepl H("mdb_addattr_pos_oob");
  ASSERT_TRUE(H.start()) << H.err;
  ASSERT_TRUE(H.strict_new_table_from_create("toob", "DEFATTR(a:string,b:string)"));
  const auto r = H.repl("ADDATTR(3,m:int)");
  ASSERT_FALSE(r.ok);
  EXPECT_NE(r.last_error.find("out of range"), std::string::npos) << r.last_error;
  H.close();
}

/// `ADDATTR(...:string)` 为已有行填充占位默认值 `"0"`（与数值列默认一致，便于网格展示）。
TEST(Mdb, AddAttrAppendsStringDefaultZeroForExistingRows) {
  TxnChainStrictRepl H("mdb_addattr_str0");
  ASSERT_TRUE(H.start()) << H.err;
  ASSERT_TRUE(H.strict_new_table_from_create("tzs", "DEFATTR(id:int,name:string)"));
  ASSERT_TRUE(H.repl("INSERT(1,1,Alice)").ok);
  ASSERT_TRUE(H.repl("ADDATTR(phone:string)").ok);
  H.log.clear();
  ASSERT_TRUE(H.repl("PAGE_JSON(1,10,id,asc)").ok);
  const std::string j = join_logs(H.log);
  EXPECT_NE(j.find("Alice"), std::string::npos) << j;
  EXPECT_NE(j.find("\"phone\""), std::string::npos) << j;
  EXPECT_NE(j.find("\"Alice\",\"0\""), std::string::npos) << j;
  H.close();
}

/// 事务 v2 重放：`ADDATTR(1,...)` 写入带 TAB 的载荷，重启后会话恢复列序。
TEST(Mdb, ReplRestoresTxnV2PositionalAddAttrAfterRestart) {
  const auto root = temp_dir("mdb_repl_txn_v2_addattr_pos");
  fs::remove_all(root);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "repl_addpos", &err)) << err;
  structdb::client::mdb::MdbInteractiveSession session;
  std::vector<std::string> log;
  auto run = [&](std::string_view line, bool fsync_op) {
    return structdb::client::mdb::mdb_repl_execute_line(eng, client, session, line, &log, false, fsync_op, nullptr);
  };
  ASSERT_TRUE(run("CREATE TABLE(tr)", false).ok);
  ASSERT_TRUE(run("USE(tr)", false).ok);
  ASSERT_TRUE(run("DEFATTR(a:string,b:string)", false).ok);
  ASSERT_TRUE(run("INSERT(1,u1,u2)", false).ok);
  ASSERT_TRUE(run("BEGIN", false).ok);
  ASSERT_TRUE(run("ADDATTR(1,m:int)", true).ok);
  client.close();
  eng.shutdown();

  structdb::facade::Engine eng2;
  eng2.config().update(1, snap);
  ASSERT_TRUE(eng2.startup(&err)) << err;
  structdb::client::EmbedClient client2(eng2);
  ASSERT_TRUE(client2.open(root / "repl_addpos", &err)) << err;
  structdb::client::mdb::MdbInteractiveSession session2;
  std::vector<std::string> log2;
  ASSERT_TRUE(
      structdb::client::mdb::mdb_repl_execute_line(eng2, client2, session2, "PAGE_JSON(1,10,a,asc)", &log2, false, false, nullptr)
          .ok);
  const std::string j = join_logs(log2);
  EXPECT_NE(j.find("\"name\":\"m\""), std::string::npos) << j;
  EXPECT_LT(j.find("\"name\":\"a\""), j.find("\"name\":\"m\"")) << j;
  client2.close();
  eng2.shutdown();
}

TEST(Mdb, AddAttrStrictUnknownTypeFails) {
  TxnChainStrictRepl H("mdb_addattr_strict_type");
  ASSERT_TRUE(H.start()) << H.err;
  ASSERT_TRUE(H.strict_new_table_from_create("tz3", "DEFATTR(a:string)"));
  const auto r = H.repl("ADDATTR(x:widget)");
  ASSERT_FALSE(r.ok);
  EXPECT_NE(r.last_error.find("unknown type"), std::string::npos) << r.last_error;
  H.close();
}

TEST(Mdb, DelAttrStrictUnknownColumnFails) {
  TxnChainStrictRepl H("mdb_delattr_strict_unknown");
  ASSERT_TRUE(H.start()) << H.err;
  ASSERT_TRUE(H.strict_new_table_from_create("tz4", "DEFATTR(a:string)"));
  const auto r = H.repl("DELATTR(ghost)");
  ASSERT_FALSE(r.ok);
  EXPECT_NE(r.last_error.find("unknown column"), std::string::npos) << r.last_error;
  H.close();
}

/// 两表独立 schema：在 `ta` 上 `ADDATTR`/`DELATTR` 后，`tb` 的列集与行数不变。
TEST(Mdb, AddDelAttrMultiTableIsolationRepl) {
  TxnChainStrictRepl H("mdb_adddel_iso");
  ASSERT_TRUE(H.start()) << H.err;
  ASSERT_TRUE(H.repl("CREATE TABLE(ta_iso)").ok);
  ASSERT_TRUE(H.repl("USE(ta_iso)").ok);
  ASSERT_TRUE(H.repl("DEFATTR(a:string)").ok);
  ASSERT_TRUE(H.repl("INSERT(1,ua)").ok);
  ASSERT_TRUE(H.repl("CREATE TABLE(tb_iso)").ok);
  ASSERT_TRUE(H.repl("USE(tb_iso)").ok);
  ASSERT_TRUE(H.repl("DEFATTR(b:int,p:string)").ok);
  ASSERT_TRUE(H.repl("INSERT(1,10,pb)").ok);

  ASSERT_TRUE(H.repl("USE(ta_iso)").ok);
  ASSERT_TRUE(H.repl("ADDATTR(x:int)").ok);
  ASSERT_TRUE(H.repl("INSERT(2,ub,0)").ok);

  ASSERT_TRUE(H.repl("USE(tb_iso)").ok);
  H.log.clear();
  ASSERT_TRUE(H.repl("COUNT").ok);
  EXPECT_NE(require_single_log_line(H.log, "[COUNT]").find("rows=1"), std::string::npos) << join_logs(H.log);
  H.log.clear();
  ASSERT_TRUE(H.repl("SHOW ATTR").ok);
  {
    const std::string all = join_logs(H.log);
    EXPECT_NE(all.find("b:int"), std::string::npos) << all;
    EXPECT_NE(all.find("p:string"), std::string::npos) << all;
    EXPECT_EQ(all.find("x:int"), std::string::npos) << "tb must not pick up ta's ADDATTR column";
  }

  ASSERT_TRUE(H.repl("USE(ta_iso)").ok);
  ASSERT_TRUE(H.repl("DELATTR(a)").ok);
  H.log.clear();
  ASSERT_TRUE(H.repl("COUNT").ok);
  EXPECT_NE(require_single_log_line(H.log, "[COUNT]").find("rows=2"), std::string::npos);

  ASSERT_TRUE(H.repl("USE(tb_iso)").ok);
  H.log.clear();
  ASSERT_TRUE(H.repl("COUNT").ok);
  EXPECT_NE(require_single_log_line(H.log, "[COUNT]").find("rows=1"), std::string::npos);
  H.log.clear();
  ASSERT_TRUE(H.repl("SHOW ATTR").ok);
  {
    const std::string all = join_logs(H.log);
    EXPECT_NE(all.find("b:int"), std::string::npos) << all;
    EXPECT_NE(all.find("p:string"), std::string::npos) << all;
  }
  H.close();
}

/// 多行非空数据删掉数值列再 `ADDATTR` 恢复：新列应为类型默认值，且行数不变。
TEST(Mdb, DelAttrManyRowsReAddAttrDefaults) {
  TxnChainStrictRepl H("mdb_adddel_many");
  ASSERT_TRUE(H.start()) << H.err;
  ASSERT_TRUE(H.strict_new_table_from_create("tmany", "DEFATTR(k:string,v:int)"));
  for (int i = 1; i <= 35; ++i) {
    std::ostringstream ins;
    ins << "INSERT(" << i << ",k" << i << "," << (i * 7) << ")";
    ASSERT_TRUE(H.repl(ins.str()).ok) << H.err << " line " << i;
  }
  H.log.clear();
  ASSERT_TRUE(H.repl("COUNT").ok);
  EXPECT_NE(require_single_log_line(H.log, "[COUNT]").find("rows=35"), std::string::npos);

  ASSERT_TRUE(H.repl("DELATTR(v)").ok);
  ASSERT_TRUE(H.repl("ADDATTR(v:int)").ok);
  H.log.clear();
  ASSERT_TRUE(H.repl("WHERE(v,=,0)").ok);
  EXPECT_NE(join_logs(H.log).find("matched 35"), std::string::npos) << join_logs(H.log);
  H.log.clear();
  ASSERT_TRUE(H.repl("COUNT").ok);
  EXPECT_NE(require_single_log_line(H.log, "[COUNT]").find("rows=35"), std::string::npos);
  H.close();
}

/// 嵌套：事务内 `SAVEPOINT` → `ADDATTR`/`UPDATE`/`INSERT` → `DELATTR` → `ROLLBACK TO` 恢复列与数据；
/// 提交后再跨表切换验证另一表未受影响。
TEST(Mdb, AddDelAttrNestedTxnSavepointAndCrossTable) {
  TxnChainStrictRepl H("mdb_adddel_nested");
  ASSERT_TRUE(H.start()) << H.err;
  ASSERT_TRUE(H.strict_new_table_from_create("tnest", "DEFATTR(tag:string,val:int)"));
  ASSERT_TRUE(H.repl("INSERT(1,t1,10)").ok);
  ASSERT_TRUE(H.repl("INSERT(2,t2,20)").ok);

  ASSERT_TRUE(H.repl("BEGIN").ok);
  ASSERT_TRUE(H.repl("SAVEPOINT s0").ok);
  ASSERT_TRUE(H.repl("ADDATTR(score:int)").ok);
  ASSERT_TRUE(H.repl("UPDATE(1,t1,10,100)").ok);
  ASSERT_TRUE(H.repl("UPDATE(2,t2,20,200)").ok);
  ASSERT_TRUE(H.repl("INSERT(3,t3,30,300)").ok);
  ASSERT_TRUE(H.repl("SAVEPOINT s1").ok);
  ASSERT_TRUE(H.repl("DELATTR(tag)").ok);
  ASSERT_TRUE(H.repl("ROLLBACK TO SAVEPOINT s1").ok);

  H.log.clear();
  ASSERT_TRUE(H.repl("WHERE(tag,=,t1)").ok);
  EXPECT_NE(join_logs(H.log).find("matched 1"), std::string::npos) << join_logs(H.log);
  H.log.clear();
  ASSERT_TRUE(H.repl("WHERE(score,=,100)").ok);
  EXPECT_NE(join_logs(H.log).find("matched 1"), std::string::npos) << join_logs(H.log);

  ASSERT_TRUE(H.repl("COMMIT").ok);

  ASSERT_TRUE(H.repl("CREATE TABLE(tnest_other)").ok);
  ASSERT_TRUE(H.repl("USE(tnest_other)").ok);
  ASSERT_TRUE(H.repl("DEFATTR(q:string)").ok);
  ASSERT_TRUE(H.repl("INSERT(1,only)").ok);
  ASSERT_TRUE(H.repl("USE(tnest)").ok);
  H.log.clear();
  ASSERT_TRUE(H.repl("COUNT").ok);
  EXPECT_NE(require_single_log_line(H.log, "[COUNT]").find("rows=3"), std::string::npos);
  ASSERT_TRUE(H.repl("USE(tnest_other)").ok);
  H.log.clear();
  ASSERT_TRUE(H.repl("COUNT").ok);
  EXPECT_NE(require_single_log_line(H.log, "[COUNT]").find("rows=1"), std::string::npos);
  H.close();
}

/// 脚本多表：交替 `USE` + 属性变更，断言两表行数与 `DELATTR` 日志互不串线。
TEST(Mdb, AddDelAttrMultiTableScriptIsolation) {
  const auto root = temp_dir("mdb_adddel_script_iso");
  fs::remove_all(root);
  fs::create_directories(root);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "session", &err)) << err;

  const fs::path script = root / "iso.mdb";
  write_script(script,
               "CREATE TABLE(tx)\n"
               "USE(tx)\n"
               "DEFATTR(u:string)\n"
               "INSERT(1,ax)\n"
               "CREATE TABLE(ty)\n"
               "USE(ty)\n"
               "DEFATTR(v:int,w:string)\n"
               "INSERT(1,1,wy)\n"
               "USE(tx)\n"
               "ADDATTR(m:int)\n"
               "INSERT(2,bx,0)\n"
               "USE(ty)\n"
               "COUNT\n"
               "USE(tx)\n"
               "DELATTR(u)\n"
               "COUNT\n"
               "USE(ty)\n"
               "COUNT\n"
               "DELATTR(w)\n"
               "ADDATTR(w:string)\n"
               "INSERT(2,2,w2)\n"
               "COUNT\n");

  std::vector<std::string> log;
  structdb::client::mdb::MdbRunOptions opt;
  opt.script_path = script;
  opt.log_sink = &log;
  const auto r = structdb::client::mdb::run_mdb_script(eng, client, opt);
  ASSERT_TRUE(r.ok) << r.last_error;

  const std::string all = join_logs(log);
  EXPECT_NE(all.find("[DELATTR] ok: key=u"), std::string::npos) << all;
  EXPECT_NE(all.find("[DELATTR] ok: key=w"), std::string::npos) << all;
  EXPECT_NE(all.find("[ADDATTR] ok:"), std::string::npos) << all;

  std::vector<std::string> count_lines;
  for (const auto& s : log) {
    if (s.find("[COUNT]") != std::string::npos) count_lines.push_back(s);
  }
  ASSERT_GE(count_lines.size(), 4u) << all;
  EXPECT_NE(count_lines[0].find("rows=1"), std::string::npos) << count_lines[0];
  EXPECT_NE(count_lines[1].find("rows=2"), std::string::npos) << count_lines[1];
  EXPECT_NE(count_lines[2].find("rows=1"), std::string::npos) << count_lines[2];
  EXPECT_NE(count_lines[3].find("rows=2"), std::string::npos) << count_lines[3];

  client.close();
  eng.shutdown();
}

TEST(Mdb, TxnChainDefattrDuplicateFails) {
  TxnChainStrictRepl H("mdb_txn_defattr_dup");
  ASSERT_TRUE(H.start()) << H.err;
  ASSERT_TRUE(H.strict_new_table_from_create("tdd", "DEFATTR(a:string)"));
  const auto r = H.repl("DEFATTR(b:string)");
  EXPECT_FALSE(r.ok);
  EXPECT_NE(r.last_error.find("schema already set"), std::string::npos);
  H.close();
}

TEST(Mdb, TxnChainCreateTableWhenExistsFails) {
  TxnChainStrictRepl H("mdb_txn_create_dup");
  ASSERT_TRUE(H.start()) << H.err;
  ASSERT_TRUE(H.strict_new_table_from_create("tdup", "DEFATTR(a:string)"));
  const auto r = H.repl("CREATE TABLE(tdup)");
  EXPECT_FALSE(r.ok);
  EXPECT_NE(r.last_error.find("table exists"), std::string::npos);
  H.close();
}

TEST(Mdb, TxnChainShowTxnWithoutActiveNoSnapLine) {
  TxnChainStrictRepl H("mdb_txn_show_txn_idle");
  ASSERT_TRUE(H.start()) << H.err;
  ASSERT_TRUE(H.strict_new_table_from_create("tst", "DEFATTR(x:string)"));
  H.log.clear();
  ASSERT_TRUE(H.repl("SHOW TXN").ok);
  const std::string t = join_logs(H.log);
  EXPECT_NE(t.find("active=no"), std::string::npos);
  EXPECT_EQ(t.find("snap_seq="), std::string::npos);
  H.close();
}

TEST(Mdb, TxnChainSavepointParseBareWordFails) {
  TxnChainStrictRepl H("mdb_txn_sp_parse");
  ASSERT_TRUE(H.start()) << H.err;
  ASSERT_TRUE(H.strict_new_table_from_create("tpr", "DEFATTR(x:string)"));
  const auto r = H.repl("SAVEPOINT");
  EXPECT_FALSE(r.ok);
  EXPECT_NE(r.last_error.find("SAVEPOINT"), std::string::npos);
  H.close();
}

TEST(Mdb, TxnChainSavepointRollbackToHappyPath) {
  TxnChainStrictRepl H("mdb_txn_sp_path");
  ASSERT_TRUE(H.start()) << H.err;
  ASSERT_TRUE(H.strict_new_table_from_create("sph", "DEFATTR(k:string)"));
  ASSERT_TRUE(H.repl("BEGIN").ok);
  ASSERT_TRUE(H.repl("INSERT(1,a)").ok);
  ASSERT_TRUE(H.repl("SAVEPOINT s1").ok);
  ASSERT_TRUE(H.repl("INSERT(2,b)").ok);
  H.log.clear();
  ASSERT_TRUE(H.repl("COUNT").ok);
  EXPECT_NE(require_single_log_line(H.log, "[COUNT]").find("rows=2"), std::string::npos);
  ASSERT_TRUE(H.repl("ROLLBACK TO SAVEPOINT s1").ok);
  H.log.clear();
  ASSERT_TRUE(H.repl("COUNT").ok);
  EXPECT_NE(require_single_log_line(H.log, "[COUNT]").find("rows=1"), std::string::npos);
  ASSERT_TRUE(H.repl("INSERT(2,c)").ok);
  H.log.clear();
  ASSERT_TRUE(H.repl("COUNT").ok);
  EXPECT_NE(require_single_log_line(H.log, "[COUNT]").find("rows=2"), std::string::npos);
  ASSERT_TRUE(H.repl("ROLLBACK").ok);
  H.log.clear();
  ASSERT_TRUE(H.repl("COUNT").ok);
  EXPECT_NE(require_single_log_line(H.log, "[COUNT]").find("rows=0"), std::string::npos);
  H.close();
}

TEST(Mdb, TxnChainScriptCommitWithoutBeginSucceeds) {
  const auto root = temp_dir("mdb_txn_script_commit_idle");
  fs::remove_all(root);
  fs::create_directories(root);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "session", &err)) << err;
  const fs::path script = root / "commit_idle.mdb";
  write_script(script,
               "CREATE TABLE(tcb)\n"
               "USE(tcb)\n"
               "DEFATTR(x:string)\n"
               "LIST TABLES\n"
               "COUNT\n"
               "COMMIT\n");
  std::vector<std::string> log;
  structdb::client::mdb::MdbRunOptions opt;
  opt.script_path = script;
  opt.log_sink = &log;
  ASSERT_TRUE(structdb::client::mdb::run_mdb_script(eng, client, opt).ok) << err;
  assert_mdb_script_log_has_strict_ddl_prefix(log, "tcb");
  EXPECT_NE(join_logs(log).find("[COMMIT] no active txn"), std::string::npos);
  client.close();
  eng.shutdown();
}

TEST(Mdb, TxnChainScriptSavepointWithoutBeginFails) {
  const auto root = temp_dir("mdb_txn_script_sp_fail");
  fs::remove_all(root);
  fs::create_directories(root);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "session", &err)) << err;
  const fs::path script = root / "sp_fail.mdb";
  write_script(script,
               "CREATE TABLE(tsv)\n"
               "USE(tsv)\n"
               "DEFATTR(x:string)\n"
               "LIST TABLES\n"
               "COUNT\n"
               "SAVEPOINT sp1\n");
  std::vector<std::string> log;
  structdb::client::mdb::MdbRunOptions opt;
  opt.script_path = script;
  opt.log_sink = &log;
  const auto r = structdb::client::mdb::run_mdb_script(eng, client, opt);
  EXPECT_FALSE(r.ok);
  EXPECT_NE(r.last_error.find("SAVEPOINT"), std::string::npos);
  assert_mdb_script_log_has_strict_ddl_prefix(log, "tsv");
  client.close();
  eng.shutdown();
}

TEST(Mdb, PersistClearsStaleSecondaryIndex) {
  const auto root = temp_dir("mdb_idx_clear");
  fs::remove_all(root);
  fs::create_directories(root);

  const fs::path script = root / "idx.mdb";
  write_script(script,
               "CREATE TABLE(ix)\n"
               "USE(ix)\n"
               "DEFATTR(name:string)\n"
               "INSERT(1,Alice)\n"
               "SETATTR(1,name,Bob)\n");

  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "session", &err)) << err;
  structdb::client::mdb::MdbRunOptions opt;
  opt.script_path = script;
  ASSERT_TRUE(structdb::client::mdb::run_mdb_script(eng, client, opt).ok);
  ASSERT_TRUE(eng.storage()->flush_memtable(nullptr));

  namespace mk = structdb::storage::mdb_keyspace;
  const std::string pfx = std::string(mk::kSecIdx) + "ix$name$";
  std::size_t n = 0;
  eng.kv_visit_prefix(pfx, [&](std::string_view key, std::string_view) {
    (void)key;
    ++n;
    return true;
  });
  EXPECT_EQ(n, 1u);

  client.close();
  eng.shutdown();
}

TEST(Mdb, ScriptFailsOnUnclosedTxnWhenOptSet) {
  const auto root = temp_dir("mdb_unclosed");
  fs::remove_all(root);
  fs::create_directories(root);

  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "session", &err)) << err;

  const fs::path script = root / "unclosed.mdb";
  write_script(script,
               "CREATE TABLE(tx)\n"
               "USE(tx)\n"
               "DEFATTR(k:string)\n"
               "BEGIN\n"
               "INSERT(1,a)\n");

  structdb::client::mdb::MdbRunOptions opt;
  opt.script_path = script;
  opt.fail_if_unclosed_txn = true;
  const auto r = structdb::client::mdb::run_mdb_script(eng, client, opt);
  ASSERT_FALSE(r.ok);
  EXPECT_FALSE(r.last_error.empty());

  client.close();
  eng.shutdown();
}

TEST(Mdb, TwoEmbedSessionsStaleReadSnapshot) {
  const auto root = temp_dir("mdb_two_embed");
  fs::remove_all(root);
  fs::create_directories(root);

  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;

  structdb::client::EmbedClient ca(eng);
  structdb::client::EmbedClient cb(eng);
  ASSERT_TRUE(ca.open(root / "sessA", &err)) << err;

  const fs::path s1 = root / "step1.mdb";
  write_script(s1,
               "CREATE TABLE(ts)\n"
               "USE(ts)\n"
               "DEFATTR(k:string)\n"
               "INSERT(1,a)\n");
  structdb::client::mdb::MdbRunOptions o1;
  o1.script_path = s1;
  ASSERT_TRUE(structdb::client::mdb::run_mdb_script(eng, ca, o1).ok) << err;

  ASSERT_TRUE(cb.open(root / "sessB", &err)) << err;
  const std::uint64_t snap_b = cb.read_snapshot_seq();

  const fs::path s2 = root / "step2.mdb";
  write_script(s2,
               "USE(ts)\n"
               "INSERT(2,b)\n");
  structdb::client::mdb::MdbRunOptions o2;
  o2.script_path = s2;
  ASSERT_TRUE(structdb::client::mdb::run_mdb_script(eng, ca, o2).ok) << err;

  namespace mk = structdb::storage::mdb_keyspace;
  std::string v;
  EXPECT_FALSE(eng.kv_get(mk::row_key("ts", "2"), &v, snap_b));

  cb.refresh_read_snapshot();
  ASSERT_TRUE(eng.kv_get(mk::row_key("ts", "2"), &v, cb.read_snapshot_seq()));
  EXPECT_NE(v.find('b'), std::string::npos);

  ca.close();
  cb.close();
  eng.shutdown();
}

TEST(Mdb, TwoEmbedSessionsAfterEngineRestart) {
  const auto root = temp_dir("mdb_two_embed_restart");
  fs::remove_all(root);
  fs::create_directories(root);

  namespace mk = structdb::storage::mdb_keyspace;
  const auto row2 = mk::row_key("tr", "2");

  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;

  structdb::client::EmbedClient ca(eng);
  structdb::client::EmbedClient cb(eng);
  ASSERT_TRUE(ca.open(root / "sessA", &err)) << err;

  const fs::path s1 = root / "step1.mdb";
  write_script(s1,
               "CREATE TABLE(tr)\n"
               "USE(tr)\n"
               "DEFATTR(k:string)\n"
               "INSERT(1,a)\n");
  structdb::client::mdb::MdbRunOptions o1;
  o1.script_path = s1;
  ASSERT_TRUE(structdb::client::mdb::run_mdb_script(eng, ca, o1).ok) << err;

  ASSERT_TRUE(cb.open(root / "sessB", &err)) << err;
  const std::uint64_t snap_b = cb.read_snapshot_seq();

  const fs::path s2 = root / "step2.mdb";
  write_script(s2,
               "USE(tr)\n"
               "INSERT(2,b)\n");
  structdb::client::mdb::MdbRunOptions o2;
  o2.script_path = s2;
  ASSERT_TRUE(structdb::client::mdb::run_mdb_script(eng, ca, o2).ok) << err;

  std::string v;
  EXPECT_FALSE(eng.kv_get(row2, &v, snap_b));

  ca.close();
  cb.close();
  eng.shutdown();

  structdb::facade::Engine eng2;
  structdb::facade::EngineConfigSnapshot snap2;
  snap2.data_dir = (root / "_data").string();
  snap2.version = 1;
  eng2.config().update(1, snap2);
  ASSERT_TRUE(eng2.startup(&err)) << err;

  structdb::client::EmbedClient cb2(eng2);
  ASSERT_TRUE(cb2.open(root / "sessB", &err)) << err;
  EXPECT_FALSE(eng2.kv_get(row2, &v, snap_b));
  cb2.refresh_read_snapshot();
  ASSERT_TRUE(eng2.kv_get(row2, &v, cb2.read_snapshot_seq()));
  EXPECT_NE(v.find('b'), std::string::npos);

  std::string v1;
  ASSERT_TRUE(eng2.kv_get(mk::row_key("tr", "1"), &v1, cb2.read_snapshot_seq()));

  cb2.close();
  eng2.shutdown();
}

TEST(Mdb, TwoReplSessionsAfterEngineRestart) {
  const auto root = temp_dir("mdb_two_repl_restart");
  fs::remove_all(root);

  namespace mk = structdb::storage::mdb_keyspace;
  const auto row2 = mk::row_key("tr", "2");

  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;

  structdb::client::EmbedClient ca(eng);
  structdb::client::EmbedClient cb(eng);
  ASSERT_TRUE(ca.open(root / "replA", &err)) << err;

  structdb::client::mdb::MdbInteractiveSession sa;
  std::vector<std::string> log_a;
  auto ra = [&](const char* line) {
    return structdb::client::mdb::mdb_repl_execute_line(eng, ca, sa, line, &log_a, false, false, nullptr);
  };
  ASSERT_TRUE(ra("CREATE TABLE(tr)").ok);
  ASSERT_TRUE(ra("USE(tr)").ok);
  ASSERT_TRUE(ra("DEFATTR(k:string)").ok);
  ASSERT_TRUE(ra("INSERT(1,a)").ok);

  ASSERT_TRUE(cb.open(root / "replB", &err)) << err;
  const std::uint64_t snap_b = cb.read_snapshot_seq();

  ASSERT_TRUE(ra("USE(tr)").ok);
  ASSERT_TRUE(ra("INSERT(2,b)").ok);

  std::string v;
  EXPECT_FALSE(eng.kv_get(row2, &v, snap_b));

  ca.close();
  cb.close();
  eng.shutdown();

  structdb::facade::Engine eng2;
  structdb::facade::EngineConfigSnapshot snap2;
  snap2.data_dir = (root / "_data").string();
  snap2.version = 1;
  eng2.config().update(1, snap2);
  ASSERT_TRUE(eng2.startup(&err)) << err;

  structdb::client::EmbedClient cb2(eng2);
  ASSERT_TRUE(cb2.open(root / "replB", &err)) << err;
  EXPECT_FALSE(eng2.kv_get(row2, &v, snap_b));
  cb2.refresh_read_snapshot();
  ASSERT_TRUE(eng2.kv_get(row2, &v, cb2.read_snapshot_seq()));
  EXPECT_NE(v.find('b'), std::string::npos);

  std::string v1;
  ASSERT_TRUE(eng2.kv_get(mk::row_key("tr", "1"), &v1, cb2.read_snapshot_seq()));

  cb2.close();
  eng2.shutdown();
}

TEST(Mdb, V2PersistSkipsV1SnapshotKey) {
  const auto root = temp_dir("mdb_v2_no_v1");
  fs::remove_all(root);
  fs::create_directories(root);

  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "session", &err)) << err;

  const fs::path script = root / "v2.mdb";
  write_script(script,
               "CREATE TABLE(tv)\n"
               "USE(tv)\n"
               "DEFATTR(x:string)\n"
               "INSERT(1,z)\n");
  structdb::client::mdb::MdbRunOptions opt;
  opt.script_path = script;
  ASSERT_TRUE(structdb::client::mdb::run_mdb_script(eng, client, opt).ok);

  std::string blob;
  EXPECT_FALSE(eng.kv_get(structdb::client::mdb::mdb_table_snapshot_key("tv"), &blob));
  namespace mk = structdb::storage::mdb_keyspace;
  ASSERT_TRUE(eng.kv_get(std::string(mk::catalog_key("tv")), &blob) && !blob.empty());

  client.close();
  eng.shutdown();
}

TEST(Mdb, ReplRestoresTxnFromSessionTxn) {
  const auto root = temp_dir("mdb_repl_txn");
  fs::remove_all(root);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "repl_sess", &err)) << err;
  structdb::client::mdb::MdbInteractiveSession session;
  std::vector<std::string> log1;
  ASSERT_TRUE(
      structdb::client::mdb::mdb_repl_execute_line(eng, client, session, "CREATE TABLE(tr)", &log1, false, false, nullptr).ok);
  ASSERT_TRUE(structdb::client::mdb::mdb_repl_execute_line(eng, client, session, "USE(tr)", &log1, false, false, nullptr).ok);
  ASSERT_TRUE(
      structdb::client::mdb::mdb_repl_execute_line(eng, client, session, "DEFATTR(a:string)", &log1, false, false, nullptr).ok);
  ASSERT_TRUE(structdb::client::mdb::mdb_repl_execute_line(eng, client, session, "INSERT(1,u)", &log1, false, false, nullptr).ok);
  ASSERT_TRUE(structdb::client::mdb::mdb_repl_execute_line(eng, client, session, "BEGIN", &log1, false, false, nullptr).ok);
  ASSERT_TRUE(fs::exists(root / "repl_sess" / structdb::client::kEmbedSessionArtifactsDir / "session.txn"));
  client.close();
  eng.shutdown();

  structdb::facade::Engine eng2;
  eng2.config().update(1, snap);
  ASSERT_TRUE(eng2.startup(&err)) << err;
  structdb::client::EmbedClient client2(eng2);
  ASSERT_TRUE(client2.open(root / "repl_sess", &err)) << err;
  structdb::client::mdb::MdbInteractiveSession session2;
  std::vector<std::string> log2;
  const auto r =
      structdb::client::mdb::mdb_repl_execute_line(eng2, client2, session2, "SHOW TXN", &log2, false, false, nullptr);
  ASSERT_TRUE(r.ok);
  bool found_recover = false;
  bool found_active = false;
  for (const auto& ln : log2) {
    if (ln.find("[RECOVER]") != std::string::npos) found_recover = true;
    if (ln.find("active=yes") != std::string::npos) found_active = true;
  }
  EXPECT_TRUE(found_recover);
  EXPECT_TRUE(found_active);
  client2.close();
  eng2.shutdown();
}

TEST(Mdb, ReplRestoresTxnV2MultiInsertAfterRestart) {
  const auto root = temp_dir("mdb_repl_txn_v2");
  fs::remove_all(root);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "repl_v2", &err)) << err;
  structdb::client::mdb::MdbInteractiveSession session;
  std::vector<std::string> log;
  auto run = [&](std::string_view line, bool fsync_op) {
    return structdb::client::mdb::mdb_repl_execute_line(eng, client, session, line, &log, false, fsync_op, nullptr);
  };
  ASSERT_TRUE(run("CREATE TABLE(tr)", false).ok);
  ASSERT_TRUE(run("USE(tr)", false).ok);
  ASSERT_TRUE(run("DEFATTR(a:string)", false).ok);
  ASSERT_TRUE(run("INSERT(1,u1)", false).ok);
  ASSERT_TRUE(run("BEGIN", false).ok);
  ASSERT_TRUE(run("INSERT(2,u2)", true).ok);
  ASSERT_TRUE(run("INSERT(3,u3)", false).ok);
  client.close();
  eng.shutdown();

  structdb::facade::Engine eng2;
  eng2.config().update(1, snap);
  ASSERT_TRUE(eng2.startup(&err)) << err;
  structdb::client::EmbedClient client2(eng2);
  ASSERT_TRUE(client2.open(root / "repl_v2", &err)) << err;
  structdb::client::mdb::MdbInteractiveSession session2;
  std::vector<std::string> log2;
  ASSERT_TRUE(structdb::client::mdb::mdb_repl_execute_line(eng2, client2, session2, "COUNT", &log2, false, false, nullptr).ok);
  bool found_three = false;
  for (const auto& ln : log2) {
    if (ln.find("rows=3") != std::string::npos) found_three = true;
  }
  EXPECT_TRUE(found_three) << "expected 3 rows after v2 replay";
  client2.close();
  eng2.shutdown();
}

TEST(Mdb, ReplSessionTxnV2CorruptLineDropsLog) {
  const auto root = temp_dir("mdb_repl_txn_v2_bad");
  fs::remove_all(root);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "repl_bad", &err)) << err;
  structdb::client::mdb::MdbInteractiveSession session;
  std::vector<std::string> log;
  ASSERT_TRUE(
      structdb::client::mdb::mdb_repl_execute_line(eng, client, session, "CREATE TABLE(tb)", &log, false, false, nullptr).ok);
  ASSERT_TRUE(structdb::client::mdb::mdb_repl_execute_line(eng, client, session, "USE(tb)", &log, false, false, nullptr).ok);
  ASSERT_TRUE(
      structdb::client::mdb::mdb_repl_execute_line(eng, client, session, "DEFATTR(x:string)", &log, false, false, nullptr).ok);
  ASSERT_TRUE(structdb::client::mdb::mdb_repl_execute_line(eng, client, session, "BEGIN", &log, false, false, nullptr).ok);
  ASSERT_TRUE(
      structdb::client::mdb::mdb_repl_execute_line(eng, client, session, "INSERT(1,a)", &log, false, false, nullptr).ok);
  const fs::path txn_path = root / "repl_bad" / structdb::client::kEmbedSessionArtifactsDir / "session.txn";
  ASSERT_TRUE(fs::exists(txn_path));
  {
    std::ofstream app(txn_path, std::ios::binary | std::ios::app);
    ASSERT_TRUE(app.is_open());
    app << "V2OP\tINSERT\tnot_valid_hex\n";
    app.flush();
  }
  client.close();
  eng.shutdown();

  structdb::facade::Engine eng2;
  eng2.config().update(1, snap);
  ASSERT_TRUE(eng2.startup(&err)) << err;
  structdb::client::EmbedClient client2(eng2);
  ASSERT_TRUE(client2.open(root / "repl_bad", &err)) << err;
  structdb::client::mdb::MdbInteractiveSession session2;
  std::vector<std::string> log2;
  ASSERT_TRUE(
      structdb::client::mdb::mdb_repl_execute_line(eng2, client2, session2, "SHOW TXN", &log2, false, false, nullptr).ok);
  EXPECT_FALSE(fs::exists(txn_path));
  bool inactive = false;
  for (const auto& ln : log2) {
    if (ln.find("active=no") != std::string::npos) inactive = true;
  }
  EXPECT_TRUE(inactive);
  client2.close();
  eng2.shutdown();
}

TEST(Mdb, Phase25ExitReplFlag) {
  TxnChainStrictRepl T("phase25_exit");
  ASSERT_TRUE(T.start());
  const auto r = T.repl("EXIT");
  ASSERT_TRUE(r.ok);
  EXPECT_TRUE(r.repl_exit_requested);
}

TEST(Mdb, Phase25DropTableAndNotPortable) {
  const auto root = temp_dir("phase25_drop");
  fs::remove_all(root);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "sess", &err)) << err;
  structdb::client::mdb::MdbInteractiveSession session;
  std::vector<std::string> log;
  auto run = [&](const char* line) {
    return structdb::client::mdb::mdb_repl_execute_line(eng, client, session, line, &log, false, false, &err);
  };
  ASSERT_TRUE(run("CREATE TABLE(dt)").ok);
  ASSERT_TRUE(run("USE(dt)").ok);
  ASSERT_TRUE(run("DEFATTR(x:int)").ok);
  ASSERT_TRUE(run("INSERT(1,42)").ok);
  log.clear();
  ASSERT_TRUE(run("DROP TABLE(dt)").ok);
  bool saw_drop = false;
  for (const auto& s : log) {
    if (s.find("[DROP]") != std::string::npos) saw_drop = true;
  }
  EXPECT_TRUE(saw_drop);
  std::string v;
  EXPECT_FALSE(eng.kv_get(structdb::storage::mdb_keyspace::catalog_key("dt"), &v));

  log.clear();
  ASSERT_TRUE(run("AUTOVACUUM 1").ok);
  bool saw_ns = false;
  for (const auto& s : log) {
    if (s.find("[NOT_SUPPORTED]") != std::string::npos) saw_ns = true;
  }
  EXPECT_TRUE(saw_ns);

  client.close();
  eng.shutdown();
}

TEST(Mdb, Phase25ReleaseSavepoint) {
  TxnChainStrictRepl T("phase25_relsp");
  ASSERT_TRUE(T.start());
  ASSERT_TRUE(T.strict_new_table_from_create("rs", "DEFATTR(n:int)"));
  T.log.clear();
  ASSERT_TRUE(T.repl("BEGIN").ok);
  ASSERT_TRUE(T.repl("SAVEPOINT a").ok);
  ASSERT_TRUE(T.repl("INSERT(1,10)").ok);
  ASSERT_TRUE(T.repl("RELEASE SAVEPOINT a").ok);
  ASSERT_TRUE(T.repl("COMMIT").ok);
  T.log.clear();
  ASSERT_TRUE(T.repl("COUNT").ok);
  bool okc = false;
  for (const auto& s : T.log) {
    if (s.find("rows=1") != std::string::npos) okc = true;
  }
  EXPECT_TRUE(okc);
}

TEST(Mdb, Phase25ShowTuningJson) {
  const auto root = temp_dir("phase25_tune");
  fs::remove_all(root);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "s", &err)) << err;
  structdb::client::mdb::MdbInteractiveSession session;
  std::vector<std::string> log;
  ASSERT_TRUE(structdb::client::mdb::mdb_repl_execute_line(eng, client, session, "SHOW TUNING JSON", &log, false, false,
                                                           nullptr)
                    .ok);
  bool hit = false;
  for (const auto& s : log) {
    if (s.find("\"kind\":\"structdb_tuning\"") != std::string::npos) hit = true;
  }
  EXPECT_TRUE(hit);
  const std::string j = join_logs(log);
  EXPECT_NE(j.find("\"compaction_merge_success_total\":"), std::string::npos) << j;
  EXPECT_NE(j.find("\"compaction_io_pool_threads\":"), std::string::npos) << j;
  EXPECT_NE(j.find("\"compaction_parallel_sst_reads\":"), std::string::npos) << j;
  EXPECT_NE(j.find("\"wal_fsync_min_interval_ms\":"), std::string::npos) << j;
  EXPECT_NE(j.find("\"wal_append_max_bytes_per_second\":"), std::string::npos) << j;
  EXPECT_NE(j.find("\"wal_append_frame_bytes_committed_total\":"), std::string::npos) << j;
  EXPECT_NE(j.find("\"mdb_incremental_persist\":"), std::string::npos) << j;
  client.close();
  eng.shutdown();
}

TEST(Mdb, Phase25StrictParseMatrix) {
  using structdb::client::mdb::MdbParsedLine;
  using structdb::client::mdb::MdbVerb;
  using structdb::client::mdb::mdb_parse_command_line;
  MdbParsedLine pl{};
  std::string pe;
  ASSERT_TRUE(mdb_parse_command_line("DROP TABLE(x)", &pl, &pe));
  EXPECT_EQ(pl.verb, MdbVerb::DropTable);
  EXPECT_EQ(std::string(pl.paren_inner), "x");

  ASSERT_FALSE(mdb_parse_command_line("DROP TABLE", &pl, &pe));
  ASSERT_FALSE(mdb_parse_command_line("RELEASE SAVEPOINT ", &pl, &pe));
  EXPECT_NE(pe.find("need"), std::string::npos);

  ASSERT_TRUE(mdb_parse_command_line("CREATE SCHEMA foo", &pl, &pe));
  EXPECT_EQ(pl.verb, MdbVerb::NotPortable);

  ASSERT_TRUE(mdb_parse_command_line("recover to lsn 1", &pl, &pe));
  EXPECT_EQ(pl.verb, MdbVerb::NotPortable);

  ASSERT_TRUE(mdb_parse_command_line("QUIT", &pl, &pe));
  EXPECT_EQ(pl.verb, MdbVerb::Exit);

  ASSERT_TRUE(mdb_parse_command_line("SHOW STATUS JSON", &pl, &pe));
  EXPECT_EQ(pl.verb, MdbVerb::ShowTuningJson);

  ASSERT_TRUE(mdb_parse_command_line("DESCRIBE", &pl, &pe));
  EXPECT_EQ(pl.verb, MdbVerb::ShowAttr);

  ASSERT_TRUE(mdb_parse_command_line("IMPORTDIR(C:/no_such_dir_xyz)", &pl, &pe));
  EXPECT_EQ(pl.verb, MdbVerb::ImportDir);
  EXPECT_EQ(std::string(pl.paren_inner), "C:/no_such_dir_xyz");

  ASSERT_TRUE(mdb_parse_command_line(
      "CONFIRM_REORDER({\"table\":\"t\",\"pairs\":[[\"1\",\"2\"],[\"3\",\"4\"]]})", &pl, &pe));
  EXPECT_EQ(pl.verb, MdbVerb::ConfirmReorder);
  EXPECT_EQ(std::string(pl.paren_inner), "{\"table\":\"t\",\"pairs\":[[\"1\",\"2\"],[\"3\",\"4\"]]}");
}

TEST(Mdb, Phase25StrictTxnBlocksDdlVacuumImport) {
  TxnChainStrictRepl T("phase25_txnblk");
  ASSERT_TRUE(T.start());
  ASSERT_TRUE(T.strict_new_table_from_create("blk", "DEFATTR(id:int)"));
  ASSERT_TRUE(T.repl("BEGIN").ok);
  const char* blocked[] = {"DROP TABLE(blk)",
                           "RENAME TABLE(x)",
                           "RESET",
                           "VACUUM",
                           "SET PRIMARY KEY(id)",
                           "IMPORTDIR(nope)",
                           "CONFIRM_REORDER({\"table\":\"blk\",\"pairs\":[[\"1\",\"2\"]]})"};
  for (const char* line : blocked) {
    const auto r = T.repl(line);
    EXPECT_FALSE(r.ok) << line;
    EXPECT_FALSE(r.last_error.empty()) << line;
  }
  T.close();
}

TEST(Mdb, Phase25StrictRenameKvAndCollision) {
  const auto root = temp_dir("phase25_rn");
  fs::remove_all(root);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "s", &err)) << err;
  structdb::client::mdb::MdbInteractiveSession session;
  std::vector<std::string> log;
  auto run = [&](const char* line) {
    return structdb::client::mdb::mdb_repl_execute_line(eng, client, session, line, &log, false, false, &err);
  };
  ASSERT_TRUE(run("CREATE TABLE(r1)").ok);
  ASSERT_TRUE(run("USE(r1)").ok);
  ASSERT_TRUE(run("DEFATTR(id:int)").ok);
  ASSERT_TRUE(run("INSERT(1,1)").ok);
  std::string v;
  ASSERT_TRUE(eng.kv_get(structdb::storage::mdb_keyspace::catalog_key("r1"), &v));
  ASSERT_TRUE(run("RENAME TABLE(r2)").ok);
  EXPECT_FALSE(eng.kv_get(structdb::storage::mdb_keyspace::catalog_key("r1"), &v));
  ASSERT_TRUE(eng.kv_get(structdb::storage::mdb_keyspace::catalog_key("r2"), &v));
  log.clear();
  ASSERT_TRUE(run("COUNT").ok);
  EXPECT_NE(join_logs(log).find("rows=1"), std::string::npos);

  ASSERT_TRUE(run("CREATE TABLE(r3)").ok);
  ASSERT_TRUE(run("USE(r3)").ok);
  ASSERT_TRUE(run("DEFATTR(id:int)").ok);
  ASSERT_TRUE(run("USE(r2)").ok);
  const auto rcol = run("RENAME TABLE(r3)");
  EXPECT_FALSE(rcol.ok);
  EXPECT_NE(rcol.last_error.find("already exists"), std::string::npos);

  client.close();
  eng.shutdown();
}

TEST(Mdb, Phase25StrictResetDescribeRebuild) {
  TxnChainStrictRepl T("phase25_rst");
  ASSERT_TRUE(T.start());
  ASSERT_TRUE(T.strict_new_table_from_create("rst", "DEFATTR(n:int)"));
  ASSERT_TRUE(T.repl("INSERT(1,7)").ok);
  ASSERT_TRUE(T.repl("RESET").ok);
  const auto bad = T.repl("DESCRIBE");
  EXPECT_FALSE(bad.ok);
  EXPECT_NE(bad.last_error.find("schema"), std::string::npos);
  ASSERT_TRUE(T.repl("DEFATTR(k:int)").ok);
  T.log.clear();
  ASSERT_TRUE(T.repl("COUNT").ok);
  EXPECT_NE(join_logs(T.log).find("rows=0"), std::string::npos);
  T.close();
}

TEST(Mdb, Phase25StrictQbalExplainAndErrors) {
  TxnChainStrictRepl T("phase25_qb");
  ASSERT_TRUE(T.start());
  ASSERT_TRUE(T.strict_new_table_from_create("qb", "DEFATTR(id:int,amt:int,seq:int)"));
  ASSERT_TRUE(T.repl("INSERT(1,1,10,1)").ok);
  ASSERT_TRUE(T.repl("INSERT(2,2,20,2)").ok);
  ASSERT_TRUE(T.repl("INSERT(3,3,30,3)").ok);
  T.log.clear();
  ASSERT_TRUE(T.repl("QBAL(amt,20)").ok);
  const std::string j = join_logs(T.log);
  EXPECT_NE(j.find("matched=2"), std::string::npos);
  EXPECT_NE(j.find("sum=50"), std::string::npos);

  EXPECT_FALSE(T.repl("QBAL(amt)").ok);
  EXPECT_FALSE(T.repl("QBAL(amt,x)").ok);
  EXPECT_FALSE(T.repl("QBAL(tag,0)").ok);
  EXPECT_FALSE(T.repl("QBAL(nope,0)").ok);

  T.log.clear();
  ASSERT_TRUE(T.repl("EXPLAIN WHERE(amt,=,20)").ok);
  EXPECT_NE(join_logs(T.log).find("matched=1"), std::string::npos);
  EXPECT_FALSE(T.repl("EXPLAIN WHERE(amt,=)").ok);
  T.close();
}

TEST(Mdb, Phase25StrictReleaseSavepointErrors) {
  TxnChainStrictRepl T("phase25_rsp_err");
  ASSERT_TRUE(T.start());
  ASSERT_TRUE(T.strict_new_table_from_create("rsp", "DEFATTR(n:int)"));
  EXPECT_FALSE(T.repl("RELEASE SAVEPOINT a").ok);
  ASSERT_TRUE(T.repl("BEGIN").ok);
  ASSERT_TRUE(T.repl("SAVEPOINT a").ok);
  EXPECT_FALSE(T.repl("RELEASE SAVEPOINT b").ok);
  ASSERT_TRUE(T.repl("RELEASE SAVEPOINT a").ok);
  ASSERT_TRUE(T.repl("ROLLBACK").ok);
  T.close();
}

TEST(Mdb, Phase25StrictDropVacuumImportPathErrors) {
  const auto root = phase25_cmd_test_root("dve");
  fs::remove_all(root);
  fs::create_directories(root);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "s", &err)) << err;
  structdb::client::mdb::MdbInteractiveSession session;
  std::vector<std::string> log;
  auto run = [&](const char* line) {
    return structdb::client::mdb::mdb_repl_execute_line(eng, client, session, line, &log, false, false, &err);
  };
  const auto rnf = run("DROP TABLE(no_such_table)");
  EXPECT_FALSE(rnf.ok);
  EXPECT_NE(rnf.last_error.find("not found"), std::string::npos);

  const std::string bad_imp = "IMPORTDIR(" + (root / "not_a_dir").string() + ")";
  const auto impf = run(bad_imp.c_str());
  EXPECT_FALSE(impf.ok);
  EXPECT_NE(impf.last_error.find("IMPORTDIR"), std::string::npos);

  log.clear();
  ASSERT_TRUE(run("VACUUM").ok);
  EXPECT_NE(join_logs(log).find("[VACUUM]"), std::string::npos);

  client.close();
  eng.shutdown();
}

TEST(Mdb, Phase25StrictImportExportBulkFastQuit) {
  const auto root = phase25_cmd_test_root("impexp2");
  fs::remove_all(root);
  const auto imp = root / "mdb_import_here";
  fs::create_directories(imp);
  write_script(imp / "a_script.mdb",
               "CREATE TABLE(impt)\n"
               "USE(impt)\n"
               "DEFATTR(id:int,v:int)\n"
               "INSERT(1,1,99)\n");
  const fs::path out_json = root / "exp_out.json";

  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "sess", &err)) << err;
  structdb::client::mdb::MdbInteractiveSession session;
  std::vector<std::string> log;
  auto run = [&](const char* line) {
    return structdb::client::mdb::mdb_repl_execute_line(eng, client, session, line, &log, false, false, &err);
  };
  const std::string imp_arg = std::string("IMPORTDIR(") + imp.generic_string() + ")";
  ASSERT_TRUE(run(imp_arg.c_str()).ok) << err << " line=" << imp_arg;
  ASSERT_TRUE(run("USE(impt)").ok);
  ASSERT_TRUE(run("BULKINSERTFAST(2,2,1|3,3,2)").ok) << err;

  const std::string exp_line = std::string("EXPORT ") + out_json.string();
  ASSERT_TRUE(run(exp_line.c_str()).ok) << err;
  std::ifstream in(out_json);
  ASSERT_TRUE(in);
  const std::string body((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
  EXPECT_NE(body.find("\"id\": \"1\""), std::string::npos);
  EXPECT_NE(body.find("\"id\": \"3\""), std::string::npos);

  log.clear();
  ASSERT_TRUE(run("SHOW STORAGE JSON").ok);
  bool st_hit = false;
  for (const auto& s : log) {
    if (s.find("\"kind\":\"structdb_storage\"") != std::string::npos) st_hit = true;
  }
  EXPECT_TRUE(st_hit);

  const auto rq = run("QUIT");
  ASSERT_TRUE(rq.ok);
  EXPECT_TRUE(rq.repl_exit_requested);

  client.close();
  eng.shutdown();
}

TEST(Mdb, Phase25StrictShowAttrWithoutSchema) {
  const auto root = temp_dir("phase25_sh");
  fs::remove_all(root);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "s", &err)) << err;
  structdb::client::mdb::MdbInteractiveSession session;
  std::vector<std::string> log;
  const auto r = structdb::client::mdb::mdb_repl_execute_line(eng, client, session, "SHOW ATTR", &log, false, false,
                                                             &err);
  EXPECT_FALSE(r.ok);
  client.close();
  eng.shutdown();
}

TEST(Mdb, Phase25StrictSetPrimaryKeyDuplicateRejected) {
  TxnChainStrictRepl T("phase25_pkdup");
  ASSERT_TRUE(T.start());
  ASSERT_TRUE(T.strict_new_table_from_create("pkd", "DEFATTR(id:int,name:string)"));
  ASSERT_TRUE(T.repl("INSERT(1,1,a)").ok);
  ASSERT_TRUE(T.repl("INSERT(2,2,a)").ok);
  const auto r = T.repl("SET PRIMARY KEY(name)");
  EXPECT_FALSE(r.ok);
  EXPECT_NE(r.last_error.find("duplicate"), std::string::npos);
  T.close();
}

TEST(Mdb, Phase25StrictStringEqExplainIndexHint) {
  TxnChainStrictRepl T("phase25_idxhint");
  ASSERT_TRUE(T.start());
  ASSERT_TRUE(T.strict_new_table_from_create("ix", "DEFATTR(id:int,tag:string)"));
  ASSERT_TRUE(T.repl("INSERT(1,1,x)").ok);
  T.log.clear();
  ASSERT_TRUE(T.repl("SHOW PLAN(tag,=,x)").ok);
  EXPECT_NE(join_logs(T.log).find("string_eq_sidecar_maybe"), std::string::npos);
  T.close();
}

TEST(Mdb, ScanMoreCursorPaging) {
  TxnChainStrictRepl T("mdb_scan_more");
  ASSERT_TRUE(T.start());
  ASSERT_TRUE(T.strict_new_table_from_create("sc2", "DEFATTR(id:int,a:string)"));
  for (int i = 1; i <= 5; ++i) {
    ASSERT_TRUE(T.repl("INSERT(" + std::to_string(i) + "," + std::to_string(i) + ",r" + std::to_string(i) + ")").ok);
  }
  T.log.clear();
  ASSERT_TRUE(T.repl("SCAN MORE(2)").ok);
  const std::string p1 = join_logs(T.log);
  EXPECT_NE(p1.find("rows_shown=2"), std::string::npos);
  EXPECT_NE(p1.find("cursor=2"), std::string::npos);
  T.log.clear();
  ASSERT_TRUE(T.repl("SCAN MORE(2)").ok);
  const std::string p2 = join_logs(T.log);
  EXPECT_NE(p2.find("rows_shown=2"), std::string::npos);
  EXPECT_NE(p2.find("cursor=4"), std::string::npos);
  T.log.clear();
  ASSERT_TRUE(T.repl("SCAN RESET").ok);
  EXPECT_NE(join_logs(T.log).find("cursor reset"), std::string::npos);
  T.close();
}

TEST(Mdb, Phase25StrictScanInsideBegin) {
  TxnChainStrictRepl T("phase25_scan_txn");
  ASSERT_TRUE(T.start());
  ASSERT_TRUE(T.strict_new_table_from_create("sc", "DEFATTR(id:int,msg:string)"));
  ASSERT_TRUE(T.repl("INSERT(1,1,base)").ok);
  ASSERT_TRUE(T.repl("BEGIN").ok);
  ASSERT_TRUE(T.repl("INSERT(2,2,in_txn)").ok);
  T.log.clear();
  ASSERT_TRUE(T.repl("SCAN").ok);
  const std::string all = join_logs(T.log);
  EXPECT_NE(all.find("id=1"), std::string::npos);
  EXPECT_NE(all.find("id=2"), std::string::npos);
  EXPECT_NE(all.find("rows_shown=2"), std::string::npos);
  T.close();
}

TEST(Mdb, Phase25StrictShowlogNonEmptyTail) {
  const auto root = temp_dir("phase25_showlog");
  fs::remove_all(root);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "sess", &err)) << err;
  structdb::client::mdb::MdbInteractiveSession session;
  std::vector<std::string> log;
  auto run = [&](const char* line) {
    return structdb::client::mdb::mdb_repl_execute_line(eng, client, session, line, &log, false, false, &err);
  };
  ASSERT_TRUE(run("CREATE TABLE(lg)").ok);
  ASSERT_TRUE(run("USE(lg)").ok);
  ASSERT_TRUE(run("DEFATTR(x:int)").ok);
  ASSERT_TRUE(run("INSERT(1,1)").ok);
  log.clear();
  ASSERT_TRUE(run("SHOWLOG").ok);
  std::size_t detail_lines = 0;
  for (const auto& ln : log) {
    if (ln.size() >= 2 && ln.compare(0, 2, "  ") == 0 && ln.size() > 2) ++detail_lines;
  }
  EXPECT_GE(detail_lines, 1u) << join_logs(log);
  client.close();
  eng.shutdown();
}

TEST(Mdb, Phase25StrictVacuumNoStorageAfterShutdown) {
  const auto root = temp_dir("phase25_vac_nost");
  fs::remove_all(root);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "s", &err)) << err;
  structdb::client::mdb::MdbInteractiveSession session;
  ASSERT_TRUE(structdb::client::mdb::mdb_repl_execute_line(eng, client, session, "HELP", nullptr, false, false, &err)
                  .ok);
  client.close();
  eng.shutdown();
  std::vector<std::string> log;
  const auto r = structdb::client::mdb::mdb_repl_execute_line(eng, client, session, "VACUUM", &log, false, false, &err);
  EXPECT_FALSE(r.ok);
  EXPECT_NE(r.last_error.find("no storage"), std::string::npos);
}

TEST(Mdb, StrictDefattrRejectsUnknownType) {
  TxnChainStrictRepl T("mdb_bad_type");
  ASSERT_TRUE(T.start());
  ASSERT_TRUE(T.repl("CREATE TABLE(bt)").ok);
  ASSERT_TRUE(T.repl("USE(bt)").ok);
  const auto r = T.repl("DEFATTR(a:widget)");
  EXPECT_FALSE(r.ok);
  EXPECT_NE(r.last_error.find("unknown type"), std::string::npos);
  T.close();
}

TEST(Mdb, StrictTypedColumnsEmptyCellAndMismatchAbortScript) {
  const auto root = temp_dir("mdb_type_abort");
  fs::remove_all(root);
  fs::create_directories(root);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "sess", &err)) << err;
  const fs::path script = root / "typed.mdb";
  write_script(script,
               "CREATE TABLE(ty)\n"
               "USE(ty)\n"
               "DEFATTR(i:int,s:string,c:char,f:float,d:double,dt:datetime,ts:timestamp,v:text)\n"
               "INSERT(1,1,,A,1.5,2.25,2020-01-02,1609459200,z)\n"
               "INSERT(2,2,bad,XY,1,1,2020-01-02,0,w)\n"
               "COUNT\n");
  structdb::client::mdb::MdbRunOptions opt;
  opt.script_path = script;
  const auto r = structdb::client::mdb::run_mdb_script(eng, client, opt);
  EXPECT_FALSE(r.ok);
  client.close();
  eng.shutdown();
}

TEST(Mdb, StrictTypedColumnsHappyPathScript) {
  const auto root = temp_dir("mdb_type_ok");
  fs::remove_all(root);
  fs::create_directories(root);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "sess", &err)) << err;
  const fs::path script = root / "typed_ok.mdb";
  write_script(script,
               "CREATE TABLE(ty)\n"
               "USE(ty)\n"
               "DEFATTR(i:int,s:string,c:char,f:float,d:double,dt:datetime,ts:timestamp,v:varchar)\n"
               "INSERT(1,1,,A,1.5,2.25,2020-01-02,1609459200,z)\n"
               "INSERT(2,2,xy,B,0.5,0.25,2020-01-03,1609545600,w2)\n"
               "COUNT\n");
  std::vector<std::string> log;
  structdb::client::mdb::MdbRunOptions opt;
  opt.script_path = script;
  opt.log_sink = &log;
  const auto r = structdb::client::mdb::run_mdb_script(eng, client, opt);
  ASSERT_TRUE(r.ok) << r.last_error;
  EXPECT_NE(join_logs(log).find("rows=2"), std::string::npos);
  client.close();
  eng.shutdown();
}

TEST(Mdb, IntegrateTxnRecoverRollbackRestartChain) {
  const auto root = temp_dir("mdb_txn_chain_rr");
  fs::remove_all(root);
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();

  {
    structdb::facade::Engine eng;
    eng.config().update(1, snap);
    std::string err;
    ASSERT_TRUE(eng.startup(&err)) << err;
    structdb::client::EmbedClient client(eng);
    ASSERT_TRUE(client.open(root / "repl_chain", &err)) << err;
    structdb::client::mdb::MdbInteractiveSession session;
    std::vector<std::string> log;
    auto run = [&](const char* line) {
      return structdb::client::mdb::mdb_repl_execute_line(eng, client, session, line, &log, false, false, &err);
    };
    ASSERT_TRUE(run("CREATE TABLE(tc)").ok);
    ASSERT_TRUE(run("USE(tc)").ok);
    ASSERT_TRUE(run("DEFATTR(n:int,t:string)").ok);
    ASSERT_TRUE(run("INSERT(1,1,a)").ok);
    ASSERT_TRUE(run("BEGIN").ok);
    ASSERT_TRUE(run("INSERT(2,2,b)").ok);
    ASSERT_TRUE(run("SAVEPOINT s1").ok);
    ASSERT_TRUE(run("INSERT(3,3,c)").ok);
    ASSERT_TRUE(run("ROLLBACK TO SAVEPOINT s1").ok);
    log.clear();
    ASSERT_TRUE(run("COUNT").ok);
    EXPECT_NE(join_logs(log).find("rows=2"), std::string::npos);
    ASSERT_TRUE(run("INSERT(4,4,d)").ok);
    client.close();
    eng.shutdown();
  }

  std::string err;
  structdb::facade::Engine eng2;
  eng2.config().update(1, snap);
  ASSERT_TRUE(eng2.startup(&err)) << err;
  structdb::client::EmbedClient client2(eng2);
  ASSERT_TRUE(client2.open(root / "repl_chain", &err)) << err;
  structdb::client::mdb::MdbInteractiveSession session2;
  std::vector<std::string> log2;
  ASSERT_TRUE(structdb::client::mdb::mdb_repl_execute_line(eng2, client2, session2, "COUNT", &log2, false, false, &err)
                  .ok);
  bool saw_recover = false;
  bool saw_three = false;
  for (const auto& ln : log2) {
    if (ln.find("[RECOVER]") != std::string::npos) saw_recover = true;
    if (ln.find("rows=3") != std::string::npos) saw_three = true;
  }
  EXPECT_TRUE(saw_recover);
  EXPECT_TRUE(saw_three);
  log2.clear();
  ASSERT_TRUE(
      structdb::client::mdb::mdb_repl_execute_line(eng2, client2, session2, "ROLLBACK", &log2, false, false, &err).ok);
  ASSERT_TRUE(structdb::client::mdb::mdb_repl_execute_line(eng2, client2, session2, "COUNT", &log2, false, false, &err)
                  .ok);
  // Baseline in session.txn after ROLLBACK TO SAVEPOINT was rewritten with two in-memory rows (1+2); full ROLLBACK
  // restores that baseline, not storage-only committed rows.
  EXPECT_NE(join_logs(log2).find("rows=2"), std::string::npos);
  client2.close();
  eng2.shutdown();
}

TEST(Mdb, CalendarInvalidDatetimeInsertRejected) {
  const auto root = temp_dir("mdb_cal_bad");
  fs::remove_all(root);
  fs::create_directories(root);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "sess", &err)) << err;
  const fs::path script = root / "cal_bad.mdb";
  write_script(script,
               "CREATE TABLE(cd)\n"
               "USE(cd)\n"
               "DEFATTR(dt:datetime)\n"
               "INSERT(1,2019-02-29)\n"
               "COUNT\n");
  structdb::client::mdb::MdbRunOptions opt;
  opt.script_path = script;
  const auto r = structdb::client::mdb::run_mdb_script(eng, client, opt);
  EXPECT_FALSE(r.ok);
  client.close();
  eng.shutdown();
}

TEST(Mdb, FloatAggregatesMinMaxScript) {
  const auto root = temp_dir("mdb_fp_agg");
  fs::remove_all(root);
  fs::create_directories(root);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "sess", &err)) << err;
  const fs::path script = root / "fp_agg.mdb";
  write_script(script,
               "CREATE TABLE(fa)\n"
               "USE(fa)\n"
               "DEFATTR(x:float)\n"
               "INSERT(1,1.25)\n"
               "INSERT(2,4.5)\n"
               "SUM(x)\n"
               "AVG(x)\n"
               "MIN(x)\n"
               "MAX(x)\n");
  std::vector<std::string> log;
  structdb::client::mdb::MdbRunOptions opt;
  opt.script_path = script;
  opt.log_sink = &log;
  const auto r = structdb::client::mdb::run_mdb_script(eng, client, opt);
  ASSERT_TRUE(r.ok) << r.last_error;
  const std::string j = join_logs(log);
  EXPECT_NE(j.find("[SUM] x=5.75"), std::string::npos);
  EXPECT_NE(j.find("[AVG] x=2.875"), std::string::npos);
  EXPECT_NE(j.find("[MIN] x=1.25"), std::string::npos);
  EXPECT_NE(j.find("[MAX] x=4.5"), std::string::npos);
  client.close();
  eng.shutdown();
}

TEST(Mdb, WhereDatetimeAndExplainLikeHints) {
  TxnChainStrictRepl T("mdb_where_dt_like");
  ASSERT_TRUE(T.start());
  ASSERT_TRUE(T.strict_new_table_from_create("wd", "DEFATTR(s:string,dt:datetime)"));
  ASSERT_TRUE(T.repl("INSERT(1,alpha,2020-05-01)").ok);
  ASSERT_TRUE(T.repl("INSERT(2,beta,2020-05-02)").ok);
  T.log.clear();
  ASSERT_TRUE(T.repl("WHERE(dt,=,2020-05-01)").ok);
  EXPECT_NE(join_logs(T.log).find("matched 1"), std::string::npos);
  T.log.clear();
  ASSERT_TRUE(T.repl("EXPLAIN WHERE(s, LIKE, al%)").ok);
  EXPECT_NE(join_logs(T.log).find("like_full_scan"), std::string::npos);
  T.log.clear();
  ASSERT_TRUE(T.repl("SHOW PLAN(s,  =  ,alpha)").ok);
  EXPECT_NE(join_logs(T.log).find("string_eq_sidecar_maybe"), std::string::npos);
  T.close();
}

TEST(Mdb, TxnSavepointNestedUpdateDeleteInsertRollback) {
  TxnChainStrictRepl T("mdb_txn_nested_ud");
  ASSERT_TRUE(T.start());
  ASSERT_TRUE(T.strict_new_table_from_create("ud", "DEFATTR(s:string,f:float,d:datetime)"));
  ASSERT_TRUE(T.repl("INSERT(1,a,1.5,2020-06-15)").ok);
  ASSERT_TRUE(T.repl("INSERT(2,b,2.5,2020-06-20)").ok);
  ASSERT_TRUE(T.repl("BEGIN").ok);
  ASSERT_TRUE(T.repl("INSERT(3,c,3.5,2020-06-25)").ok);
  ASSERT_TRUE(T.repl("SAVEPOINT s1").ok);
  ASSERT_TRUE(T.repl("DELETEWHERE(s, =, a)").ok);
  ASSERT_TRUE(T.repl("UPDATEWHERE(f, 99.0, WHERE, id, =, 2)").ok);
  T.log.clear();
  ASSERT_TRUE(T.repl("COUNT").ok);
  EXPECT_NE(join_logs(T.log).find("rows=2"), std::string::npos);
  ASSERT_TRUE(T.repl("ROLLBACK TO SAVEPOINT s1").ok);
  T.log.clear();
  ASSERT_TRUE(T.repl("COUNT").ok);
  EXPECT_NE(join_logs(T.log).find("rows=3"), std::string::npos);
  T.log.clear();
  ASSERT_TRUE(T.repl("WHERE(f, =, 2.5)").ok);
  EXPECT_NE(join_logs(T.log).find("matched 1"), std::string::npos);
  ASSERT_TRUE(T.repl("DELETEWHERE(s, LIKE, %b%)").ok);
  T.log.clear();
  ASSERT_TRUE(T.repl("COUNT").ok);
  EXPECT_NE(join_logs(T.log).find("rows=2"), std::string::npos);
  ASSERT_TRUE(T.repl("ROLLBACK").ok);
  T.log.clear();
  ASSERT_TRUE(T.repl("COUNT").ok);
  EXPECT_NE(join_logs(T.log).find("rows=2"), std::string::npos);
  T.log.clear();
  ASSERT_TRUE(T.repl("FIND(3)").ok);
  EXPECT_NE(join_logs(T.log).find("not found"), std::string::npos);
  T.log.clear();
  ASSERT_TRUE(T.repl("FIND(1)").ok);
  EXPECT_EQ(join_logs(T.log).find("not found"), std::string::npos);
  T.close();
}

TEST(Mdb, Phase38ReorderHappyPath) {
  TxnChainStrictRepl T("phase38_reord");
  ASSERT_TRUE(T.start());
  ASSERT_TRUE(T.strict_new_table_from_create("r38", "DEFATTR(id:int)"));
  ASSERT_TRUE(T.repl("INSERT(1,10)").ok);
  ASSERT_TRUE(T.repl("INSERT(2,20)").ok);
  T.log.clear();
  ASSERT_TRUE(T.repl("CONFIRM_REORDER({\"table\":\"r38\",\"pairs\":[[\"1\",\"10\"],[\"2\",\"20\"]]})").ok);
  const std::string all = join_logs(T.log);
  EXPECT_NE(all.find("[REORDER_MAP_JSON]"), std::string::npos);
  T.log.clear();
  ASSERT_TRUE(T.repl("FIND(10)").ok);
  EXPECT_EQ(join_logs(T.log).find("not found"), std::string::npos);
  T.log.clear();
  ASSERT_TRUE(T.repl("FIND(1)").ok);
  EXPECT_NE(join_logs(T.log).find("not found"), std::string::npos);
  T.close();
}

TEST(Mdb, Phase38ReorderTwoTablesScript) {
  const auto root = temp_dir("phase38_two");
  fs::remove_all(root);
  fs::create_directories(root);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "session", &err)) << err;

  const fs::path script = root / "phase38.mdb";
  write_script(script,
               "CREATE TABLE(ta)\n"
               "USE(ta)\n"
               "DEFATTR(id:int)\n"
               "INSERT(1,1)\n"
               "CONFIRM_REORDER({\"table\":\"ta\",\"pairs\":[[\"1\",\"9\"]]})\n"
               "CREATE TABLE(tb)\n"
               "USE(tb)\n"
               "DEFATTR(id:int)\n"
               "INSERT(1,2)\n"
               "CONFIRM_REORDER({\"table\":\"tb\",\"pairs\":[[\"1\",\"8\"]]})\n");

  std::vector<std::string> log;
  structdb::client::mdb::MdbRunOptions opt;
  opt.script_path = script;
  opt.log_sink = &log;
  ASSERT_TRUE(structdb::client::mdb::run_mdb_script(eng, client, opt).ok) << err;

  std::size_t n = 0;
  for (const auto& s : log) {
    if (s.find("[REORDER_MAP_JSON]") != std::string::npos) ++n;
  }
  EXPECT_EQ(n, 2u) << join_logs(log);

  client.close();
  eng.shutdown();
}

TEST(Mdb, Phase38ReorderSwap) {
  TxnChainStrictRepl T("phase38_swap");
  ASSERT_TRUE(T.start());
  ASSERT_TRUE(T.strict_new_table_from_create("sw", "DEFATTR(id:int)"));
  ASSERT_TRUE(T.repl("INSERT(1,11)").ok);
  ASSERT_TRUE(T.repl("INSERT(2,22)").ok);
  ASSERT_TRUE(T.repl("CONFIRM_REORDER({\"table\":\"sw\",\"pairs\":[[\"1\",\"2\"],[\"2\",\"1\"]]})").ok);
  T.log.clear();
  ASSERT_TRUE(T.repl("COUNT").ok);
  EXPECT_NE(join_logs(T.log).find("rows=2"), std::string::npos);
  T.close();
}

/// 多层 SAVEPOINT + 事务内大表 PAGE / PAGE_JSON（id 主键 asc，触发 partial_sort 路径）+ 未提交关闭后冷启动
/// 恢复 txn v2，再全量 ROLLBACK 回到会话基线。
TEST(Mdb, TxnStrictNestedSavepointsPageJsonRecoverRestart) {
  TxnChainStrictRepl T("mdb_nested_pg_rec");
  ASSERT_TRUE(T.start());
  ASSERT_TRUE(T.strict_new_table_from_create("npg", "DEFATTR(s:string)"));
  ASSERT_TRUE(T.repl("INSERT(1,r1)").ok);
  ASSERT_TRUE(T.repl("BEGIN").ok);
  ASSERT_TRUE(T.repl("INSERT(2,r2)").ok);
  ASSERT_TRUE(T.repl("SAVEPOINT s1").ok);
  ASSERT_TRUE(T.repl("INSERT(3,r3)").ok);
  ASSERT_TRUE(T.repl("SAVEPOINT s2").ok);
  ASSERT_TRUE(T.repl("INSERT(4,r4)").ok);
  ASSERT_TRUE(T.repl("ROLLBACK TO SAVEPOINT s2").ok);
  T.log.clear();
  ASSERT_TRUE(T.repl("COUNT").ok);
  EXPECT_NE(join_logs(T.log).find("rows=3"), std::string::npos) << join_logs(T.log);
  ASSERT_TRUE(T.repl("ROLLBACK TO SAVEPOINT s1").ok);
  T.log.clear();
  ASSERT_TRUE(T.repl("COUNT").ok);
  EXPECT_NE(join_logs(T.log).find("rows=2"), std::string::npos) << join_logs(T.log);
  for (int id = 3; id <= 40; ++id) {
    const std::string line = "INSERT(" + std::to_string(id) + ",r" + std::to_string(id) + ")";
    ASSERT_TRUE(T.repl(line).ok) << T.err << " line=" << line;
  }
  T.log.clear();
  ASSERT_TRUE(T.repl("COUNT").ok);
  EXPECT_NE(join_logs(T.log).find("rows=40"), std::string::npos) << join_logs(T.log);

  T.log.clear();
  ASSERT_TRUE(T.repl("PAGE(1,10,id,asc)").ok) << T.err;
  {
    const std::string page_log = join_logs(T.log);
    EXPECT_NE(page_log.find("[PAGE] table=npg page=1 size=10"), std::string::npos) << page_log;
    for (int i = 1; i <= 10; ++i) {
      const std::string needle = std::string("  id=") + std::to_string(i);
      EXPECT_NE(page_log.find(needle), std::string::npos) << "missing " << needle << " in:\n"
                                                            << page_log;
    }
  }

  T.log.clear();
  ASSERT_TRUE(T.repl("PAGE_JSON(1,10,id,asc)").ok) << T.err;
  {
    const std::string pj = join_logs(T.log);
    EXPECT_NE(pj.find("[PAGE_JSON]{"), std::string::npos) << pj;
    EXPECT_NE(pj.find("\"headers\""), std::string::npos) << pj;
    EXPECT_NE(pj.find("\"name\":\"id\""), std::string::npos) << pj;
    // First data row: rank 1, then row pk "1", then first schema column "r1".
    EXPECT_NE(pj.find("[\"1\",\"1\",\"r1\""), std::string::npos) << pj;
  }

  T.client.close();
  T.eng.shutdown();

  std::string err2;
  structdb::facade::Engine eng2;
  eng2.config().update(1, T.snap);
  ASSERT_TRUE(eng2.startup(&err2)) << err2;
  structdb::client::EmbedClient client2(eng2);
  ASSERT_TRUE(client2.open(T.root / "session", &err2)) << err2;
  structdb::client::mdb::MdbInteractiveSession session2;
  std::vector<std::string> log2;
  auto run2 = [&](const char* line) {
    err2.clear();
    return structdb::client::mdb::mdb_repl_execute_line(eng2, client2, session2, line, &log2, false, false, &err2);
  };
  ASSERT_TRUE(run2("COUNT").ok) << err2;
  {
    const std::string all = join_logs(log2);
    bool saw_recover = false;
    for (const auto& ln : log2) {
      if (ln.find("[RECOVER]") != std::string::npos) saw_recover = true;
    }
    EXPECT_TRUE(saw_recover) << "expected txn recover log; got:\n" << all;
    EXPECT_NE(all.find("rows=40"), std::string::npos) << all;
  }
  log2.clear();
  ASSERT_TRUE(run2("ROLLBACK").ok) << err2;
  ASSERT_TRUE(run2("COUNT").ok) << err2;
  {
    const std::string all2 = join_logs(log2);
    // Same class of baseline as IntegrateTxnRecoverRollbackRestartChain: after replayed txn, full ROLLBACK
    // restores the in-txn snapshot that existed right after the last ROLLBACK TO SAVEPOINT (here: two rows).
    EXPECT_NE(all2.find("rows=2"), std::string::npos) << all2;
  }
  client2.close();
  eng2.shutdown();
}

/// 与 `TxnStrictNestedSavepointsPageJsonRecoverRestart` 相同嵌套 + 大表分页，但在 **COMMIT** 后冷启动；
/// 验证存储持久化：`USE` + `COUNT` 为 40，且无未提交 txn（与计划 §「COMMIT + 第二轮冷启动」一致）。
TEST(Mdb, TxnStrictNestedSavepointsPageJsonCommitRestart) {
  TxnChainStrictRepl T("mdb_nested_pg_commit");
  ASSERT_TRUE(T.start());
  ASSERT_TRUE(T.strict_new_table_from_create("npc", "DEFATTR(s:string)"));
  ASSERT_TRUE(T.repl("INSERT(1,r1)").ok);
  ASSERT_TRUE(T.repl("BEGIN").ok);
  ASSERT_TRUE(T.repl("INSERT(2,r2)").ok);
  ASSERT_TRUE(T.repl("SAVEPOINT s1").ok);
  ASSERT_TRUE(T.repl("INSERT(3,r3)").ok);
  ASSERT_TRUE(T.repl("SAVEPOINT s2").ok);
  ASSERT_TRUE(T.repl("INSERT(4,r4)").ok);
  ASSERT_TRUE(T.repl("ROLLBACK TO SAVEPOINT s2").ok);
  T.log.clear();
  ASSERT_TRUE(T.repl("COUNT").ok);
  EXPECT_NE(join_logs(T.log).find("rows=3"), std::string::npos) << join_logs(T.log);
  ASSERT_TRUE(T.repl("ROLLBACK TO SAVEPOINT s1").ok);
  T.log.clear();
  ASSERT_TRUE(T.repl("COUNT").ok);
  EXPECT_NE(join_logs(T.log).find("rows=2"), std::string::npos) << join_logs(T.log);
  for (int id = 3; id <= 40; ++id) {
    const std::string line = "INSERT(" + std::to_string(id) + ",r" + std::to_string(id) + ")";
    ASSERT_TRUE(T.repl(line).ok) << T.err << " line=" << line;
  }
  T.log.clear();
  ASSERT_TRUE(T.repl("PAGE_JSON(2,10,id,asc)").ok) << T.err;
  {
    const std::string pj = join_logs(T.log);
    EXPECT_NE(pj.find("[PAGE_JSON]{"), std::string::npos) << pj;
    // Page 2 asc: ids 11..20 → first row rank 11, id "11".
    EXPECT_NE(pj.find("[\"11\",\"11\",\"r11\""), std::string::npos) << pj;
  }
  ASSERT_TRUE(T.repl("COMMIT").ok) << T.err;
  T.client.close();
  T.eng.shutdown();

  std::string err3;
  structdb::facade::Engine eng3;
  eng3.config().update(1, T.snap);
  ASSERT_TRUE(eng3.startup(&err3)) << err3;
  structdb::client::EmbedClient client3(eng3);
  ASSERT_TRUE(client3.open(T.root / "session", &err3)) << err3;
  structdb::client::mdb::MdbInteractiveSession session3;
  std::vector<std::string> log3;
  auto run3 = [&](const char* line) {
    err3.clear();
    return structdb::client::mdb::mdb_repl_execute_line(eng3, client3, session3, line, &log3, false, false, &err3);
  };
  ASSERT_TRUE(run3("USE(npc)").ok) << err3;
  log3.clear();
  ASSERT_TRUE(run3("COUNT").ok) << err3;
  EXPECT_NE(join_logs(log3).find("rows=40"), std::string::npos) << join_logs(log3);
  client3.close();
  eng3.shutdown();
}

/// `SETATTRMULTI` 解析：大小写、空白、内层 CSV 形态（与 `Phase25StrictParseMatrix` 互补）。
TEST(Mdb, SetAttrMultiParseMatrix) {
  using structdb::client::mdb::MdbParsedLine;
  using structdb::client::mdb::MdbVerb;
  using structdb::client::mdb::mdb_parse_command_line;
  MdbParsedLine pl{};
  std::string pe;
  ASSERT_TRUE(mdb_parse_command_line("SETATTRMULTI(col,val,1,2)", &pl, &pe));
  EXPECT_EQ(pl.verb, MdbVerb::SetAttrMulti);
  EXPECT_EQ(std::string(pl.paren_inner), "col,val,1,2");

  ASSERT_TRUE(mdb_parse_command_line("setattrmulti( a , b , c )", &pl, &pe));
  EXPECT_EQ(pl.verb, MdbVerb::SetAttrMulti);
  EXPECT_EQ(std::string(pl.paren_inner), " a , b , c ");

  ASSERT_FALSE(mdb_parse_command_line("SETATTRMULTI(", &pl, &pe));
  EXPECT_FALSE(pe.empty());
}

/// 失败语义矩阵：缺表、参数不足、未知列、类型不匹配；成功分支含「跳过不存在 id」仅更新计数。
TEST(Mdb, SetAttrMultiFailureAndPartialSuccessMatrix) {
  {
    TxnChainStrictRepl T0("mdb_sam_notable");
    ASSERT_TRUE(T0.start());
    const auto r0 = T0.repl("SETATTRMULTI(s,x,1)");
    ASSERT_FALSE(r0.ok);
    EXPECT_EQ(r0.last_error, "no table");
    T0.close();
  }

  TxnChainStrictRepl T("mdb_sam_fail");
  ASSERT_TRUE(T.start());
  ASSERT_TRUE(T.strict_new_table_from_create("sam", "DEFATTR(s:string,n:int)"));
  {
    const auto r1 = T.repl("SETATTRMULTI(s)");
    ASSERT_FALSE(r1.ok);
    EXPECT_NE(r1.last_error.find("need"), std::string::npos) << r1.last_error;
  }
  ASSERT_TRUE(T.repl("INSERT(1,a,10)").ok);
  ASSERT_TRUE(T.repl("INSERT(2,b,20)").ok);
  {
    const auto r2 = T.repl("SETATTRMULTI(no_col,z,1)");
    ASSERT_FALSE(r2.ok);
    EXPECT_NE(r2.last_error.find("unknown"), std::string::npos) << r2.last_error;
  }
  {
    const auto r3 = T.repl("SETATTRMULTI(n,not_int,1)");
    ASSERT_FALSE(r3.ok);
    EXPECT_NE(r3.last_error.find("int"), std::string::npos) << r3.last_error;
  }

  T.log.clear();
  ASSERT_TRUE(T.repl("SETATTRMULTI(s,Z,1,2,ghost)").ok);
  {
    const std::string all = join_logs(T.log);
    EXPECT_NE(all.find("updated 2"), std::string::npos) << all;
  }
  T.log.clear();
  ASSERT_TRUE(T.repl("WHERE(s,=,Z)").ok);
  EXPECT_NE(join_logs(T.log).find("matched 2"), std::string::npos) << join_logs(T.log);
  T.close();
}

/// 嵌套 SAVEPOINT：`SETATTRMULTI` → 更深 savepoint 再改 → `ROLLBACK TO` 分层恢复语义。
TEST(Mdb, SetAttrMultiNestedSavepointRollbackMatrix) {
  TxnChainStrictRepl T("mdb_sam_nested");
  ASSERT_TRUE(T.start());
  ASSERT_TRUE(T.strict_new_table_from_create("nest", "DEFATTR(s:string)"));
  ASSERT_TRUE(T.repl("INSERT(1,a)").ok);
  ASSERT_TRUE(T.repl("INSERT(2,b)").ok);
  ASSERT_TRUE(T.repl("BEGIN").ok);
  ASSERT_TRUE(T.repl("SAVEPOINT s1").ok);
  ASSERT_TRUE(T.repl("SETATTRMULTI(s,X,1,2)").ok);
  T.log.clear();
  ASSERT_TRUE(T.repl("WHERE(s,=,X)").ok);
  EXPECT_NE(join_logs(T.log).find("matched 2"), std::string::npos);

  ASSERT_TRUE(T.repl("SAVEPOINT s2").ok);
  ASSERT_TRUE(T.repl("SETATTRMULTI(s,Y,1)").ok);
  T.log.clear();
  ASSERT_TRUE(T.repl("WHERE(s,=,Y)").ok);
  EXPECT_NE(join_logs(T.log).find("matched 1"), std::string::npos);

  ASSERT_TRUE(T.repl("ROLLBACK TO SAVEPOINT s2").ok);
  T.log.clear();
  ASSERT_TRUE(T.repl("WHERE(s,=,X)").ok);
  EXPECT_NE(join_logs(T.log).find("matched 2"), std::string::npos);

  ASSERT_TRUE(T.repl("ROLLBACK TO SAVEPOINT s1").ok);
  T.log.clear();
  ASSERT_TRUE(T.repl("WHERE(s,=,a)").ok);
  EXPECT_NE(join_logs(T.log).find("matched 1"), std::string::npos);
  ASSERT_TRUE(T.repl("WHERE(s,=,b)").ok);
  EXPECT_NE(join_logs(T.log).find("matched 1"), std::string::npos);

  ASSERT_TRUE(T.repl("ROLLBACK").ok);
  T.log.clear();
  ASSERT_TRUE(T.repl("COUNT").ok);
  EXPECT_NE(join_logs(T.log).find("rows=2"), std::string::npos);
  T.close();
}

namespace {

bool run_script_restart_verify_where(const fs::path& root, structdb::facade::EngineConfigSnapshot snap,
                                     std::string_view script_text, std::string_view expect_substr_after_restart) {
  fs::remove_all(root);
  fs::create_directories(root);
  snap.data_dir = (root / "_data").string();
  structdb::facade::Engine eng;
  eng.config().update(1, snap);
  std::string err;
  if (!eng.startup(&err)) return false;
  structdb::client::EmbedClient client(eng);
  if (!client.open(root / "sess", &err)) {
    eng.shutdown();
    return false;
  }
  const fs::path script = root / "run.mdb";
  write_script(script, script_text);
  structdb::client::mdb::MdbRunOptions opt;
  opt.script_path = script;
  if (!structdb::client::mdb::run_mdb_script(eng, client, opt).ok) {
    client.close();
    eng.shutdown();
    return false;
  }
  client.close();
  eng.shutdown();

  structdb::facade::Engine eng2;
  eng2.config().update(1, snap);
  if (!eng2.startup(&err)) return false;
  structdb::client::EmbedClient client2(eng2);
  if (!client2.open(root / "sess", &err)) {
    eng2.shutdown();
    return false;
  }
  structdb::client::mdb::MdbInteractiveSession session;
  std::vector<std::string> log;
  if (!structdb::client::mdb::mdb_repl_execute_line(eng2, client2, session, "USE(six)", &log, false, false, &err).ok) {
    client2.close();
    eng2.shutdown();
    return false;
  }
  log.clear();
  if (!structdb::client::mdb::mdb_repl_execute_line(eng2, client2, session, "WHERE(s,=,Z)", &log, false, false, &err).ok) {
    client2.close();
    eng2.shutdown();
    return false;
  }
  const std::string j = join_logs(log);
  const bool ok = j.find(expect_substr_after_restart) != std::string::npos;
  client2.close();
  eng2.shutdown();
  return ok;
}

}  // namespace

/// `mdb_incremental_persist` 开/关：同脚本 + 冷启动后 `WHERE` 命中行数须一致（全量 / 增量路径等价）。
TEST(Mdb, SetAttrMultiIncrementalPersistParityAfterRestart) {
  const std::string_view scr =
      "CREATE TABLE(six)\n"
      "USE(six)\n"
      "DEFATTR(s:string)\n"
      "INSERT(1,a)\n"
      "INSERT(2,b)\n"
      "INSERT(3,c)\n"
      "SETATTRMULTI(s,Z,1,2,nope)\n";

  const auto root_on = temp_dir("mdb_inc_on");
  const auto root_off = temp_dir("mdb_inc_off");
  structdb::facade::EngineConfigSnapshot snap_on;
  snap_on.mdb_incremental_persist = true;
  structdb::facade::EngineConfigSnapshot snap_off;
  snap_off.mdb_incremental_persist = false;

  ASSERT_TRUE(run_script_restart_verify_where(root_on, snap_on, scr, "matched 2"))
      << "incremental on: expected WHERE matched 2 after restart";
  ASSERT_TRUE(run_script_restart_verify_where(root_off, snap_off, scr, "matched 2"))
      << "incremental off: expected WHERE matched 2 after restart";
}

/// 二级索引：`SETATTRMULTI` + flush 后两行侧车 posting，重启后 `WHERE` 仍命中两行。
TEST(Mdb, SetAttrMultiSecondaryIndexFlushRestart) {
  const auto root = temp_dir("mdb_sam_sec");
  fs::remove_all(root);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "s", &err)) << err;
  const fs::path script = root / "sec.mdb";
  write_script(script,
               "CREATE TABLE(ix)\n"
               "USE(ix)\n"
               "DEFATTR(s:string)\n"
               "INSERT(1,Alice)\n"
               "INSERT(2,Bob)\n"
               "SETATTRMULTI(s,Zed,1,2)\n");
  structdb::client::mdb::MdbRunOptions opt;
  opt.script_path = script;
  ASSERT_TRUE(structdb::client::mdb::run_mdb_script(eng, client, opt).ok);
  ASSERT_TRUE(eng.storage()->flush_memtable(nullptr));

  namespace mk = structdb::storage::mdb_keyspace;
  const std::string pfx = std::string(mk::kSecIdx) + "ix$s$";
  std::size_t n = 0;
  eng.kv_visit_prefix(pfx, [&](std::string_view key, std::string_view) {
    (void)key;
    ++n;
    return true;
  });
  // Two rows with `s=Zed` → two postings under the string sidecar prefix (no stale Alice/Bob-only keys for these ids).
  EXPECT_EQ(n, 2u);

  client.close();
  eng.shutdown();

  structdb::facade::Engine eng2;
  eng2.config().update(1, snap);
  ASSERT_TRUE(eng2.startup(&err)) << err;
  structdb::client::EmbedClient client2(eng2);
  ASSERT_TRUE(client2.open(root / "s", &err)) << err;
  structdb::client::mdb::MdbInteractiveSession session;
  std::vector<std::string> log;
  ASSERT_TRUE(
      structdb::client::mdb::mdb_repl_execute_line(eng2, client2, session, "USE(ix)", &log, false, false, &err).ok);
  log.clear();
  ASSERT_TRUE(
      structdb::client::mdb::mdb_repl_execute_line(eng2, client2, session, "WHERE(s,=,Zed)", &log, false, false, &err).ok);
  EXPECT_NE(join_logs(log).find("matched 2"), std::string::npos) << join_logs(log);
  client2.close();
  eng2.shutdown();
}

/// 未提交事务 + txn v2：`SETATTRMULTI` 重放后计数与列值，再 `ROLLBACK` 回到基线。
TEST(Mdb, SetAttrMultiTxnRecoverRollbackMatrix) {
  const auto root = temp_dir("mdb_sam_txn_rec");
  fs::remove_all(root);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "trx", &err)) << err;
  structdb::client::mdb::MdbInteractiveSession session;
  std::vector<std::string> log;
  auto run = [&](const char* line, bool fsync_batch) {
    err.clear();
    return structdb::client::mdb::mdb_repl_execute_line(eng, client, session, line, &log, false, fsync_batch, &err);
  };
  ASSERT_TRUE(run("CREATE TABLE(tr)", false).ok);
  ASSERT_TRUE(run("USE(tr)", false).ok);
  ASSERT_TRUE(run("DEFATTR(s:string)", false).ok);
  ASSERT_TRUE(run("INSERT(1,a)", false).ok);
  ASSERT_TRUE(run("INSERT(2,b)", false).ok);
  ASSERT_TRUE(run("BEGIN", false).ok);
  ASSERT_TRUE(run("SETATTRMULTI(s,QQ,1,2)", true).ok);
  client.close();
  eng.shutdown();

  structdb::facade::Engine eng2;
  eng2.config().update(1, snap);
  ASSERT_TRUE(eng2.startup(&err)) << err;
  structdb::client::EmbedClient client2(eng2);
  ASSERT_TRUE(client2.open(root / "trx", &err)) << err;
  structdb::client::mdb::MdbInteractiveSession session2;
  std::vector<std::string> log2;
  auto run2 = [&](const char* line) {
    err.clear();
    return structdb::client::mdb::mdb_repl_execute_line(eng2, client2, session2, line, &log2, false, false, &err);
  };
  bool saw_recover = false;
  ASSERT_TRUE(run2("COUNT").ok);
  for (const auto& ln : log2) {
    if (ln.find("[RECOVER]") != std::string::npos) saw_recover = true;
  }
  EXPECT_TRUE(saw_recover) << join_logs(log2);
  EXPECT_NE(join_logs(log2).find("rows=2"), std::string::npos) << join_logs(log2);
  log2.clear();
  ASSERT_TRUE(run2("WHERE(s,=,QQ)").ok);
  EXPECT_NE(join_logs(log2).find("matched 2"), std::string::npos) << join_logs(log2);
  ASSERT_TRUE(run2("ROLLBACK").ok);
  log2.clear();
  ASSERT_TRUE(run2("WHERE(s,=,a)").ok);
  EXPECT_NE(join_logs(log2).find("matched 1"), std::string::npos) << join_logs(log2);
  ASSERT_TRUE(run2("WHERE(s,=,b)").ok);
  EXPECT_NE(join_logs(log2).find("matched 1"), std::string::npos) << join_logs(log2);
  client2.close();
  eng2.shutdown();
}

/// 复杂脚本：事务内 `SETATTRMULTI` + `UPDATEWHERE` + `DELETEWHERE` + `COMMIT`，重启后行数与 `WHERE` 一致。
TEST(Mdb, SetAttrMultiComplexTxnCommitRestartMatrix) {
  const auto root = temp_dir("mdb_sam_cplx");
  fs::remove_all(root);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "s", &err)) << err;
  const fs::path script = root / "cplx.mdb";
  write_script(script,
               "CREATE TABLE(cx)\n"
               "USE(cx)\n"
               "DEFATTR(s:string,n:int)\n"
               "INSERT(1,a,1)\n"
               "INSERT(2,b,2)\n"
               "INSERT(3,c,3)\n"
               "BEGIN\n"
               "SETATTRMULTI(s,X,1,2)\n"
               "UPDATEWHERE(n, 9, WHERE, id, =, 3)\n"
               "DELETEWHERE(s, =, X)\n"
               "COMMIT\n");
  structdb::client::mdb::MdbRunOptions opt;
  opt.script_path = script;
  ASSERT_TRUE(structdb::client::mdb::run_mdb_script(eng, client, opt).ok);
  client.close();
  eng.shutdown();

  structdb::facade::Engine eng2;
  eng2.config().update(1, snap);
  ASSERT_TRUE(eng2.startup(&err)) << err;
  structdb::client::EmbedClient client2(eng2);
  ASSERT_TRUE(client2.open(root / "s", &err)) << err;
  structdb::client::mdb::MdbInteractiveSession session;
  std::vector<std::string> log;
  ASSERT_TRUE(
      structdb::client::mdb::mdb_repl_execute_line(eng2, client2, session, "USE(cx)", &log, false, false, &err).ok);
  log.clear();
  ASSERT_TRUE(structdb::client::mdb::mdb_repl_execute_line(eng2, client2, session, "COUNT", &log, false, false, &err).ok);
  EXPECT_NE(join_logs(log).find("rows=1"), std::string::npos) << join_logs(log);
  log.clear();
  ASSERT_TRUE(
      structdb::client::mdb::mdb_repl_execute_line(eng2, client2, session, "WHERE(s,=,c)", &log, false, false, &err).ok);
  EXPECT_NE(join_logs(log).find("matched 1"), std::string::npos) << join_logs(log);
  log.clear();
  ASSERT_TRUE(
      structdb::client::mdb::mdb_repl_execute_line(eng2, client2, session, "WHERE(n,=,9)", &log, false, false, &err).ok);
  EXPECT_NE(join_logs(log).find("matched 1"), std::string::npos) << join_logs(log);
  client2.close();
  eng2.shutdown();
}

/// 非 `id` 排序列分页：由全表 `std::sort` 改为 `partial_sort` 窗口，语义与「全排后切片」一致。
TEST(Mdb, PageJsonNonIdColumnPartialSortWindow) {
  TxnChainStrictRepl T("mdb_page_nonid");
  ASSERT_TRUE(T.start());
  ASSERT_TRUE(T.strict_new_table_from_create("pgn", "DEFATTR(s:string)"));
  ASSERT_TRUE(T.repl("INSERT(1,c)").ok);
  ASSERT_TRUE(T.repl("INSERT(2,a)").ok);
  ASSERT_TRUE(T.repl("INSERT(3,b)").ok);
  T.log.clear();
  ASSERT_TRUE(T.repl("PAGE_JSON(1,2,s,asc)").ok) << T.err;
  {
    const std::string out = join_logs(T.log);
    EXPECT_NE(out.find("[\"1\",\"2\",\"a\""), std::string::npos) << out;
    EXPECT_NE(out.find("[\"2\",\"3\",\"b\""), std::string::npos) << out;
  }
  T.log.clear();
  ASSERT_TRUE(T.repl("PAGE_JSON(2,1,s,asc)").ok) << T.err;
  {
    const std::string out2 = join_logs(T.log);
    EXPECT_NE(out2.find("[\"2\",\"3\",\"b\""), std::string::npos) << out2;
  }
  T.log.clear();
  ASSERT_TRUE(T.repl("PAGE_JSON(3,1,s,asc)").ok) << T.err;
  {
    const std::string out3 = join_logs(T.log);
    EXPECT_NE(out3.find("[\"3\",\"1\",\"c\""), std::string::npos) << out3;
  }
  T.close();
}
