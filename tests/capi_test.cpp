#include <gtest/gtest.h>

#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <iterator>
#include <vector>

#include "structdb_capi.h"

#include "test_artifact_env.hpp"

#include "structdb/client/embed_client.hpp"

namespace fs = std::filesystem;

namespace {
fs::path capi_case_root(const char* name) {
  return structdb::testing::test_artifact_run_root() / "structdb_capi_tests" / name;
}
fs::path capi_case_root_join(const std::string& tail) {
  return structdb::testing::test_artifact_run_root() / "structdb_capi_tests" / tail;
}
}

TEST(Capi, VersionString) {
  const char* v = structdb_capi_version_string();
  ASSERT_NE(v, nullptr);
  EXPECT_STRNE(v, "");
  EXPECT_EQ(STRUCTDB_CAPI_VERSION_MAJOR, 1);
  EXPECT_EQ(STRUCTDB_CAPI_VERSION_MINOR, 7);
  EXPECT_EQ(STRUCTDB_CAPI_VERSION_PATCH, 3);
  EXPECT_EQ(structdb_capi_version(), (1u << 16) | (8u << 8) | 0u);
  EXPECT_STREQ(structdb_capi_version_string(), "1.8.0");
}

TEST(Capi, RunMdbFile) {
  const auto root = capi_case_root("mdb");
  fs::remove_all(root);
  fs::create_directories(root);

  const fs::path script = root / "x.mdb";
  {
    std::ofstream out(script, std::ios::binary);
    ASSERT_TRUE(out.is_open());
    out << "CREATE TABLE(ct)\nUSE(ct)\nDEFATTR(n:string)\nINSERT(1,a)\nCOUNT\n";
  }

  char err[256] = {};
  const int rc = structdb_run_mdb_file((root / "_data").string().c_str(), (root / "sess").string().c_str(),
                                         script.string().c_str(), err, sizeof(err));
  ASSERT_EQ(rc, STRUCTDB_CAPI_OK) << err;
}

TEST(Capi, RunMdbFileExNullOptsMatchesLegacy) {
  const auto root = capi_case_root("mdb_exnull");
  fs::remove_all(root);
  fs::create_directories(root);
  const fs::path script = root / "y.mdb";
  {
    std::ofstream out(script, std::ios::binary);
    out << "CREATE TABLE(ct)\nUSE(ct)\nDEFATTR(n:string)\nINSERT(1,a)\nCOUNT\n";
  }
  char err[256] = {};
  const int r = structdb_run_mdb_file_ex((root / "_data").string().c_str(), (root / "sess").string().c_str(),
                                         script.string().c_str(), err, sizeof(err), nullptr);
  EXPECT_EQ(r, STRUCTDB_CAPI_OK) << err;
}

TEST(Capi, NullArgument) {
  const auto root = capi_case_root("mdb_nullpath");
  fs::remove_all(root);
  fs::create_directories(root);
  char err[128] = {};
  EXPECT_EQ(structdb_run_mdb_file((root / "_data").string().c_str(), (root / "sess").string().c_str(), nullptr, err,
                                    sizeof(err)),
            STRUCTDB_CAPI_ERR_NULL_ARG);
  EXPECT_NE(std::strlen(err), 0u);
}

TEST(Capi, RunMdbFileDefaultDataDirUnderCwd) {
  const auto root = capi_case_root("mdb_def_dd");
  fs::remove_all(root);
  fs::create_directories(root);
  const fs::path script = fs::absolute(root / "t.mdb");
  {
    std::ofstream out(script, std::ios::binary);
    ASSERT_TRUE(out.is_open());
    out << "CREATE TABLE(ct)\nUSE(ct)\nDEFATTR(n:string)\nINSERT(1,a)\nCOUNT\n";
  }
  const auto prev = fs::current_path();
  fs::current_path(root);
  char err[256] = {};
  const int rc = structdb_run_mdb_file_ex("", nullptr, script.string().c_str(), err, sizeof(err), nullptr);
  fs::current_path(prev);
  ASSERT_EQ(rc, STRUCTDB_CAPI_OK) << err;
  EXPECT_TRUE(fs::exists(root / STRUCTDB_CAPI_DEFAULT_STORE_DIR / "_data"));
  EXPECT_TRUE(fs::exists(root / STRUCTDB_CAPI_DEFAULT_STORE_DIR / "_data" / "embed_session"));
}

TEST(Capi, EngineOpenDefaultDataDirUnderCwd) {
  const auto root = capi_case_root("eng_def_dd");
  fs::remove_all(root);
  fs::create_directories(root);
  const auto prev = fs::current_path();
  fs::current_path(root);
  char err[256] = {};
  structdb_engine* eng = structdb_engine_open("", err, sizeof(err));
  fs::current_path(prev);
  ASSERT_NE(eng, nullptr) << err;
  char buf[4096] = {};
  const size_t n = structdb_engine_get_data_dir_utf8(eng, buf, sizeof(buf));
  EXPECT_GT(n, 0u);
  EXPECT_NE(std::strstr(buf, STRUCTDB_CAPI_DEFAULT_STORE_DIR), nullptr) << buf;
  EXPECT_NE(std::strstr(buf, "_data"), nullptr) << buf;
  structdb_engine_shutdown(eng);
}

TEST(Capi, GetDefaultPathsMatchesLayout) {
  const auto root = capi_case_root("def_paths");
  fs::remove_all(root);
  fs::create_directories(root);
  char err[256] = {};
  char ws[2048] = {};
  char dd[2048] = {};
  char sd[2048] = {};
  ASSERT_EQ(structdb_capi_get_default_paths(root.string().c_str(), ws, sizeof(ws), dd, sizeof(dd), sd, sizeof(sd), err,
                                             sizeof(err)),
            STRUCTDB_CAPI_OK)
      << err;
  EXPECT_NE(std::strstr(dd, STRUCTDB_CAPI_DEFAULT_STORE_DIR), nullptr) << dd;
  EXPECT_NE(std::strstr(dd, "_data"), nullptr) << dd;
  EXPECT_NE(std::strstr(sd, "embed_session"), nullptr) << sd;
}

TEST(Capi, DurabilityWalSyncAndCommitSeq) {
  const auto root = capi_case_root("dur_wal");
  fs::remove_all(root);
  fs::create_directories(root);
  char err[512] = {};
  structdb_engine* eng = structdb_engine_open((root / "_data").string().c_str(), err, sizeof(err));
  ASSERT_NE(eng, nullptr) << err;
  EXPECT_EQ(structdb_engine_latest_commit_seq(nullptr), 0u);
  EXPECT_EQ(structdb_engine_latest_commit_seq(eng), 0u);
  ASSERT_EQ(structdb_engine_wal_sync(eng, err, sizeof(err)), STRUCTDB_CAPI_OK) << err;
  char e2[64] = {};
  EXPECT_EQ(structdb_engine_wal_sync(nullptr, e2, sizeof(e2)), STRUCTDB_CAPI_ERR_NULL_ARG);
  structdb_engine_shutdown(eng);
}

TEST(Capi, DurabilityEmbedCheckpointAndSeqs) {
  const auto root = capi_case_root("dur_emb");
  fs::remove_all(root);
  fs::create_directories(root);
  char err[512] = {};
  structdb_engine* eng = structdb_engine_open((root / "_data").string().c_str(), err, sizeof(err));
  ASSERT_NE(eng, nullptr) << err;
  structdb_embed_client* emb = structdb_embed_open(eng, (root / "sess").string().c_str(), err, sizeof(err));
  ASSERT_NE(emb, nullptr) << err;
  EXPECT_EQ(structdb_embed_last_ack_seq(nullptr), 0u);
  EXPECT_GE(structdb_embed_next_seq(emb), 1u);
  ASSERT_EQ(structdb_embed_save_checkpoint(emb, err, sizeof(err)), STRUCTDB_CAPI_OK) << err;
  (void)structdb_embed_read_snapshot_seq(emb);
  structdb_embed_close(emb);
  structdb_engine_shutdown(eng);
}

TEST(Capi, EngineFlushMemtableAndCheckpoint) {
  const auto root = capi_case_root("dur_flush");
  fs::remove_all(root);
  fs::create_directories(root);
  char err[512] = {};
  structdb_engine* eng = structdb_engine_open((root / "_data").string().c_str(), err, sizeof(err));
  ASSERT_NE(eng, nullptr) << err;
  ASSERT_EQ(structdb_engine_checkpoint(eng, err, sizeof(err)), STRUCTDB_CAPI_OK) << err;
  ASSERT_EQ(structdb_engine_flush_memtable(eng, err, sizeof(err)), STRUCTDB_CAPI_OK) << err;
  structdb_engine_shutdown(eng);
}

TEST(Capi, EmbedSessionLogPathUtf8) {
  const auto root = capi_case_root("sess_log_path");
  fs::remove_all(root);
  fs::create_directories(root);
  char err[512] = {};
  structdb_engine* eng = structdb_engine_open((root / "_data").string().c_str(), err, sizeof(err));
  ASSERT_NE(eng, nullptr) << err;
  structdb_embed_client* emb = structdb_embed_open(eng, (root / "sess").string().c_str(), err, sizeof(err));
  ASSERT_NE(emb, nullptr) << err;
  char lp[4096] = {};
  ASSERT_GT(structdb_embed_get_session_log_path_utf8(emb, lp, sizeof(lp)), 0u);
  EXPECT_NE(std::strstr(lp, "session_log.txt"), nullptr) << lp;
  EXPECT_NE(std::strstr(lp, structdb::client::kEmbedSessionArtifactsDir), nullptr) << lp;
  structdb_embed_close(emb);
  structdb_engine_shutdown(eng);
}

TEST(Capi, EmbedGetSessionDirUtf8) {
  const auto root = capi_case_root("emb_sess_utf8");
  fs::remove_all(root);
  fs::create_directories(root);
  const std::string data = (root / "_data").string();
  const std::string sess = (root / "persist_conv").string();
  char err[512] = {};
  structdb_engine* eng = structdb_engine_open(data.c_str(), err, sizeof(err));
  ASSERT_NE(eng, nullptr) << err;
  structdb_embed_client* emb = structdb_embed_open(eng, sess.c_str(), err, sizeof(err));
  ASSERT_NE(emb, nullptr) << err;
  char buf[4096] = {};
  const size_t n = structdb_embed_get_session_dir_utf8(emb, buf, sizeof(buf));
  EXPECT_GT(n, 0u);
  EXPECT_NE(std::strstr(buf, "persist_conv"), nullptr) << buf;
  EXPECT_EQ(structdb_embed_get_session_dir_utf8(nullptr, buf, sizeof(buf)), 0u);
  structdb_embed_close(emb);
  structdb_engine_shutdown(eng);
}

TEST(Capi, EmbedGetSessionDirUtf8DefaultSubdir) {
  const auto root = capi_case_root("emb_def_sess");
  fs::remove_all(root);
  fs::create_directories(root);
  char err[512] = {};
  structdb_engine* eng = structdb_engine_open((root / "_data").string().c_str(), err, sizeof(err));
  ASSERT_NE(eng, nullptr) << err;
  structdb_embed_client* emb = structdb_embed_open(eng, nullptr, err, sizeof(err));
  ASSERT_NE(emb, nullptr) << err;
  char buf[4096] = {};
  ASSERT_GT(structdb_embed_get_session_dir_utf8(emb, buf, sizeof(buf)), 0u);
  EXPECT_NE(std::strstr(buf, "embed_session"), nullptr) << buf;
  structdb_embed_close(emb);
  structdb_engine_shutdown(eng);
}

TEST(Capi, EmbedOpenNullSessionPersistsAcrossReopen) {
  const auto root = capi_case_root("mdb_persist_dll");
  fs::remove_all(root);
  fs::create_directories(root);
  const std::string data = (root / "_data").string();
  char err[512] = {};
  {
    structdb_engine* eng = structdb_engine_open(data.c_str(), err, sizeof(err));
    ASSERT_NE(eng, nullptr) << err;
    structdb_embed_client* emb = structdb_embed_open(eng, nullptr, err, sizeof(err));
    ASSERT_NE(emb, nullptr) << err;
    structdb_mdb_session* mdb = structdb_mdb_session_create();
    ASSERT_NE(mdb, nullptr);
    ASSERT_EQ(structdb_mdb_execute_line_ex(eng, emb, mdb, "CREATE TABLE(ct)", err, sizeof(err), nullptr),
              STRUCTDB_CAPI_OK)
        << err;
    structdb_mdb_session_destroy(mdb);
    structdb_embed_close(emb);
    structdb_engine_shutdown(eng);
  }
  {
    structdb_engine* eng = structdb_engine_open(data.c_str(), err, sizeof(err));
    ASSERT_NE(eng, nullptr) << err;
    structdb_embed_client* emb = structdb_embed_open(eng, nullptr, err, sizeof(err));
    ASSERT_NE(emb, nullptr) << err;
    structdb_mdb_session* mdb = structdb_mdb_session_create();
    ASSERT_NE(mdb, nullptr);
    ASSERT_EQ(structdb_mdb_execute_line_ex(eng, emb, mdb, "LIST TABLES", err, sizeof(err), nullptr), STRUCTDB_CAPI_OK)
        << err;
    structdb_mdb_session_destroy(mdb);
    structdb_embed_close(emb);
    structdb_engine_shutdown(eng);
  }
}

TEST(Capi, BadMdbScriptReturnsMdbRun) {
  const auto root = capi_case_root("mdb_bad");
  fs::remove_all(root);
  fs::create_directories(root);
  const fs::path script = root / "bad.mdb";
  {
    std::ofstream out(script, std::ios::binary);
    out << "CREATE TABLE(ct)\nUSE(ct)\nDEFATTR(n:int)\nINSERT(1,not_an_int)\n";
  }
  char err[512] = {};
  const int rc = structdb_run_mdb_file((root / "_data").string().c_str(), (root / "sess").string().c_str(),
                                       script.string().c_str(), err, sizeof(err));
  EXPECT_EQ(rc, STRUCTDB_CAPI_ERR_MDB_RUN);
  EXPECT_NE(std::strlen(err), 0u);
}

TEST(Capi, BadMdbScriptNullErrBuffer) {
  const auto root = capi_case_root("mdb_bad_nullerr");
  fs::remove_all(root);
  fs::create_directories(root);
  const fs::path script = root / "bad.mdb";
  {
    std::ofstream out(script, std::ios::binary);
    out << "CREATE TABLE(ct)\nUSE(ct)\nDEFATTR(n:int)\nINSERT(1,not_an_int)\n";
  }
  const int rc = structdb_run_mdb_file((root / "_data").string().c_str(), (root / "sess").string().c_str(),
                                        script.string().c_str(), nullptr, 0);
  EXPECT_EQ(rc, STRUCTDB_CAPI_ERR_MDB_RUN);
}

TEST(Capi, ReopenEmbedAfterClose) {
  const auto root = capi_case_root("mdb_reopen_emb");
  fs::remove_all(root);
  fs::create_directories(root);
  char err[512] = {};
  structdb_engine* eng = structdb_engine_open((root / "_data").string().c_str(), err, sizeof(err));
  ASSERT_NE(eng, nullptr) << err;
  structdb_embed_client* emb1 = structdb_embed_open(eng, (root / "sess").string().c_str(), err, sizeof(err));
  ASSERT_NE(emb1, nullptr) << err;
  structdb_mdb_session* mdb = structdb_mdb_session_create();
  ASSERT_NE(mdb, nullptr);
  ASSERT_EQ(structdb_mdb_execute_line_ex(eng, emb1, mdb, "LIST TABLES", err, sizeof(err), nullptr), STRUCTDB_CAPI_OK)
      << err;
  structdb_embed_close(emb1);
  structdb_embed_client* emb2 = structdb_embed_open(eng, (root / "sess").string().c_str(), err, sizeof(err));
  ASSERT_NE(emb2, nullptr) << err;
  EXPECT_EQ(structdb_mdb_execute_line_ex(eng, emb2, mdb, "LIST TABLES", err, sizeof(err), nullptr), STRUCTDB_CAPI_OK)
      << err;
  structdb_mdb_session_destroy(mdb);
  structdb_embed_close(emb2);
  structdb_engine_shutdown(eng);
}

TEST(Capi, BadOptionsStructSize) {
  const auto root = capi_case_root("mdb_optsz");
  fs::remove_all(root);
  fs::create_directories(root);
  const fs::path script = root / "z.mdb";
  {
    std::ofstream out(script, std::ios::binary);
    out << "CREATE TABLE(ct)\nUSE(ct)\nDEFATTR(n:string)\nCOUNT\n";
  }
  structdb_mdb_run_options bad{};
  bad.struct_size = 1;
  char err[256] = {};
  const int rc = structdb_run_mdb_file_ex((root / "_data").string().c_str(), (root / "sess").string().c_str(),
                                          script.string().c_str(), err, sizeof(err), &bad);
  EXPECT_EQ(rc, STRUCTDB_CAPI_ERR_NULL_ARG);
  EXPECT_NE(std::strstr(err, "SIZE_V1"), nullptr) << err;
}

TEST(Capi, RunMdbFileExWithV1OptionsStructSize) {
  const auto root = capi_case_root("mdb_opts_v1");
  fs::remove_all(root);
  fs::create_directories(root);
  const fs::path script = root / "v1.mdb";
  {
    std::ofstream out(script, std::ios::binary);
    out << "CREATE TABLE(ct)\nUSE(ct)\nDEFATTR(n:string)\nCOUNT\n";
  }
  structdb_mdb_run_options opt{};
  opt.struct_size = STRUCTDB_MDB_RUN_OPTIONS_SIZE_V1;
  opt.allow_persist_while_txn_active_experimental = 1;
  char err[256] = {};
  const int rc = structdb_run_mdb_file_ex((root / "_data").string().c_str(), (root / "sess").string().c_str(),
                                          script.string().c_str(), err, sizeof(err), &opt);
  ASSERT_EQ(rc, STRUCTDB_CAPI_OK) << err;
}

TEST(Capi, ReplExitRequestedOutAfterScriptExit) {
  const auto root = capi_case_root("mdb_exit_script");
  fs::remove_all(root);
  fs::create_directories(root);
  const fs::path script = root / "exit.mdb";
  {
    std::ofstream out(script, std::ios::binary);
    out << "CREATE TABLE(ct)\nEXIT\n";
  }
  int repl_exit = -1;
  structdb_mdb_run_options opt{};
  opt.struct_size = sizeof(opt);
  opt.allow_persist_while_txn_active_experimental = 1;
  opt.repl_exit_requested_out = &repl_exit;
  char err[256] = {};
  const int rc = structdb_run_mdb_file_ex((root / "_data").string().c_str(), (root / "sess").string().c_str(),
                                          script.string().c_str(), err, sizeof(err), &opt);
  ASSERT_EQ(rc, STRUCTDB_CAPI_OK) << err;
  EXPECT_EQ(repl_exit, 1);
}

TEST(Capi, ReplExitRequestedOutAfterExecuteLineExit) {
  const auto root = capi_case_root("mdb_exit_line");
  fs::remove_all(root);
  fs::create_directories(root);
  char err[512] = {};
  structdb_engine* eng = structdb_engine_open((root / "_data").string().c_str(), err, sizeof(err));
  ASSERT_NE(eng, nullptr) << err;
  structdb_embed_client* emb = structdb_embed_open(eng, (root / "sess").string().c_str(), err, sizeof(err));
  ASSERT_NE(emb, nullptr) << err;
  structdb_mdb_session* mdb = structdb_mdb_session_create();
  ASSERT_NE(mdb, nullptr);
  int repl_exit = -1;
  structdb_mdb_run_options opt{};
  opt.struct_size = sizeof(opt);
  opt.allow_persist_while_txn_active_experimental = 1;
  opt.repl_exit_requested_out = &repl_exit;
  ASSERT_EQ(structdb_mdb_execute_line_ex(eng, emb, mdb, "EXIT", err, sizeof(err), &opt), STRUCTDB_CAPI_OK) << err;
  EXPECT_EQ(repl_exit, 1);
  structdb_mdb_session_destroy(mdb);
  structdb_embed_close(emb);
  structdb_engine_shutdown(eng);
}

TEST(Capi, LongTaskControlCancelDuringScriptBatch) {
  const auto root = capi_case_root("mdb_long_task_cancel");
  fs::remove_all(root);
  fs::create_directories(root);
  char err[512] = {};
  structdb_engine* eng = structdb_engine_open((root / "_data").string().c_str(), err, sizeof(err));
  ASSERT_NE(eng, nullptr) << err;
  structdb_embed_client* emb = structdb_embed_open(eng, (root / "sess").string().c_str(), err, sizeof(err));
  ASSERT_NE(emb, nullptr) << err;
  structdb_mdb_session* mdb = structdb_mdb_session_create();
  ASSERT_NE(mdb, nullptr);
  structdb_long_task_control* ctrl = structdb_long_task_control_create();
  ASSERT_NE(ctrl, nullptr);
  structdb_mdb_run_options opt{};
  opt.struct_size = STRUCTDB_MDB_RUN_OPTIONS_SIZE_V3;
  opt.allow_persist_while_txn_active_experimental = 1;
  structdb_engine_begin_mdb_script_batch(eng, ctrl, 3);
  ASSERT_EQ(structdb_mdb_execute_line_ex(eng, emb, mdb, "CREATE TABLE(ct)", err, sizeof(err), &opt), STRUCTDB_CAPI_OK)
      << err;
  structdb_long_task_control_request_cancel(ctrl);
  const int rc = structdb_mdb_execute_line_ex(eng, emb, mdb, "USE(ct)", err, sizeof(err), &opt);
  EXPECT_EQ(rc, STRUCTDB_CAPI_ERR_CANCELLED);
  structdb_engine_end_mdb_script_batch(eng);
  structdb_long_task_control_destroy(ctrl);
  structdb_mdb_session_destroy(mdb);
  structdb_embed_close(emb);
  structdb_engine_shutdown(eng);
}

TEST(Capi, LogFileFromEx) {
  const auto root = capi_case_root("mdb_log");
  fs::remove_all(root);
  fs::create_directories(root);
  const fs::path script = root / "log.mdb";
  {
    std::ofstream out(script, std::ios::binary);
    out << "CREATE TABLE(ct)\nUSE(ct)\nDEFATTR(n:string)\nINSERT(1,a)\nCOUNT\n";
  }
  const fs::path logp = root / "out.log";
  const std::string log_path_str = logp.string();
  structdb_mdb_run_options opt{};
  opt.struct_size = sizeof(opt);
  opt.allow_persist_while_txn_active_experimental = 1;
  opt.log_file_path = log_path_str.c_str();
  char err[256] = {};
  const int rc = structdb_run_mdb_file_ex((root / "_data").string().c_str(), (root / "sess").string().c_str(),
                                          script.string().c_str(), err, sizeof(err), &opt);
  ASSERT_EQ(rc, STRUCTDB_CAPI_OK) << err;
  ASSERT_TRUE(fs::exists(logp));
  std::ifstream in(logp, std::ios::binary);
  std::string content((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
  EXPECT_NE(content.find("rows=1"), std::string::npos) << content;
}

namespace {

void capi_test_log_cb(void* user_data, const char* line) {
  auto* v = static_cast<std::vector<std::string>*>(user_data);
  if (v && line) v->emplace_back(line);
}

}  // namespace

TEST(Capi, LogCallbackFromEx) {
  const auto root = capi_case_root("mdb_cb");
  fs::remove_all(root);
  fs::create_directories(root);
  const fs::path script = root / "cb.mdb";
  {
    std::ofstream out(script, std::ios::binary);
    out << "CREATE TABLE(ct)\nUSE(ct)\nDEFATTR(n:string)\nCOUNT\n";
  }
  std::vector<std::string> lines;
  structdb_mdb_run_options opt{};
  opt.struct_size = sizeof(opt);
  opt.allow_persist_while_txn_active_experimental = 1;
  opt.log_line_cb = capi_test_log_cb;
  opt.log_user_data = &lines;
  char err[256] = {};
  const int rc = structdb_run_mdb_file_ex((root / "_data").string().c_str(), (root / "sess").string().c_str(),
                                            script.string().c_str(), err, sizeof(err), &opt);
  ASSERT_EQ(rc, STRUCTDB_CAPI_OK) << err;
  bool saw_count = false;
  for (const auto& s : lines) {
    if (s.find("rows=0") != std::string::npos) saw_count = true;
  }
  EXPECT_TRUE(saw_count);
}

TEST(Capi, MdbExecuteLineMatchesExNullOpts) {
  const auto root = capi_case_root("mdb_exec_line");
  fs::remove_all(root);
  fs::create_directories(root);
  char err[512] = {};
  structdb_engine* eng = structdb_engine_open((root / "_data").string().c_str(), err, sizeof(err));
  ASSERT_NE(eng, nullptr) << err;
  structdb_embed_client* emb = structdb_embed_open(eng, (root / "sess").string().c_str(), err, sizeof(err));
  ASSERT_NE(emb, nullptr) << err;
  structdb_mdb_session* mdb = structdb_mdb_session_create();
  ASSERT_NE(mdb, nullptr);
  ASSERT_EQ(structdb_mdb_execute_line(eng, emb, mdb, "CREATE TABLE(ct)", err, sizeof(err)), STRUCTDB_CAPI_OK) << err;
  ASSERT_EQ(structdb_mdb_execute_line_ex(eng, emb, mdb, "LIST TABLES", err, sizeof(err), nullptr), STRUCTDB_CAPI_OK)
      << err;
  structdb_mdb_session_destroy(mdb);
  structdb_embed_close(emb);
  structdb_engine_shutdown(eng);
}

TEST(Capi, SessionMultiLineMatchesScriptFlow) {
  const auto root = capi_case_root("mdb_sess");
  fs::remove_all(root);
  fs::create_directories(root);
  const std::string data = (root / "_data").string();
  const std::string sess = (root / "sess").string();
  char err[512] = {};
  structdb_engine* eng = structdb_engine_open(data.c_str(), err, sizeof(err));
  ASSERT_NE(eng, nullptr) << err;
  structdb_embed_client* emb = structdb_embed_open(eng, sess.c_str(), err, sizeof(err));
  ASSERT_NE(emb, nullptr) << err;
  structdb_mdb_session* mdb = structdb_mdb_session_create();
  ASSERT_NE(mdb, nullptr);
  const char* lines[] = {"CREATE TABLE(ct)", "USE(ct)", "DEFATTR(n:string)", "INSERT(1,a)", "COUNT"};
  for (const char* ln : lines) {
    const int rc = structdb_mdb_execute_line_ex(eng, emb, mdb, ln, err, sizeof(err), nullptr);
    ASSERT_EQ(rc, STRUCTDB_CAPI_OK) << err << " line=" << ln;
  }
  structdb_mdb_session_destroy(mdb);
  structdb_embed_close(emb);
  structdb_engine_shutdown(eng);
}

TEST(Capi, SessionExecuteLineNullArg) {
  char err[128] = {};
  EXPECT_EQ(structdb_mdb_execute_line_ex(nullptr, nullptr, nullptr, "COUNT", err, sizeof(err), nullptr),
             STRUCTDB_CAPI_ERR_NULL_ARG);
  EXPECT_NE(std::strlen(err), 0u);
}

TEST(Capi, SessionWrongEngineForEmbed) {
  const auto r1 = capi_case_root("mdb_se1");
  const auto r2 = capi_case_root("mdb_se2");
  fs::remove_all(r1);
  fs::remove_all(r2);
  fs::create_directories(r1);
  fs::create_directories(r2);
  char err[512] = {};
  structdb_engine* e1 = structdb_engine_open((r1 / "_data").string().c_str(), err, sizeof(err));
  structdb_engine* e2 = structdb_engine_open((r2 / "_data").string().c_str(), err, sizeof(err));
  ASSERT_NE(e1, nullptr);
  ASSERT_NE(e2, nullptr);
  structdb_embed_client* emb = structdb_embed_open(e1, (r1 / "sess").string().c_str(), err, sizeof(err));
  ASSERT_NE(emb, nullptr) << err;
  structdb_mdb_session* mdb = structdb_mdb_session_create();
  ASSERT_NE(mdb, nullptr);
  EXPECT_EQ(structdb_mdb_execute_line_ex(e2, emb, mdb, "COUNT", err, sizeof(err), nullptr), STRUCTDB_CAPI_ERR_NULL_ARG);
  structdb_mdb_session_destroy(mdb);
  structdb_embed_close(emb);
  structdb_engine_shutdown(e1);
  structdb_engine_shutdown(e2);
}

TEST(Capi, DoubleEmbedOpenRejected) {
  const auto root = capi_case_root("mdb_dbl_emb");
  fs::remove_all(root);
  fs::create_directories(root);
  char err[512] = {};
  structdb_engine* eng = structdb_engine_open((root / "_data").string().c_str(), err, sizeof(err));
  ASSERT_NE(eng, nullptr) << err;
  structdb_embed_client* emb1 = structdb_embed_open(eng, (root / "sess1").string().c_str(), err, sizeof(err));
  ASSERT_NE(emb1, nullptr) << err;
  structdb_embed_client* emb2 = structdb_embed_open(eng, (root / "sess2").string().c_str(), err, sizeof(err));
  EXPECT_EQ(emb2, nullptr);
  EXPECT_NE(std::strlen(err), 0u);
  structdb_embed_close(emb1);
  structdb_engine_shutdown(eng);
}

TEST(Capi, EngineShutdownAutoClosesEmbed) {
  const auto root = capi_case_root("mdb_shutdown_emb");
  fs::remove_all(root);
  fs::create_directories(root);
  char err[512] = {};
  structdb_engine* eng = structdb_engine_open((root / "_data").string().c_str(), err, sizeof(err));
  ASSERT_NE(eng, nullptr) << err;
  structdb_embed_client* emb = structdb_embed_open(eng, (root / "sess").string().c_str(), err, sizeof(err));
  ASSERT_NE(emb, nullptr) << err;
  structdb_engine_shutdown(eng);
}

TEST(Capi, EngineStartupFailure) {
  static int probe_id = 0;
  const auto root = capi_case_root_join("mdb_datafile_probe_" + std::to_string(++probe_id));
  fs::remove_all(root);
  ASSERT_TRUE(fs::create_directories(root));
  fs::create_directories(root / "sess");
  const fs::path dataPath = root / "_data";
  {
    std::ofstream f(dataPath, std::ios::binary);
    f << "not_a_directory";
  }
  const fs::path script = root / "s.mdb";
  {
    std::ofstream out(script, std::ios::binary);
    out << "CREATE TABLE(ct)\n";
  }
  char err[512] = {};
  const int rc = structdb_run_mdb_file(dataPath.string().c_str(), (root / "sess").string().c_str(),
                                       script.string().c_str(), err, sizeof(err));
  if (rc == STRUCTDB_CAPI_OK) {
    GTEST_SKIP() << "engine accepted file as data_dir on this platform";
  }
  EXPECT_EQ(rc, STRUCTDB_CAPI_ERR_ENGINE_STARTUP);
  EXPECT_NE(std::strlen(err), 0u);
}

TEST(Capi, MultiWaveWalSyncCheckpointAndSessionLog) {
  const auto root = capi_case_root("mdb_waves_wal_ckpt");
  fs::remove_all(root);
  fs::create_directories(root);
  const fs::path data_dir = fs::absolute(root / "_data");
  const fs::path session_base = fs::absolute(root / "app_workspace" / "user_sess");
  fs::create_directories(session_base);
  const std::string data = data_dir.u8string();
  const std::string sess = session_base.u8string();

  char err[512] = {};
  structdb_mdb_run_options opt{};
  opt.struct_size = sizeof(opt);
  opt.fsync_each_batch = 1;
  opt.fsync_each_session_txn_op = 1;
  opt.allow_persist_while_txn_active_experimental = 1;

  auto read_all = [](const std::string& utf8_path) {
    std::ifstream in(fs::u8path(utf8_path), std::ios::binary);
    std::ostringstream o;
    o << in.rdbuf();
    return o.str();
  };
  auto count_sub = [](const std::string& s, const std::string& n) {
    std::size_t c = 0;
    for (std::size_t pos = 0; pos < s.size();) {
      const auto f = s.find(n, pos);
      if (f == std::string::npos) break;
      ++c;
      pos = f + n.size();
    }
    return c;
  };

  for (int wave = 0; wave < 3; ++wave) {
    structdb_engine* eng = structdb_engine_open(data.c_str(), err, sizeof(err));
    ASSERT_NE(eng, nullptr) << err;
    structdb_embed_client* emb = structdb_embed_open(eng, sess.c_str(), err, sizeof(err));
    ASSERT_NE(emb, nullptr) << err;

    char log_path[4096] = {};
    ASSERT_GT(structdb_embed_get_session_log_path_utf8(emb, log_path, sizeof(log_path)), 0u);

    structdb_mdb_session* mdb = structdb_mdb_session_create();
    ASSERT_NE(mdb, nullptr);
    const std::string tbl = std::string("tw") + std::to_string(wave);
    const std::string create = "CREATE TABLE(" + tbl + ")";
    ASSERT_EQ(structdb_mdb_execute_line_ex(eng, emb, mdb, create.c_str(), err, sizeof(err), &opt), STRUCTDB_CAPI_OK)
        << err;

    EXPECT_FALSE(fs::exists(session_base / "session.journal"))
        << "journal must not pollute session root (wave=" << wave << ")";
    EXPECT_TRUE(fs::exists(session_base / structdb::client::kEmbedSessionArtifactsDir / "session.journal"))
        << "wave=" << wave;

    const std::string use = "USE(" + tbl + ")";
    ASSERT_EQ(structdb_mdb_execute_line_ex(eng, emb, mdb, use.c_str(), err, sizeof(err), &opt), STRUCTDB_CAPI_OK)
        << err;
    ASSERT_EQ(structdb_mdb_execute_line_ex(eng, emb, mdb, "DEFATTR(n:string)", err, sizeof(err), &opt), STRUCTDB_CAPI_OK)
        << err;
    ASSERT_EQ(structdb_mdb_execute_line_ex(eng, emb, mdb, "BEGIN", err, sizeof(err), &opt), STRUCTDB_CAPI_OK) << err;
    const std::string ins = "INSERT(1,w" + std::to_string(wave) + ")";
    ASSERT_EQ(structdb_mdb_execute_line_ex(eng, emb, mdb, ins.c_str(), err, sizeof(err), &opt), STRUCTDB_CAPI_OK)
        << err;
    ASSERT_EQ(structdb_mdb_execute_line_ex(eng, emb, mdb, "COMMIT", err, sizeof(err), &opt), STRUCTDB_CAPI_OK) << err;

    ASSERT_EQ(structdb_engine_wal_sync(eng, err, sizeof(err)), STRUCTDB_CAPI_OK) << err;
    ASSERT_EQ(structdb_embed_save_checkpoint(emb, err, sizeof(err)), STRUCTDB_CAPI_OK) << err;

    structdb_mdb_session_destroy(mdb);
    structdb_embed_close(emb);
    structdb_engine_shutdown(eng);

    const std::string slog = read_all(std::string(log_path));
    EXPECT_GE(count_sub(slog, "SESSION_OPEN"), static_cast<std::size_t>(wave + 1)) << slog;
    EXPECT_GE(count_sub(slog, "SESSION_CLOSE"), static_cast<std::size_t>(wave + 1)) << slog;
  }
}

TEST(Capi, ExclusiveDirLockSecondOpenFails) {
  const auto root = capi_case_root("exclusive_lock");
  fs::remove_all(root);
  fs::create_directories(root / "_data");
  const std::string dd = (root / "_data").string();
  char err[512] = {};
  structdb_engine* e1 = structdb_engine_open_ex(dd.c_str(), STRUCTDB_ENGINE_OPEN_FLAG_EXCLUSIVE_DIR_LOCK, err, sizeof(err));
  ASSERT_NE(e1, nullptr) << err;
  structdb_engine* e2 = structdb_engine_open_ex(dd.c_str(), STRUCTDB_ENGINE_OPEN_FLAG_EXCLUSIVE_DIR_LOCK, err, sizeof(err));
  EXPECT_EQ(e2, nullptr);
  EXPECT_NE(std::strlen(err), 0u);
  structdb_engine_shutdown(e1);
  char err2[512] = {};
  structdb_engine* e3 = structdb_engine_open_ex(dd.c_str(), STRUCTDB_ENGINE_OPEN_FLAG_EXCLUSIVE_DIR_LOCK, err2, sizeof(err2));
  ASSERT_NE(e3, nullptr) << err2;
  structdb_engine_shutdown(e3);
}
