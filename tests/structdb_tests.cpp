#include <gtest/gtest.h>
#include <gtest_capi.h>

#include <chrono>
#include <cstdint>
#include <limits>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>
#include <atomic>
#include <vector>

#include "structdb/client/embed_client.hpp"
#include "structdb/client/mdb_runner.hpp"
#include "structdb/facade/engine.hpp"
#include "structdb/infra/io_backend.hpp"
#include "structdb/infra/logging.hpp"
#include "structdb/orchestrator/orchestrator.hpp"
#include "structdb/planner/execution_plan.hpp"
#include "structdb/runtime/graph_executor.hpp"
#include "structdb/scheduler/budget.hpp"
#include "structdb/scheduler/scheduler.hpp"
#include "structdb/storage/checkpoint.hpp"
#include "structdb/storage/checkpoint_chain.hpp"
#include "structdb/storage/manifest.hpp"
#include "structdb/storage/mdb_keyspace.hpp"
#include "structdb/storage/storage_engine.hpp"
#include "structdb/storage/memtable_backend.hpp"
#include "structdb/storage/memtable_skiplist.hpp"
#include "structdb/infra/long_task_progress.hpp"
#include "structdb/storage/byte_token_bucket.hpp"
#include "structdb/storage/wal.hpp"

#include "test_artifact_env.hpp"

namespace {

std::filesystem::path temp_dir(const char* name) {
  return structdb::testing::test_artifact_run_root() / "structdb_tests" / name;
}

}  // namespace

TEST(Orchestrator, BackpressureCallbackFires) {
  structdb::scheduler::BudgetConfig cfg;
  cfg.memtable_bytes_budget = 0;
  auto budget = std::make_shared<structdb::scheduler::ResourceBudget>(cfg);
  auto sched = std::make_shared<structdb::scheduler::ExecutionScheduler>(budget);
  auto ex = std::make_shared<structdb::runtime::GraphExecutor>();
  struct Noop final : public structdb::runtime::IOperator {
    std::string_view name() const override { return "noop"; }
    bool execute(structdb::runtime::OperatorContext&) override { return true; }
  };
  ex->register_operator("noop", std::make_shared<Noop>());
  int hits = 0;
  structdb::orchestrator::Orchestrator orch(sched, ex, [](std::uint64_t ep) {
    return structdb::planner::ExecutionPlan::make_linear(ep, std::vector<std::string>{"noop"});
  });
  orch.set_on_backpressure([&](structdb::scheduler::BackpressureReason) { ++hits; });
  (void)orch;
  std::string err;
  structdb::runtime::GraphExecuteDiagnostics diag{};
  ASSERT_FALSE(ex->execute(structdb::planner::ExecutionPlan::make_linear(1, std::vector<std::string>{"noop"}),
                           *sched, true, &err, &diag));
  ASSERT_NE(err.find("backpressure:"), std::string::npos) << err;
  ASSERT_NE(err.find("MemTableFull"), std::string::npos) << err;
  ASSERT_EQ(diag.outcome, structdb::runtime::GraphExecuteOutcome::Backpressure);
  ASSERT_EQ(diag.backpressure_reason, structdb::scheduler::BackpressureReason::MemTableFull);
  ASSERT_GT(hits, 0);
}

TEST(Orchestrator, Phase22CompactionProbeBackpressureFires) {
  structdb::scheduler::BudgetConfig cfg;
  cfg.wal_queue_depth = 64;
  cfg.compaction_slots = 0;
  cfg.memtable_bytes_budget = 64 * 1024 * 1024;
  auto budget = std::make_shared<structdb::scheduler::ResourceBudget>(cfg);
  auto sched = std::make_shared<structdb::scheduler::ExecutionScheduler>(budget);
  auto ex = std::make_shared<structdb::runtime::GraphExecutor>();
  struct Noop final : public structdb::runtime::IOperator {
    std::string_view name() const override { return "noop"; }
    bool execute(structdb::runtime::OperatorContext&) override { return true; }
  };
  ex->register_operator("noop", std::make_shared<Noop>());
  int hits = 0;
  structdb::orchestrator::Orchestrator orch(sched, ex, [](std::uint64_t ep) {
    return structdb::planner::ExecutionPlan::make_linear(ep, std::vector<std::string>{"noop"});
  });
  orch.set_on_backpressure([&](structdb::scheduler::BackpressureReason) { ++hits; });
  (void)orch;
  std::string err;
  structdb::runtime::GraphExecuteDiagnostics diag{};
  ASSERT_FALSE(ex->execute(structdb::planner::ExecutionPlan::make_linear(1, std::vector<std::string>{"noop"}),
                           *sched, true, &err, &diag));
  ASSERT_NE(err.find("backpressure:"), std::string::npos) << err;
  ASSERT_NE(err.find("CompactionBusy"), std::string::npos) << err;
  ASSERT_EQ(diag.outcome, structdb::runtime::GraphExecuteOutcome::Backpressure);
  ASSERT_EQ(diag.backpressure_reason, structdb::scheduler::BackpressureReason::CompactionBusy);
  ASSERT_GT(hits, 0);
}

TEST(Planner, LinearDagValidates) {
  auto p = structdb::planner::ExecutionPlan::make_linear(1, std::vector<std::string>{"a", "b", "c"});
  std::string err;
  ASSERT_TRUE(p.validate_dag(&err)) << err;
}

TEST(Scheduler, MemTableBudgetBlocks) {
  structdb::scheduler::BudgetConfig cfg;
  cfg.memtable_bytes_budget = 4;
  auto b = std::make_shared<structdb::scheduler::ResourceBudget>(cfg);
  std::string why;
  ASSERT_TRUE(b->try_acquire(structdb::scheduler::ResourceType::MemTableBytes, 2, &why));
  ASSERT_TRUE(b->try_acquire(structdb::scheduler::ResourceType::MemTableBytes, 2, &why));
  ASSERT_FALSE(b->try_acquire(structdb::scheduler::ResourceType::MemTableBytes, 1, &why));
  b->release(structdb::scheduler::ResourceType::MemTableBytes, 4);
}

TEST(GraphExecutor, RunsLinearPlan) {
  structdb::scheduler::BudgetConfig cfg;
  auto budget = std::make_shared<structdb::scheduler::ResourceBudget>(cfg);
  structdb::scheduler::ExecutionScheduler sched(budget);
  structdb::runtime::GraphExecutor ex;
  structdb::planner::ExecutionPlan plan =
      structdb::planner::ExecutionPlan::make_linear(7, std::vector<std::string>{"noop"});

  struct Noop final : public structdb::runtime::IOperator {
    std::string_view name() const override { return "noop"; }
    bool execute(structdb::runtime::OperatorContext&) override { return true; }
  };
  ex.register_operator("noop", std::make_shared<Noop>());

  std::string err;
  structdb::runtime::GraphExecuteDiagnostics diag{};
  ASSERT_TRUE(ex.execute(std::move(plan), sched, true, &err, &diag)) << err;
  ASSERT_EQ(diag.outcome, structdb::runtime::GraphExecuteOutcome::Ok);
}

TEST(MemTableSkipList, PutGetSealOrderMatchesSortedExpectation) {
  structdb::storage::MemTableSkipList t;
  t.put("b", "2");
  t.put("a", "1");
  t.put("c", "3");
  std::string v;
  ASSERT_TRUE(t.get("a", &v));
  EXPECT_EQ(v, "1");
  std::vector<std::string> order;
  ASSERT_TRUE(t.for_each_sorted([&](const std::string& k, const std::string& val) {
    order.push_back(k + "=" + val);
    return true;
  }));
  ASSERT_EQ(order.size(), 3u);
  EXPECT_EQ(order[0], "a=1");
  EXPECT_EQ(order[1], "b=2");
  EXPECT_EQ(order[2], "c=3");
}

TEST(ByteTokenBucket, RefundRestoresBudgetAfterFailedConsume) {
  structdb::storage::SteadyClockByteTokenBucket tb;
  tb.set_max_bytes_per_second(1'000'000);
  tb.set_burst_bytes(4096);
  std::atomic<std::uint64_t> slept{0};
  tb.throttle(2048, &slept);
  tb.refund(2048);
  const auto t0 = std::chrono::steady_clock::now();
  tb.throttle(2048, &slept);
  const auto elapsed_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
  EXPECT_LT(elapsed_ms, 50);
}

TEST(ByteTokenBucket, ThrottleSleepsWhenBurstExhausted) {
  structdb::storage::SteadyClockByteTokenBucket tb;
  tb.set_max_bytes_per_second(4000);
  tb.set_burst_bytes(512);
  std::atomic<std::uint64_t> slept{0};
  tb.throttle(512, &slept);
  const auto t0 = std::chrono::steady_clock::now();
  tb.throttle(512, &slept);
  const auto elapsed_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
  EXPECT_GT(slept.load(), 0u);
  EXPECT_GE(elapsed_ms, 50);
}

TEST(LongTaskProgress, ReporterEmitsCancelAndComplete) {
  structdb::infra::LongTaskReporter reporter(structdb::infra::LongTaskKind::MdbScript);
  std::vector<structdb::infra::LongTaskProgressSnapshot> samples;
  reporter.set_progress_callback([&](const structdb::infra::LongTaskProgressSnapshot& s) { samples.push_back(s); });
  reporter.report(structdb::infra::LongTaskStatus::Running, 1, 3);
  EXPECT_TRUE(reporter.poll_cancel_and_report_cancelling() == false);
  reporter.cancel_token()->request_cancel();
  EXPECT_TRUE(reporter.poll_cancel_and_report_cancelling());
  reporter.report(structdb::infra::LongTaskStatus::Cancelled, 1, 3);
  ASSERT_GE(samples.size(), 3u);
  EXPECT_EQ(samples.back().status, structdb::infra::LongTaskStatus::Cancelled);
  EXPECT_STREQ(structdb::infra::long_task_kind_api_string(structdb::infra::LongTaskKind::MdbScript), "mdbScript");
}

TEST(Engine, StartupWithMemTableSkipListBackend) {
  const auto dir = temp_dir("eng_mem_skiplist");
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (dir / "_data").string();
  snap.version = 1;
  snap.memtable_backend = structdb::storage::MemTableBackend::SkipList;
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  ASSERT_TRUE(eng.storage()->put("sk", "v", false));
  std::string out;
  ASSERT_TRUE(eng.storage()->get("sk", &out));
  EXPECT_EQ(out, "v");
  EXPECT_EQ(eng.storage()->memtable_backend(), structdb::storage::MemTableBackend::SkipList);
  eng.shutdown();
}

TEST(Engine, DefaultMemTableBackendIsSkipList) {
  structdb::facade::EngineConfigSnapshot snap;
  EXPECT_EQ(snap.memtable_backend, structdb::storage::MemTableBackend::SkipList);
}

TEST(Engine, StartupShutdown) {
  const auto dir = temp_dir("engine_basic");
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);

  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (dir / "_data").string();
  snap.version = 1;
  eng.config().update(1, snap);

  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  ASSERT_NE(eng.storage(), nullptr);
  ASSERT_TRUE(eng.storage()->put("k", "v", false));
  std::string v;
  ASSERT_TRUE(eng.storage()->get("k", &v));
  ASSERT_EQ(v, "v");
  eng.shutdown();
}

TEST(Engine, WalReplaySurvivesRestartWithoutFlush) {
  const auto dir = temp_dir("wal_replay_restart");
  std::filesystem::remove_all(dir);
  namespace mk = structdb::storage::mdb_keyspace;
  const std::string k = std::string(mk::kCatalog) + "walreplay_t";
  {
    structdb::facade::Engine eng;
    structdb::facade::EngineConfigSnapshot snap;
    snap.data_dir = (dir / "_data").string();
    snap.version = 1;
    eng.config().update(1, snap);
    std::string err;
    ASSERT_TRUE(eng.startup(&err)) << err;
    ASSERT_TRUE(eng.kv_put(k, "1", true));
    eng.shutdown();
  }
  {
    structdb::facade::Engine eng2;
    structdb::facade::EngineConfigSnapshot snap2;
    snap2.data_dir = (dir / "_data").string();
    snap2.version = 1;
    eng2.config().update(1, snap2);
    std::string err;
    ASSERT_TRUE(eng2.startup(&err)) << err;
    std::string v;
    ASSERT_TRUE(eng2.kv_get(k, &v));
    EXPECT_EQ(v, "1");
    eng2.shutdown();
  }
}

TEST(Engine, StartupPassesStorageOpenFlags) {
  const auto dir = temp_dir("eng_storage_open_flags");
  std::filesystem::remove_all(dir);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (dir / "_data").string();
  snap.version = 1;
  snap.storage_open_flags = structdb::storage::StorageEngine::kOpenFlagRebuildUndoStackFromLog;
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  ASSERT_NE(eng.storage(), nullptr);
  eng.shutdown();
}

TEST(Engine, WalAutoTrimAfterFlushFromConfig) {
  const auto dir = temp_dir("wal_auto_trim_cfg");
  std::filesystem::remove_all(dir);
  namespace mk = structdb::storage::mdb_keyspace;
  const std::string rk = mk::row_key("wt_cfg", "1");
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (dir / "_data").string();
  snap.version = 1;
  snap.wal_auto_trim_prefix_after_flush = true;
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  ASSERT_TRUE(eng.storage()->put(rk, "a", false));
  ASSERT_TRUE(eng.storage()->put(rk, "b", false));
  ASSERT_TRUE(eng.storage()->flush_memtable(&err)) << err;
  structdb::storage::CheckpointState ck{};
  ASSERT_TRUE(eng.storage()->read_checkpoint_state(&ck));
  EXPECT_EQ(ck.wal_offset, 0u);
  EXPECT_EQ(eng.storage()->wal_log_bytes_on_disk(), 0u);
  eng.shutdown();
  structdb::storage::StorageEngine st2(dir / "_data");
  ASSERT_TRUE(st2.open(&err)) << err;
  std::string v;
  ASSERT_TRUE(st2.get(rk, &v));
  EXPECT_EQ(v, "b");
  st2.close();
}

TEST(StorageEngine, Phase4aObservabilityAfterVersionedPutAndFlush) {
  const auto dir = temp_dir("p4a_obs");
  std::filesystem::remove_all(dir);
  namespace mk = structdb::storage::mdb_keyspace;
  const std::string rk = mk::row_key("p4a_t", "1");
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  ASSERT_TRUE(st.put(rk, "first", false));
  EXPECT_GT(st.wal_log_bytes_on_disk(), 0u);
  ASSERT_TRUE(st.put(rk, "second", false));
  EXPECT_GT(st.undo_log_bytes_on_disk(), 0u);
  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  structdb::storage::CheckpointState ck{};
  ASSERT_TRUE(st.read_checkpoint_state(&ck));
  EXPECT_GT(ck.wal_offset, 0u);
  EXPECT_LE(ck.wal_offset, st.wal_log_bytes_on_disk());
  EXPECT_GE(ck.checkpoint_seq, 1u);
  st.close();
}

TEST(StorageEngine, WalTrimPrefixThroughCheckpointSurvivesRestart) {
  const auto dir = temp_dir("wal_trim_restart");
  std::filesystem::remove_all(dir);
  namespace mk = structdb::storage::mdb_keyspace;
  const std::string rk = mk::row_key("wtrim", "1");
  {
    structdb::storage::StorageEngine st(dir / "d");
    std::string err;
    ASSERT_TRUE(st.open(&err)) << err;
    ASSERT_TRUE(st.put(rk, "x", false));
    ASSERT_TRUE(st.put(rk, "y", false));
    ASSERT_TRUE(st.flush_memtable(&err)) << err;
    ASSERT_GT(st.wal_log_bytes_on_disk(), 0u);
    ASSERT_TRUE(st.wal_try_trim_prefix_through_checkpoint(&err)) << err;
    structdb::storage::CheckpointState ck{};
    ASSERT_TRUE(st.read_checkpoint_state(&ck));
    EXPECT_EQ(ck.wal_offset, 0u);
    EXPECT_EQ(st.wal_log_bytes_on_disk(), 0u);
    st.close();
  }
  {
    structdb::storage::StorageEngine st2(dir / "d");
    std::string err;
    ASSERT_TRUE(st2.open(&err)) << err;
    std::string v;
    ASSERT_TRUE(st2.get(rk, &v));
    EXPECT_EQ(v, "y");
    st2.close();
  }
}

TEST(StorageEngine, WalTrimRejectsCheckpointPastEof) {
  const auto dir = temp_dir("wal_trim_eof");
  std::filesystem::remove_all(dir);
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  ASSERT_TRUE(st.put("ztrim", "1", false));
  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  structdb::storage::CheckpointState ck{};
  ASSERT_TRUE(st.read_checkpoint_state(&ck));
  std::error_code ec;
  std::filesystem::remove(dir / "d" / "checkpoint.a", ec);
  std::filesystem::remove(dir / "d" / "checkpoint.b", ec);
  std::filesystem::remove(dir / "d" / "checkpoint.active", ec);
  std::ostringstream bad;
  bad << "1000000000000 " << ck.redo_offset << " " << ck.manifest_version << " " << ck.mdb_catalog_epoch << "\n";
  {
    std::ofstream out(dir / "d" / "checkpoint", std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(out.is_open());
    out << bad.str();
    out.flush();
  }
  ASSERT_FALSE(st.wal_try_trim_prefix_through_checkpoint(&err));
  ASSERT_NE(err.find("past EOF"), std::string::npos);
  st.close();
}

TEST(StorageEngine, CheckpointOpenRejectsCheckpointAheadOfManifest) {
  const auto dir = temp_dir("ckpt_ahead_manifest");
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir / "d");
  {
    std::ofstream man(dir / "d" / "MANIFEST", std::ios::trunc);
    man << "1\n0\n";
  }
  structdb::storage::CheckpointWriter cw;
  structdb::storage::CheckpointState st{};
  st.manifest_version = 2;
  std::string werr;
  ASSERT_TRUE(cw.write_rotating(dir / "d", st, &werr)) << werr;
  structdb::storage::StorageEngine eng(dir / "d");
  std::string err;
  EXPECT_FALSE(eng.open(&err));
  EXPECT_NE(err.find("checkpoint ahead of manifest"), std::string::npos);
  eng.close();
}

TEST(CheckpointWriter, ReadLatestFallsBackWhenActiveSlotCorrupt) {
  const auto dir = temp_dir("ckpt_fallback");
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  structdb::storage::CheckpointWriter cw;
  structdb::storage::CheckpointState a{};
  a.wal_offset = 10;
  a.manifest_version = 1;
  std::string werr;
  ASSERT_TRUE(cw.write_rotating(dir, a, &werr)) << werr;
  structdb::storage::CheckpointState b{};
  b.wal_offset = 20;
  b.manifest_version = 1;
  ASSERT_TRUE(cw.write_rotating(dir, b, &werr)) << werr;
  structdb::storage::CheckpointState cur{};
  ASSERT_TRUE(cw.read_latest(dir, &cur, &werr));
  EXPECT_EQ(cur.wal_offset, 20u);
  {
    std::ofstream trash(dir / "checkpoint.a", std::ios::binary | std::ios::trunc);
    trash << "XXXX";
  }
  structdb::storage::CheckpointState cur2{};
  ASSERT_TRUE(cw.read_latest(dir, &cur2, &werr));
  EXPECT_EQ(cur2.wal_offset, 20u);
}

TEST(StorageEngine, LegacyCheckpointOnlyStillOpens) {
  const auto dir = temp_dir("ckpt_legacy_only");
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir / "d");
  {
    std::ofstream ck(dir / "d" / "checkpoint", std::ios::trunc);
    ck << "0 0 0 0\n";
  }
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  st.close();
}

TEST(StorageEngine, VersionedUndoRollbackOne) {
  const auto dir = temp_dir("undo_rb");
  std::filesystem::remove_all(dir);
  namespace mk = structdb::storage::mdb_keyspace;
  const std::string rk = mk::row_key("urt", "1");
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  ASSERT_TRUE(st.put(rk, "first", false));
  ASSERT_TRUE(st.put(rk, "second", false));
  std::string v;
  ASSERT_TRUE(st.get(rk, &v));
  EXPECT_EQ(v, "second");
  ASSERT_TRUE(st.rollback_one_undo_frame(&err)) << err;
  ASSERT_TRUE(st.get(rk, &v));
  EXPECT_EQ(v, "first");
  st.close();
}

TEST(StorageEngine, RebuildUndoStackFromLogAfterClose) {
  const auto dir = temp_dir("undo_rebuild_ok");
  std::filesystem::remove_all(dir);
  namespace mk = structdb::storage::mdb_keyspace;
  const std::string rk = mk::row_key("urb_ok", "1");
  {
    structdb::storage::StorageEngine st(dir / "d");
    std::string err;
    ASSERT_TRUE(st.open(&err)) << err;
    ASSERT_TRUE(st.put(rk, "first", false));
    ASSERT_TRUE(st.put(rk, "second", false));
    std::string v;
    ASSERT_TRUE(st.get(rk, &v));
    EXPECT_EQ(v, "second");
    st.close();
  }
  {
    structdb::storage::StorageEngine st2(dir / "d");
    std::string err;
    ASSERT_TRUE(st2.open(&err, structdb::storage::StorageEngine::kOpenFlagRebuildUndoStackFromLog)) << err;
    std::string v;
    ASSERT_TRUE(st2.get(rk, &v));
    EXPECT_EQ(v, "second");
    ASSERT_TRUE(st2.rollback_one_undo_frame(&err)) << err;
    ASSERT_TRUE(st2.get(rk, &v));
    EXPECT_EQ(v, "first");
    st2.close();
  }
}

TEST(StorageEngine, RebuildUndoStackFromLogRejectsBadMagic) {
  const auto dir = temp_dir("undo_rebuild_bad");
  std::filesystem::remove_all(dir);
  namespace mk = structdb::storage::mdb_keyspace;
  const std::string rk = mk::row_key("urb_bad", "1");
  {
    structdb::storage::StorageEngine st(dir / "d");
    std::string err;
    ASSERT_TRUE(st.open(&err)) << err;
    ASSERT_TRUE(st.put(rk, "first", false));
    ASSERT_TRUE(st.put(rk, "second", false));
    st.close();
  }
  {
    std::ofstream app(dir / "d" / "undo.log", std::ios::binary | std::ios::app);
    ASSERT_TRUE(app.is_open());
    auto w32 = [&app](std::uint32_t v) {
      char b[4];
      b[0] = static_cast<char>(v & 0xffu);
      b[1] = static_cast<char>((v >> 8) & 0xffu);
      b[2] = static_cast<char>((v >> 16) & 0xffu);
      b[3] = static_cast<char>((v >> 24) & 0xffu);
      app.write(b, 4);
    };
    w32(8u);
    app.write("XXXXXXXX", 8);
  }
  structdb::storage::StorageEngine st2(dir / "d");
  std::string err;
  EXPECT_FALSE(st2.open(&err, structdb::storage::StorageEngine::kOpenFlagRebuildUndoStackFromLog));
  st2.close();
}

TEST(StorageEngine, UndoTruncateRejectsWhenStackNonEmpty) {
  const auto dir = temp_dir("undo_trunc_nonempty");
  std::filesystem::remove_all(dir);
  namespace mk = structdb::storage::mdb_keyspace;
  const std::string rk = mk::row_key("ut_ne", "1");
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  ASSERT_TRUE(st.put(rk, "first", false));
  ASSERT_TRUE(st.put(rk, "second", false));
  EXPECT_GT(st.undo_log_bytes_on_disk(), 0u);
  EXPECT_FALSE(st.undo_try_truncate_when_stack_empty(&err));
  EXPECT_EQ(err, "undo_stack not empty");
  st.close();
}

TEST(StorageEngine, UndoTruncateSucceedsWhenStackEmptyAfterRollback) {
  const auto dir = temp_dir("undo_trunc_rb");
  std::filesystem::remove_all(dir);
  namespace mk = structdb::storage::mdb_keyspace;
  const std::string rk = mk::row_key("ut_rb", "1");
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  ASSERT_TRUE(st.put(rk, "first", false));
  ASSERT_TRUE(st.put(rk, "second", false));
  ASSERT_TRUE(st.rollback_one_undo_frame(&err)) << err;
  ASSERT_TRUE(st.rollback_one_undo_frame(&err)) << err;
  ASSERT_TRUE(st.undo_try_truncate_when_stack_empty(&err)) << err;
  EXPECT_EQ(st.undo_log_bytes_on_disk(), 0u);
  ASSERT_TRUE(st.put(rk, "third", false));
  std::string v;
  ASSERT_TRUE(st.get(rk, &v));
  EXPECT_EQ(v, "third");
  st.close();
}

TEST(StorageEngine, UndoTruncateSucceedsAfterFlushMemtableClearsStack) {
  const auto dir = temp_dir("undo_trunc_flush");
  std::filesystem::remove_all(dir);
  namespace mk = structdb::storage::mdb_keyspace;
  const std::string rk = mk::row_key("ut_fl", "1");
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  ASSERT_TRUE(st.put(rk, "a", false));
  ASSERT_TRUE(st.put(rk, "b", false));
  EXPECT_GT(st.undo_log_bytes_on_disk(), 0u);
  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  ASSERT_TRUE(st.undo_try_truncate_when_stack_empty(&err)) << err;
  EXPECT_EQ(st.undo_log_bytes_on_disk(), 0u);
  std::string v;
  ASSERT_TRUE(st.get(rk, &v));
  EXPECT_EQ(v, "b");
  st.close();
}

TEST(StorageEngine, UndoTruncateThenOpenWithRebuildEmptyStack) {
  const auto dir = temp_dir("undo_trunc_reopen");
  std::filesystem::remove_all(dir);
  namespace mk = structdb::storage::mdb_keyspace;
  const std::string rk = mk::row_key("ut_ro", "1");
  {
    structdb::storage::StorageEngine st(dir / "d");
    std::string err;
    ASSERT_TRUE(st.open(&err)) << err;
    ASSERT_TRUE(st.put(rk, "x", false));
    ASSERT_TRUE(st.put(rk, "y", false));
    ASSERT_TRUE(st.flush_memtable(&err)) << err;
    ASSERT_TRUE(st.undo_try_truncate_when_stack_empty(&err)) << err;
    st.close();
  }
  structdb::storage::StorageEngine st2(dir / "d");
  std::string err;
  ASSERT_TRUE(st2.open(&err, structdb::storage::StorageEngine::kOpenFlagRebuildUndoStackFromLog)) << err;
  EXPECT_FALSE(st2.rollback_one_undo_frame(&err));
  std::string v;
  ASSERT_TRUE(st2.get(rk, &v));
  EXPECT_EQ(v, "y");
  st2.close();
}

TEST(Infra, IoBackendDefaultIsBlocking) {
  structdb::infra::IoBackendConfig c{};
  EXPECT_TRUE(structdb::infra::io_backend_kind_is_blocking(c.kind));
}

TEST(Infra, IoBackendPhase18AsyncPlaceholders) {
  structdb::infra::IoBackendConfig c{};
  c.kind = structdb::infra::IoBackendKind::IocpAsync;
  EXPECT_FALSE(structdb::infra::io_backend_kind_is_blocking(c.kind));
  EXPECT_TRUE(structdb::infra::io_backend_kind_is_async_placeholder(c.kind));
}

TEST(ResourceBudget, WalQueuePressureDeltaFromPhase14) {
  structdb::scheduler::BudgetConfig cfg;
  cfg.wal_queue_depth = 100;
  structdb::scheduler::ResourceBudget b(cfg);
  b.set_wal_queue_depth_pressure_delta(-50);
  std::string r;
  ASSERT_TRUE(b.try_acquire(structdb::scheduler::ResourceType::WalQueueDepth, 40, &r)) << r;
  ASSERT_FALSE(b.try_acquire(structdb::scheduler::ResourceType::WalQueueDepth, 60, &r));
}

TEST(ResourceBudget, CompactionSlotsPressureDeltaFromPhase21) {
  structdb::scheduler::BudgetConfig cfg;
  cfg.compaction_slots = 3;
  structdb::scheduler::ResourceBudget b(cfg);
  b.set_compaction_slots_pressure_delta(-1);
  std::string r;
  ASSERT_TRUE(b.try_acquire(structdb::scheduler::ResourceType::CompactionSlots, 1, &r)) << r;
  ASSERT_TRUE(b.try_acquire(structdb::scheduler::ResourceType::CompactionSlots, 1, &r)) << r;
  ASSERT_FALSE(b.try_acquire(structdb::scheduler::ResourceType::CompactionSlots, 1, &r));
}

TEST(StorageEngine, Phase13DeferL0CompactThenDrain) {
  const auto dir = temp_dir("p13_defer");
  std::filesystem::remove_all(dir);
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  st.set_l0_compact_trigger_threshold(2);
  st.set_l0_compact_max_rounds_per_flush(16);
  st.set_l0_compact_defer_after_flush(true);
  for (int i = 0; i < 5; ++i) {
    const std::string key = std::string("k") + std::to_string(i);
    ASSERT_TRUE(st.put(key, "v", false));
    ASSERT_TRUE(st.flush_memtable(&err)) << err;
  }
  EXPECT_EQ(st.manifest().sst_files().size(), 5u);
  ASSERT_TRUE(st.drain_pending_l0_compactions(16, &err)) << err;
  EXPECT_LE(st.manifest().sst_files().size(), 2u);
  for (int i = 0; i < 5; ++i) {
    const std::string key = std::string("k") + std::to_string(i);
    std::string v;
    ASSERT_TRUE(st.get(key, &v));
    EXPECT_EQ(v, "v");
  }
  st.close();
}

TEST(StorageEngine, Phase16WalSegmentsMetadataWritten) {
  const auto dir = temp_dir("p16_walseg");
  std::filesystem::remove_all(dir);
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  ASSERT_TRUE(st.put("a", "b", false));
  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  const auto seg = dir / "d" / "wal.segments";
  ASSERT_TRUE(std::filesystem::exists(seg));
  EXPECT_EQ(st.wal_log_segment_count_observed(), 1u);
  st.close();
}

TEST(Manifest, Phase15Format2LoadsLevel2) {
  const auto dir = temp_dir("p15_mnf");
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  const auto p = dir / "MANIFEST";
  {
    std::ofstream out(p);
    out << "1\nFORMAT2\n3\n0 a.sst\n1 b.sst\n2 c.sst\n";
  }
  structdb::storage::Manifest m;
  ASSERT_TRUE(m.load(p));
  ASSERT_EQ(m.sst_entries().size(), 3u);
  EXPECT_EQ(m.sst_entries()[2].level, 2u);
  EXPECT_EQ(m.sst_entries()[2].relative_path, "c.sst");
}

TEST(StorageEngine, Phase15L1ToL2Compact) {
  const auto dir = temp_dir("p15_l2");
  std::filesystem::remove_all(dir);
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  st.set_l1_compact_output_from_l0_merge(true);
  st.set_l2_compact_output_from_l1_merge(true);
  ASSERT_TRUE(st.put("k", "a", false));
  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  ASSERT_TRUE(st.put("k", "b", false));
  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  ASSERT_TRUE(st.compact_merge_two_oldest_l0(&err)) << err;
  ASSERT_TRUE(st.put("k2", "x", false));
  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  ASSERT_TRUE(st.put("k2", "y", false));
  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  ASSERT_TRUE(st.compact_merge_two_oldest_l0(&err)) << err;
  const auto& ent = st.manifest().sst_entries();
  ASSERT_EQ(ent.size(), 2u);
  EXPECT_EQ(ent[0].level, 1u);
  EXPECT_EQ(ent[1].level, 1u);
  ASSERT_TRUE(st.compact_merge_two_oldest_l1_to_l2(&err)) << err;
  ASSERT_EQ(st.manifest().sst_entries().size(), 1u);
  EXPECT_EQ(st.manifest().sst_entries()[0].level, 2u);
  std::string v;
  ASSERT_TRUE(st.get("k", &v));
  EXPECT_EQ(v, "b");
  ASSERT_TRUE(st.get("k2", &v));
  EXPECT_EQ(v, "y");
  st.close();
}

TEST(Manifest, Phase22Format2LoadsLevel3) {
  const auto dir = temp_dir("p22_mnf");
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  const auto p = dir / "MANIFEST";
  {
    std::ofstream out(p);
    out << "1\nFORMAT2\n4\n0 a.sst\n1 b.sst\n2 c.sst\n3 d.sst\n";
  }
  structdb::storage::Manifest m;
  ASSERT_TRUE(m.load(p));
  ASSERT_EQ(m.sst_entries().size(), 4u);
  EXPECT_EQ(m.sst_entries()[3].level, 3u);
  EXPECT_EQ(m.sst_entries()[3].relative_path, "d.sst");
}

TEST(StorageEngine, Phase22L2ToL3CompactAndRestart) {
  const auto dir = temp_dir("p22_l3");
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir / "d");
  auto write_sst_one_kv = [](const std::filesystem::path& p, const std::string& k, const std::string& v) {
    std::ofstream o(p, std::ios::binary);
    auto u32 = [&o](std::uint32_t x) {
      for (int i = 0; i < 4; ++i) o.put(static_cast<char>((x >> (8 * i)) & 0xff));
    };
    u32(static_cast<std::uint32_t>(k.size()));
    o.write(k.data(), static_cast<std::streamsize>(k.size()));
    u32(static_cast<std::uint32_t>(v.size()));
    o.write(v.data(), static_cast<std::streamsize>(v.size()));
  };
  write_sst_one_kv(dir / "d" / "L2-a.sst", "ka", "va");
  write_sst_one_kv(dir / "d" / "L2-b.sst", "kb", "vb");
  {
    std::ofstream m(dir / "d" / "MANIFEST", std::ios::trunc);
    m << "1\nFORMAT2\n2\n2 L2-a.sst\n2 L2-b.sst\n";
  }
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  st.set_l3_compact_output_from_l2_merge(true);
  ASSERT_TRUE(st.compact_merge_two_oldest_l2_to_l3(&err)) << err;
  std::size_t n3 = 0;
  for (const auto& e : st.manifest().sst_entries()) {
    if (e.level == 3) ++n3;
  }
  ASSERT_EQ(n3, 1u);
  std::string v;
  ASSERT_TRUE(st.get("ka", &v));
  EXPECT_EQ(v, "va");
  ASSERT_TRUE(st.get("kb", &v));
  EXPECT_EQ(v, "vb");
  st.close();

  structdb::storage::StorageEngine st2(dir / "d");
  ASSERT_TRUE(st2.open(&err)) << err;
  ASSERT_TRUE(st2.get("kb", &v));
  EXPECT_EQ(v, "vb");
  st2.close();
}

TEST(StorageEngine, Phase22UndoSegmentsRoll) {
  const auto dir = temp_dir("p22_undo_seg");
  std::filesystem::remove_all(dir);
  structdb::storage::StorageEngine st(dir / "d");
  st.set_undo_segment_roll_max_bytes(48);
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  const std::string key = "mdb$roll$k";
  for (int i = 0; i < 24; ++i) {
    const std::vector<std::pair<std::string, std::string>> puts{
        {key, std::string("v") + std::to_string(i)}};
    ASSERT_TRUE(st.commit_embed_batch({}, puts, false, &err)) << err;
  }
  EXPECT_GE(st.undo_log_segment_count_observed(), 2u);
  EXPECT_TRUE(std::filesystem::exists(dir / "d" / "undo.segments"));
  st.close();
}

TEST(StorageEngine, Phase23L0InlineCapLimitsRoundsPerFlush) {
  auto run_flush_burst = [](std::uint32_t inline_cap, std::uint64_t* merge_count_out) {
    const auto dir = temp_dir(inline_cap ? "p23_l0_cap1" : "p23_l0_cap0");
    std::filesystem::remove_all(dir);
    structdb::storage::StorageEngine st(dir / "d");
    std::string err;
    ASSERT_TRUE(st.open(&err)) << err;
    st.set_l0_compact_trigger_threshold(0);
    for (int i = 0; i < 10; ++i) {
      const std::string key = std::string("p23k") + std::to_string(i);
      ASSERT_TRUE(st.put(key, "v", false));
      ASSERT_TRUE(st.flush_memtable(&err)) << err;
    }
    ASSERT_EQ(st.manifest().l0_prefix_length(), 10u);
    st.set_l0_compact_trigger_threshold(2);
    st.set_l0_compact_max_rounds_per_flush(16);
    st.set_l0_compact_max_inline_rounds_per_flush(inline_cap);
    ASSERT_TRUE(st.put("p23k_last", "v", false));
    ASSERT_TRUE(st.flush_memtable(&err)) << err;
    *merge_count_out = st.compaction_merge_count();
    st.close();
  };
  std::uint64_t merges_capped = 0;
  std::uint64_t merges_full = 0;
  run_flush_burst(1, &merges_capped);
  run_flush_burst(0, &merges_full);
  EXPECT_GT(merges_full, merges_capped);
}

TEST(Manifest, Phase23Format2LoadsLevel4) {
  const auto dir = temp_dir("p23_mnf");
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  const auto p = dir / "MANIFEST";
  {
    std::ofstream out(p);
    out << "1\nFORMAT2\n5\n0 a.sst\n1 b.sst\n2 c.sst\n3 d.sst\n4 e.sst\n";
  }
  structdb::storage::Manifest m;
  ASSERT_TRUE(m.load(p));
  ASSERT_EQ(m.sst_entries().size(), 5u);
  EXPECT_EQ(m.sst_entries()[4].level, 4u);
  EXPECT_EQ(m.sst_entries()[4].relative_path, "e.sst");
}

TEST(StorageEngine, Phase23L3ToL4CompactAndRestart) {
  const auto dir = temp_dir("p23_l4");
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir / "d");
  auto write_sst_one_kv = [](const std::filesystem::path& p, const std::string& k, const std::string& v) {
    std::ofstream o(p, std::ios::binary);
    auto u32 = [&o](std::uint32_t x) {
      for (int i = 0; i < 4; ++i) o.put(static_cast<char>((x >> (8 * i)) & 0xff));
    };
    u32(static_cast<std::uint32_t>(k.size()));
    o.write(k.data(), static_cast<std::streamsize>(k.size()));
    u32(static_cast<std::uint32_t>(v.size()));
    o.write(v.data(), static_cast<std::streamsize>(v.size()));
  };
  write_sst_one_kv(dir / "d" / "L3-a.sst", "ka", "va");
  write_sst_one_kv(dir / "d" / "L3-b.sst", "kb", "vb");
  {
    std::ofstream m(dir / "d" / "MANIFEST", std::ios::trunc);
    m << "1\nFORMAT2\n2\n3 L3-a.sst\n3 L3-b.sst\n";
  }
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  st.set_l4_compact_output_from_l3_merge(true);
  ASSERT_TRUE(st.compact_merge_two_oldest_l3_to_l4(&err)) << err;
  std::size_t n4 = 0;
  for (const auto& e : st.manifest().sst_entries()) {
    if (e.level == 4) ++n4;
  }
  ASSERT_EQ(n4, 1u);
  std::string v;
  ASSERT_TRUE(st.get("ka", &v));
  EXPECT_EQ(v, "va");
  ASSERT_TRUE(st.get("kb", &v));
  EXPECT_EQ(v, "vb");
  st.close();

  structdb::storage::StorageEngine st2(dir / "d");
  ASSERT_TRUE(st2.open(&err)) << err;
  ASSERT_TRUE(st2.get("kb", &v));
  EXPECT_EQ(v, "vb");
  st2.close();
}

TEST(StorageEngine, StoragePressureWalAndFlushCounters) {
  const auto dir = temp_dir("st_pressure_wal_ct");
  std::filesystem::remove_all(dir);
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  structdb::storage::StoragePressureSnapshot p0{};
  st.read_storage_pressure_snapshot(&p0);
  EXPECT_EQ(p0.wal_append_record_calls_total, 0u);
  EXPECT_EQ(p0.wal_fsync_calls_total, 0u);
  EXPECT_EQ(p0.flush_memtable_success_total, 0u);
  EXPECT_EQ(p0.checkpoint_success_total, 0u);
  EXPECT_EQ(p0.compaction_merge_success_total, 0u);
  EXPECT_EQ(p0.compaction_worker_tasks_submitted_total, 0u);
  EXPECT_EQ(p0.compaction_worker_tasks_completed_total, 0u);

  ASSERT_TRUE(st.put("k1", "a", false));
  structdb::storage::StoragePressureSnapshot p1{};
  st.read_storage_pressure_snapshot(&p1);
  EXPECT_GE(p1.wal_append_record_calls_total, 1u);
  EXPECT_EQ(p1.wal_fsync_calls_total, 0u);
  EXPECT_GE(p1.wal_append_frame_bytes_committed_total, 8u);

  ASSERT_TRUE(st.put("k2", "b", true));
  structdb::storage::StoragePressureSnapshot p2{};
  st.read_storage_pressure_snapshot(&p2);
  EXPECT_GE(p2.wal_append_record_calls_total, 2u);
  EXPECT_GE(p2.wal_fsync_calls_total, 1u);

  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  structdb::storage::StoragePressureSnapshot p3{};
  st.read_storage_pressure_snapshot(&p3);
  EXPECT_GE(p3.flush_memtable_success_total, 1u);

  ASSERT_TRUE(st.checkpoint(&err)) << err;
  structdb::storage::StoragePressureSnapshot p4{};
  st.read_storage_pressure_snapshot(&p4);
  EXPECT_GE(p4.checkpoint_success_total, 1u);

  ASSERT_TRUE(st.wal_sync(&err)) << err;
  structdb::storage::StoragePressureSnapshot p5{};
  st.read_storage_pressure_snapshot(&p5);
  EXPECT_GE(p5.wal_fsync_calls_total, p2.wal_fsync_calls_total + 1u);

  st.close();
}

TEST(WalWriter, FsyncMinIntervalSpacesOutSecondFsync) {
  const auto root = temp_dir("wal_fsync_min_interval");
  std::filesystem::remove_all(root);
  std::filesystem::create_directories(root / "wal");
  structdb::storage::WalWriter w;
  structdb::infra::IoBackendConfig io{};
  ASSERT_TRUE(w.open(root / "wal", io));
  constexpr std::uint32_t kGapMs = 40;
  w.set_fsync_min_interval_ms(kGapMs);
  using clock = std::chrono::steady_clock;
  const auto t0 = clock::now();
  ASSERT_TRUE(w.append_record("a", 1, true));
  ASSERT_TRUE(w.append_record("b", 1, true));
  const auto elapsed_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(clock::now() - t0).count();
  EXPECT_GE(elapsed_ms, static_cast<std::int64_t>(kGapMs) - 15);
  w.close();
}

TEST(StorageEngine, WalFrameByteEstimatesSanity) {
  using structdb::storage::StorageEngine;
  EXPECT_GE(StorageEngine::estimate_put_wal_frame_bytes("k", "v"), 10u);
  std::vector<std::string> dels;
  std::vector<std::pair<std::string, std::string>> puts{{"a", "bc"}};
  EXPECT_GE(StorageEngine::estimate_commit_embed_batch_wal_frame_bytes(dels, puts), 64u);
}

TEST(Engine, SyncSchedulerWalBytesSoftPressureAddsNegativeWalDelta) {
  const auto dir = temp_dir("eng_wal_press");
  std::filesystem::remove_all(dir);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (dir / "_data").string();
  snap.storage_pressure_wal_bytes_soft_start = 200;
  snap.storage_pressure_wal_bytes_soft_step_bytes = 400;
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  eng.sync_scheduler_budget_from_storage_pressure();
  const std::int64_t d0 = eng.orchestrator()->scheduler().budget().wal_queue_depth_pressure_delta();
  std::string blob(900, 'y');
  ASSERT_TRUE(eng.kv_put("kblob", blob, false));
  eng.sync_scheduler_budget_from_storage_pressure();
  const std::int64_t d1 = eng.orchestrator()->scheduler().budget().wal_queue_depth_pressure_delta();
  EXPECT_LT(d1, d0);
  eng.shutdown();
}

TEST(Engine, WalAppendMaxBytesPerSecondThrottlesViaStorage) {
  const auto dir = temp_dir("eng_wal_bps");
  std::filesystem::remove_all(dir);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (dir / "_data").string();
  snap.wal_append_max_bytes_per_second = 8000;
  snap.wal_append_burst_bytes = 8192;
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  std::string payload(12000, 'z');
  ASSERT_TRUE(eng.kv_put("kpl1", payload, false));
  ASSERT_TRUE(eng.kv_put("kpl2", payload, false));
  structdb::storage::StoragePressureSnapshot p{};
  eng.storage_pressure_snapshot(&p);
  EXPECT_GT(p.wal_append_throttle_sleep_ns_total, 0u);
  eng.shutdown();
}

TEST(StorageEngine, StoragePressureCompactionMergeMatchesCounter) {
  const auto dir = temp_dir("st_pressure_compact_merge");
  std::filesystem::remove_all(dir);
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  st.set_l0_compact_trigger_threshold(2);
  st.set_l0_compact_max_rounds_per_flush(16);
  for (int i = 0; i < 5; ++i) {
    ASSERT_TRUE(st.put(std::string("pk") + std::to_string(i), "v", false));
    ASSERT_TRUE(st.flush_memtable(&err)) << err;
  }
  structdb::storage::StoragePressureSnapshot p{};
  st.read_storage_pressure_snapshot(&p);
  EXPECT_GT(p.compaction_merge_success_total, 0u);
  EXPECT_EQ(p.compaction_merge_success_total, st.compaction_merge_count());
  st.close();
}

TEST(StorageEngine, StoragePressureWorkerSubmitCompleteCounters) {
  const auto dir = temp_dir("st_pressure_worker_tasks");
  std::filesystem::remove_all(dir);
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  structdb::storage::StoragePressureSnapshot p0{};
  st.read_storage_pressure_snapshot(&p0);
  EXPECT_EQ(p0.compaction_worker_tasks_submitted_total, 0u);
  EXPECT_EQ(p0.compaction_worker_tasks_completed_total, 0u);

  st.start_compaction_worker(4);
  ASSERT_TRUE(st.enqueue_drain_l0_compaction_and_wait(1, &err, 0)) << err;
  structdb::storage::StoragePressureSnapshot p1{};
  st.read_storage_pressure_snapshot(&p1);
  EXPECT_GE(p1.compaction_worker_tasks_submitted_total, 1u);
  EXPECT_GE(p1.compaction_worker_tasks_completed_total, 1u);
  EXPECT_EQ(p1.compaction_worker_tasks_submitted_total, p1.compaction_worker_tasks_completed_total);
  st.stop_compaction_worker();
  st.close();
}

TEST(Engine, Phase14SyncBudgetUsesStoragePressure) {
  const auto dir = temp_dir("p14_pressure");
  std::filesystem::remove_all(dir);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (dir / "_data").string();
  snap.version = 1;
  snap.storage_pressure_l0_soft_start = 1;
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  for (int i = 0; i < 3; ++i) {
    const std::string key = std::string("pk") + std::to_string(i);
    ASSERT_TRUE(eng.storage()->put(key, "z", false));
    ASSERT_TRUE(eng.storage()->flush_memtable(&err)) << err;
  }
  eng.sync_scheduler_budget_from_storage_pressure();
  EXPECT_NE(eng.orchestrator()->scheduler().budget().wal_queue_depth_pressure_delta(), 0);
  eng.shutdown();
}

TEST(Engine, Phase21DeferredL0TightensCompactionSlotsBudget) {
  const auto dir = temp_dir("p21_defer_pressure");
  std::filesystem::remove_all(dir);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (dir / "_data").string();
  snap.version = 1;
  snap.storage_pressure_deferred_l0_slot_tighten = true;
  snap.l0_compact_trigger_threshold = 2;
  snap.l0_compact_defer_after_flush = true;
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  for (int i = 0; i < 3; ++i) {
    ASSERT_TRUE(eng.kv_put(std::string("p21k") + std::to_string(i), "v", false));
    ASSERT_TRUE(eng.storage()->flush_memtable(&err)) << err;
  }
  structdb::storage::StoragePressureSnapshot p{};
  eng.storage_pressure_snapshot(&p);
  EXPECT_TRUE(p.pending_deferred_l0_compact);
  eng.sync_scheduler_budget_from_storage_pressure();
  EXPECT_EQ(eng.orchestrator()->scheduler().budget().compaction_slots_pressure_delta(), -1);
  ASSERT_TRUE(eng.drain_l0_compaction_queue(16, &err)) << err;
  eng.sync_scheduler_budget_from_storage_pressure();
  EXPECT_EQ(eng.orchestrator()->scheduler().budget().compaction_slots_pressure_delta(), 0);
  eng.shutdown();
}

TEST(Engine, Phase19DeferPlanIncludesDrainAndReplanReducesL0) {
  const auto dir = temp_dir("p19_repipe");
  std::filesystem::remove_all(dir);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (dir / "_data").string();
  snap.version = 1;
  snap.l0_compact_trigger_threshold = 2;
  snap.l0_compact_max_rounds_per_flush = 16;
  snap.l0_compact_defer_after_flush = true;
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  for (int i = 0; i < 5; ++i) {
    const std::string key = std::string("z") + std::to_string(i);
    ASSERT_TRUE(eng.storage()->put(key, "v", false));
    ASSERT_TRUE(eng.storage()->flush_memtable(&err)) << err;
  }
  EXPECT_EQ(eng.storage()->manifest().sst_files().size(), 5u);
  ASSERT_TRUE(eng.rerun_default_pipeline(&err)) << err;
  EXPECT_LE(eng.storage()->manifest().sst_files().size(), 2u);
  eng.shutdown();
}

TEST(CheckpointState, UndoPrefixBytesDefaultZero) {
  structdb::storage::CheckpointState st{};
  EXPECT_EQ(st.undo_log_safe_prefix_bytes, 0u);
}

TEST(StorageEngine, Phase43CheckpointChainAppendOnRotate) {
  const auto dir = temp_dir("p43_chain");
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  structdb::storage::CheckpointWriter w;
  structdb::storage::CheckpointState st{};
  st.wal_offset = 10;
  st.manifest_version = 1;
  ASSERT_TRUE(w.write_rotating(dir, st, nullptr));
  st.wal_offset = 20;
  ASSERT_TRUE(w.write_rotating(dir, st, nullptr));
  std::vector<structdb::storage::CheckpointChainEntry> entries;
  ASSERT_TRUE(structdb::storage::checkpoint_chain_read_all(dir, &entries, nullptr));
  ASSERT_GE(entries.size(), 2u);
  EXPECT_EQ(entries.back().wal_offset, 20u);
  ASSERT_TRUE(structdb::storage::checkpoint_chain_validate(dir, false, nullptr));
}

TEST(StorageEngine, Phase43ChainValidateMismatchStrict) {
  const auto dir = temp_dir("p43_chain_strict");
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  structdb::storage::CheckpointWriter w;
  structdb::storage::CheckpointState st{};
  st.checkpoint_seq = 1;
  st.wal_offset = 8;
  ASSERT_TRUE(w.write_rotating(dir, st, nullptr));
  std::ofstream chain(dir / "checkpoint.chain", std::ios::trunc);
  chain << "99 999 0 1 0 0 1\n";
  chain.close();
  std::string err;
  EXPECT_FALSE(structdb::storage::checkpoint_chain_validate(dir, true, &err));
  EXPECT_FALSE(err.empty());
}

TEST(StorageEngine, Phase10FlushPersistsUndoSafePrefixInCheckpointV2) {
  const auto dir = temp_dir("p10_ckpt_undo");
  std::filesystem::remove_all(dir);
  namespace mk = structdb::storage::mdb_keyspace;
  const std::string rk = mk::row_key("p10_u", "1");
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  ASSERT_TRUE(st.put(rk, "a", false));
  ASSERT_TRUE(st.put(rk, "b", false));
  const std::uint64_t undo_sz = st.undo_log_bytes_on_disk();
  ASSERT_GT(undo_sz, 0u);
  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  structdb::storage::CheckpointState ck{};
  ASSERT_TRUE(st.read_checkpoint_state(&ck));
  EXPECT_EQ(ck.undo_log_safe_prefix_bytes, undo_sz);
  const auto slot_a = dir / "d" / "checkpoint.a";
  const auto slot_b = dir / "d" / "checkpoint.b";
  std::uintmax_t bin_sz = 0;
  if (std::filesystem::exists(slot_a)) bin_sz = std::filesystem::file_size(slot_a);
  if (bin_sz == 0 && std::filesystem::exists(slot_b)) bin_sz = std::filesystem::file_size(slot_b);
  ASSERT_EQ(bin_sz, 68u);
  st.close();
}

TEST(StorageEngine, Phase10UndoTryTruncateRecyclablePrefixAfterFlush) {
  const auto dir = temp_dir("p10_trunc_pref");
  std::filesystem::remove_all(dir);
  namespace mk = structdb::storage::mdb_keyspace;
  const std::string rk = mk::row_key("p10_tr", "1");
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  ASSERT_TRUE(st.put(rk, "x", false));
  ASSERT_TRUE(st.put(rk, "y", false));
  ASSERT_GT(st.undo_log_bytes_on_disk(), 0u);
  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  ASSERT_TRUE(st.undo_try_truncate_recyclable_prefix(&err)) << err;
  EXPECT_EQ(st.undo_log_bytes_on_disk(), 0u);
  std::string v;
  ASSERT_TRUE(st.get(rk, &v));
  EXPECT_EQ(v, "y");
  st.close();
}

TEST(StorageEngine, Phase10DoubleRollbackThenPrefixTruncateEmptiesUndoLog) {
  const auto dir = temp_dir("p10_rb2_trunc");
  std::filesystem::remove_all(dir);
  namespace mk = structdb::storage::mdb_keyspace;
  const std::string rk = mk::row_key("p10_rb2", "1");
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  ASSERT_TRUE(st.put(rk, "a", false));
  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  ASSERT_TRUE(st.put(rk, "b", false));
  ASSERT_TRUE(st.put(rk, "c", false));
  ASSERT_GT(st.undo_log_bytes_on_disk(), 0u);
  ASSERT_TRUE(st.rollback_one_undo_frame(&err)) << err;
  ASSERT_TRUE(st.rollback_one_undo_frame(&err)) << err;
  ASSERT_TRUE(st.undo_try_truncate_recyclable_prefix(&err)) << err;
  EXPECT_EQ(st.undo_log_bytes_on_disk(), 0u);
  std::string v;
  ASSERT_TRUE(st.get(rk, &v));
  EXPECT_EQ(v, "a");
  st.close();
}

TEST(StorageEngine, Phase11NoAutoCompactWhenThresholdZero) {
  const auto dir = temp_dir("p11_off");
  std::filesystem::remove_all(dir);
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  st.set_l0_compact_trigger_threshold(0);
  for (int i = 0; i < 5; ++i) {
    const std::string key = std::string("k") + std::to_string(i);
    ASSERT_TRUE(st.put(key, "v", false));
    ASSERT_TRUE(st.flush_memtable(&err)) << err;
  }
  EXPECT_EQ(st.manifest().sst_files().size(), 5u);
  EXPECT_EQ(st.compaction_merge_count(), 0u);
  st.close();
}

TEST(StorageEngine, Phase11AutoL0CompactAfterFlushReducesSstCount) {
  const auto dir = temp_dir("p11_auto");
  std::filesystem::remove_all(dir);
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  st.set_l0_compact_trigger_threshold(2);
  st.set_l0_compact_max_rounds_per_flush(16);
  for (int i = 0; i < 5; ++i) {
    const std::string key = std::string("k") + std::to_string(i);
    ASSERT_TRUE(st.put(key, "v", false));
    ASSERT_TRUE(st.flush_memtable(&err)) << err;
  }
  EXPECT_LE(st.manifest().sst_files().size(), 2u);
  EXPECT_GT(st.compaction_merge_count(), 0u);
  for (int i = 0; i < 5; ++i) {
    const std::string key = std::string("k") + std::to_string(i);
    std::string v;
    ASSERT_TRUE(st.get(key, &v));
    EXPECT_EQ(v, "v");
  }
  st.close();
}

TEST(StorageEngine, CompactionMergeMinIntervalAddsThrottleSleepToSnapshot) {
  const auto dir = temp_dir("p_merge_min_iv");
  std::filesystem::remove_all(dir);
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  st.set_l0_compact_trigger_threshold(0);
  for (int i = 0; i < 10; ++i) {
    const std::string key = std::string("k") + std::to_string(i);
    ASSERT_TRUE(st.put(key, "v", false));
    ASSERT_TRUE(st.flush_memtable(&err)) << err;
  }
  EXPECT_EQ(st.manifest().sst_files().size(), 10u);
  st.set_compaction_merge_min_interval_ms(12);
  st.set_l0_compact_trigger_threshold(1);
  st.set_l0_compact_max_rounds_per_flush(32);
  ASSERT_TRUE(st.put("kz", "vz", false));
  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  structdb::storage::StoragePressureSnapshot p{};
  st.read_storage_pressure_snapshot(&p);
  EXPECT_GE(p.compaction_merge_throttle_sleep_ns_total, 50'000'000ull)
      << "multiple L0 merges in one flush should sleep between rounds when compaction_merge_min_interval_ms>0";
  EXPECT_GT(st.compaction_merge_count(), 0u);
  st.close();
}

TEST(StorageEngine, CompactionMergeByteThrottleAddsSleepToSnapshot) {
  const auto dir = temp_dir("p_merge_byte_tb");
  std::filesystem::remove_all(dir);
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  st.set_l0_compact_trigger_threshold(0);
  for (int i = 0; i < 7; ++i) {
    const std::string key = std::string("bk") + std::to_string(i);
    ASSERT_TRUE(st.put(key, std::string(900, 'y'), false));
    ASSERT_TRUE(st.flush_memtable(&err)) << err;
  }
  EXPECT_EQ(st.manifest().sst_files().size(), 7u);
  st.set_compaction_merge_max_bytes_per_second(120'000);
  st.set_compaction_merge_burst_bytes(64 * 1024);
  st.set_l0_compact_trigger_threshold(1);
  st.set_l0_compact_max_rounds_per_flush(16);
  ASSERT_TRUE(st.put("bkz", "z", false));
  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  structdb::storage::StoragePressureSnapshot p{};
  st.read_storage_pressure_snapshot(&p);
  EXPECT_GE(p.compaction_merge_byte_throttle_sleep_ns_total, 1'000'000ull)
      << "low compaction_merge_max_bytes_per_second should sleep during merge materialize I/O";
  EXPECT_GT(st.compaction_merge_count(), 0u);
  st.close();
}

TEST(StorageEngine, CompactionDedicatedIoExecutorAndChunkedByteThrottle) {
  const auto dir = temp_dir("p_merge_ded_io_chunk");
  std::filesystem::remove_all(dir);
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  st.set_l0_compact_trigger_threshold(0);
  for (int i = 0; i < 7; ++i) {
    const std::string key = std::string("dk") + std::to_string(i);
    ASSERT_TRUE(st.put(key, std::string(900, 'y'), false));
    ASSERT_TRUE(st.flush_memtable(&err)) << err;
  }
  EXPECT_EQ(st.manifest().sst_files().size(), 7u);
  st.set_compaction_dedicated_io_executor(true);
  st.set_compaction_io_chunk_bytes(32 * 1024);
  st.set_compaction_merge_max_bytes_per_second(120'000);
  st.set_compaction_merge_burst_bytes(64 * 1024);
  st.set_l0_compact_trigger_threshold(1);
  st.set_l0_compact_max_rounds_per_flush(16);
  ASSERT_TRUE(st.put("dkz", "z", false));
  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  structdb::storage::StoragePressureSnapshot p{};
  st.read_storage_pressure_snapshot(&p);
  EXPECT_GE(p.compaction_merge_byte_throttle_sleep_ns_total, 1'000'000ull)
      << "dedicated I/O + chunked merge should still accumulate merge byte throttle sleep";
  EXPECT_GT(st.compaction_merge_count(), 0u);
  st.close();
}

TEST(StorageEngine, CompactionDedicatedIoExecutorNoByteThrottleL0Merge) {
  const auto dir = temp_dir("p_ded_io_no_bps");
  std::filesystem::remove_all(dir);
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  st.set_l0_compact_trigger_threshold(0);
  const std::string payload(600, 'q');
  for (int i = 0; i < 6; ++i) {
    ASSERT_TRUE(st.put(std::string("nq") + std::to_string(i), payload, false));
    ASSERT_TRUE(st.flush_memtable(&err)) << err;
  }
  EXPECT_EQ(st.manifest().sst_files().size(), 6u);
  st.set_compaction_dedicated_io_executor(true);
  st.set_compaction_io_chunk_bytes(12 * 1024);
  st.set_l0_compact_trigger_threshold(1);
  st.set_l0_compact_max_rounds_per_flush(24);
  ASSERT_TRUE(st.put("nqz", "z", false));
  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  structdb::storage::StoragePressureSnapshot p{};
  st.read_storage_pressure_snapshot(&p);
  EXPECT_EQ(p.compaction_merge_byte_throttle_sleep_ns_total, 0u)
      << "merge byte throttle should stay zero when compaction_merge_max_bytes_per_second is off";
  EXPECT_GT(st.compaction_merge_count(), 0u);
  for (int i = 0; i < 6; ++i) {
    std::string v;
    ASSERT_TRUE(st.get(std::string("nq") + std::to_string(i), &v));
    EXPECT_EQ(v, payload);
  }
  st.close();
}

TEST(StorageEngine, CompactionExplicitIoChunkByteThrottleWithoutDedicatedIo) {
  const auto dir = temp_dir("p_chunk_bps_no_ded");
  std::filesystem::remove_all(dir);
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  st.set_l0_compact_trigger_threshold(0);
  st.set_compaction_dedicated_io_executor(false);
  st.set_compaction_io_chunk_bytes(6 * 1024);
  st.set_compaction_merge_max_bytes_per_second(100'000);
  st.set_compaction_merge_burst_bytes(48 * 1024);
  const std::string payload(700, 't');
  for (int i = 0; i < 6; ++i) {
    ASSERT_TRUE(st.put(std::string("tq") + std::to_string(i), payload, false));
    ASSERT_TRUE(st.flush_memtable(&err)) << err;
  }
  EXPECT_EQ(st.manifest().sst_files().size(), 6u);
  st.set_l0_compact_trigger_threshold(1);
  st.set_l0_compact_max_rounds_per_flush(24);
  ASSERT_TRUE(st.put("tqz", "z", false));
  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  structdb::storage::StoragePressureSnapshot p{};
  st.read_storage_pressure_snapshot(&p);
  EXPECT_GE(p.compaction_merge_byte_throttle_sleep_ns_total, 500'000ull)
      << "explicit compaction_io_chunk_bytes with merge bps should throttle per chunk without dedicated executor";
  EXPECT_GT(st.compaction_merge_count(), 0u);
  st.close();
}

TEST(Engine, DedicatedIoAndChunkFromConfigRunsL0Compact) {
  const auto dir = temp_dir("eng_ded_io_chunk_cfg");
  std::filesystem::remove_all(dir);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (dir / "_data").string();
  snap.version = 1;
  snap.l0_compact_trigger_threshold = 2;
  snap.l0_compact_max_rounds_per_flush = 16;
  snap.compaction_dedicated_io_executor = true;
  snap.compaction_io_chunk_bytes = 10 * 1024;
  snap.compaction_merge_max_bytes_per_second = 130'000;
  snap.compaction_merge_burst_bytes = 64 * 1024;
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  ASSERT_NE(eng.storage(), nullptr);
  const std::string blob(500, 'u');
  for (int i = 0; i < 5; ++i) {
    ASSERT_TRUE(eng.storage()->put(std::string("cfgk") + std::to_string(i), blob, false));
    ASSERT_TRUE(eng.storage()->flush_memtable(&err)) << err;
  }
  EXPECT_LE(eng.storage()->manifest().sst_files().size(), 2u);
  EXPECT_GT(eng.storage()->compaction_merge_count(), 0u);
  structdb::storage::StoragePressureSnapshot p{};
  eng.storage_pressure_snapshot(&p);
  EXPECT_GE(p.compaction_merge_byte_throttle_sleep_ns_total, 500'000ull);
  for (int i = 0; i < 5; ++i) {
    std::string v;
    ASSERT_TRUE(eng.storage()->get(std::string("cfgk") + std::to_string(i), &v));
    EXPECT_EQ(v, blob);
  }
  eng.shutdown();
}

TEST(StorageEngine, CompactionDeferredWorkerDrainUsesDedicatedIoMaterialize) {
  const auto dir = temp_dir("p_defer_worker_dedio");
  std::filesystem::remove_all(dir);
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  st.set_l0_compact_defer_after_flush(true);
  st.set_l0_compact_trigger_threshold(2);
  st.set_l0_compact_max_rounds_per_flush(32);
  st.set_compaction_dedicated_io_executor(true);
  st.set_compaction_io_chunk_bytes(8 * 1024);
  st.set_compaction_merge_max_bytes_per_second(90'000);
  st.set_compaction_merge_burst_bytes(64 * 1024);
  const std::string payload(800, 'v');
  for (int i = 0; i < 4; ++i) {
    ASSERT_TRUE(st.put(std::string("wk") + std::to_string(i), payload, false));
    ASSERT_TRUE(st.flush_memtable(&err)) << err;
  }
  EXPECT_EQ(st.manifest().l0_prefix_length(), 4u);

  st.start_compaction_worker(8);
  ASSERT_TRUE(st.enqueue_drain_l0_compaction_and_wait(32, &err, 0)) << err;
  st.stop_compaction_worker();
  EXPECT_LE(st.manifest().l0_prefix_length(), 2u);
  EXPECT_GE(st.compaction_merge_count(), 2u);
  structdb::storage::StoragePressureSnapshot p{};
  st.read_storage_pressure_snapshot(&p);
  EXPECT_GE(p.compaction_merge_byte_throttle_sleep_ns_total, 300'000ull);
  for (int i = 0; i < 4; ++i) {
    std::string v;
    ASSERT_TRUE(st.get(std::string("wk") + std::to_string(i), &v));
    EXPECT_EQ(v, payload);
  }
  st.close();
}

TEST(StorageEngine, CompactionTieredL1ToL2DedicatedIoByteThrottle) {
  const auto dir = temp_dir("p_tier_l1l2_dedio_bps");
  std::filesystem::remove_all(dir);
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  const std::string blob(2200, 'w');
  st.set_l1_compact_output_from_l0_merge(true);
  st.set_l2_compact_output_from_l1_merge(true);
  ASSERT_TRUE(st.put("tierk", blob, false));
  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  ASSERT_TRUE(st.put("tierk", blob + "b", false));
  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  ASSERT_TRUE(st.compact_merge_two_oldest_l0(&err)) << err;
  ASSERT_TRUE(st.put("tierk2", blob, false));
  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  ASSERT_TRUE(st.put("tierk2", blob + "c", false));
  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  ASSERT_TRUE(st.compact_merge_two_oldest_l0(&err)) << err;
  const auto& ent = st.manifest().sst_entries();
  ASSERT_EQ(ent.size(), 2u);
  EXPECT_EQ(ent[0].level, 1u);
  EXPECT_EQ(ent[1].level, 1u);

  st.set_compaction_dedicated_io_executor(true);
  st.set_compaction_io_chunk_bytes(16 * 1024);
  st.set_compaction_merge_max_bytes_per_second(45'000);
  st.set_compaction_merge_burst_bytes(64 * 1024);
  structdb::storage::StoragePressureSnapshot p0{};
  st.read_storage_pressure_snapshot(&p0);
  ASSERT_TRUE(st.compact_merge_two_oldest_l1_to_l2(&err)) << err;
  structdb::storage::StoragePressureSnapshot p1{};
  st.read_storage_pressure_snapshot(&p1);
  EXPECT_GE(p1.compaction_merge_byte_throttle_sleep_ns_total - p0.compaction_merge_byte_throttle_sleep_ns_total,
            200'000ull)
      << "L1->L2 materialize should consume merge byte tokens when bps is low";
  ASSERT_EQ(st.manifest().sst_entries().size(), 1u);
  EXPECT_EQ(st.manifest().sst_entries()[0].level, 2u);
  std::string v;
  ASSERT_TRUE(st.get("tierk", &v));
  EXPECT_EQ(v, blob + "b");
  ASSERT_TRUE(st.get("tierk2", &v));
  EXPECT_EQ(v, blob + "c");
  st.close();
}

TEST(Engine, L0AutoCompactAfterFlushFromConfig) {
  const auto dir = temp_dir("l0_auto_cfg");
  std::filesystem::remove_all(dir);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (dir / "_data").string();
  snap.version = 1;
  snap.l0_compact_trigger_threshold = 2;
  snap.l0_compact_max_rounds_per_flush = 16;
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  ASSERT_NE(eng.storage(), nullptr);
  for (int i = 0; i < 5; ++i) {
    const std::string key = std::string("fk") + std::to_string(i);
    ASSERT_TRUE(eng.storage()->put(key, "x", false));
    ASSERT_TRUE(eng.storage()->flush_memtable(&err)) << err;
  }
  EXPECT_LE(eng.storage()->manifest().sst_files().size(), 2u);
  eng.shutdown();
}

TEST(Manifest, Phase12LegacyLoadAndSaveFormat2) {
  const auto dir = temp_dir("p12_mnf");
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  const auto p = dir / "MANIFEST";
  {
    std::ofstream out(p);
    out << "3\n2\nalpha.sst\nbeta.sst\n";
  }
  structdb::storage::Manifest m;
  ASSERT_TRUE(m.load(p));
  EXPECT_EQ(m.version(), 3u);
  ASSERT_EQ(m.sst_entries().size(), 2u);
  EXPECT_EQ(m.sst_entries()[0].level, 0u);
  EXPECT_EQ(m.sst_entries()[0].relative_path, "alpha.sst");
  ASSERT_TRUE(m.save(p));
  structdb::storage::Manifest m2;
  ASSERT_TRUE(m2.load(p));
  ASSERT_EQ(m2.sst_entries().size(), 2u);
  EXPECT_EQ(m2.sst_entries()[1].relative_path, "beta.sst");
}

TEST(StorageEngine, Phase12ManifestWrittenAsFormat2) {
  const auto dir = temp_dir("p12_fmt2");
  std::filesystem::remove_all(dir);
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  ASSERT_TRUE(st.put("x", "y", false));
  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  std::ifstream in(dir / "d" / "MANIFEST");
  std::string line1, line2;
  ASSERT_TRUE(static_cast<bool>(std::getline(in, line1)));
  ASSERT_TRUE(static_cast<bool>(std::getline(in, line2)));
  EXPECT_EQ(line2, "FORMAT2");
  st.close();
}

TEST(StorageEngine, Phase12L1OutputCompactThenRead) {
  const auto dir = temp_dir("p12_l1");
  std::filesystem::remove_all(dir);
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  st.set_l1_compact_output_from_l0_merge(true);
  ASSERT_TRUE(st.put("k", "a", false));
  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  ASSERT_TRUE(st.put("k", "b", false));
  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  ASSERT_TRUE(st.compact_merge_two_oldest_l0(&err)) << err;
  const auto& ent = st.manifest().sst_entries();
  ASSERT_EQ(ent.size(), 1u);
  EXPECT_EQ(ent[0].level, 1u);
  EXPECT_EQ(ent[0].relative_path.rfind("L1-", 0), 0u);
  std::string v;
  ASSERT_TRUE(st.get("k", &v));
  EXPECT_EQ(v, "b");
  ASSERT_TRUE(st.put("k", "c", false));
  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  ASSERT_TRUE(st.get("k", &v));
  EXPECT_EQ(v, "c");
  ASSERT_EQ(st.manifest().sst_entries().size(), 2u);
  st.close();
}

TEST(Engine, L1CompactOutputFromConfig) {
  const auto dir = temp_dir("l1_cfg");
  std::filesystem::remove_all(dir);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (dir / "_data").string();
  snap.version = 1;
  snap.l1_compact_output_from_l0_merge = true;
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  ASSERT_NE(eng.storage(), nullptr);
  ASSERT_TRUE(eng.storage()->put("p", "q", false));
  ASSERT_TRUE(eng.storage()->flush_memtable(&err)) << err;
  ASSERT_TRUE(eng.storage()->put("p", "r", false));
  ASSERT_TRUE(eng.storage()->flush_memtable(&err)) << err;
  ASSERT_TRUE(eng.storage()->compact_merge_two_oldest_l0(&err)) << err;
  EXPECT_EQ(eng.storage()->manifest().sst_entries().size(), 1u);
  EXPECT_EQ(eng.storage()->manifest().sst_entries()[0].level, 1u);
  std::string v;
  ASSERT_TRUE(eng.storage()->get("p", &v));
  EXPECT_EQ(v, "r");
  eng.shutdown();
}

TEST(StorageEngine, CompactionRejectsLessThanTwoSsts) {
  const auto dir = temp_dir("compact_need2");
  std::filesystem::remove_all(dir);
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  ASSERT_TRUE(st.put("only", "1", false));
  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  EXPECT_FALSE(st.compact_merge_two_oldest_l0(&err));
  EXPECT_EQ(err, "compact: need at least two L0 SSTs");
  st.close();
}

TEST(StorageEngine, CompactionMergeTwoOldestSurvivesRestart) {
  const auto dir = temp_dir("compact_restart");
  std::filesystem::remove_all(dir);
  namespace mk = structdb::storage::mdb_keyspace;
  const std::string k1 = mk::row_key("cmp", "1");
  const std::string k2 = mk::row_key("cmp", "2");
  {
    structdb::storage::StorageEngine st(dir / "d");
    std::string err;
    ASSERT_TRUE(st.open(&err)) << err;
    ASSERT_TRUE(st.put(k1, "a", false));
    ASSERT_TRUE(st.flush_memtable(&err)) << err;
    ASSERT_TRUE(st.put(k2, "b", false));
    ASSERT_TRUE(st.flush_memtable(&err)) << err;
    ASSERT_EQ(st.compaction_merge_count(), 0u);
    ASSERT_TRUE(st.compact_merge_two_oldest_l0(&err)) << err;
    EXPECT_EQ(st.compaction_merge_count(), 1u);
    ASSERT_EQ(st.manifest().sst_files().size(), 1u);
    std::string v1, v2;
    ASSERT_TRUE(st.get(k1, &v1));
    ASSERT_TRUE(st.get(k2, &v2));
    EXPECT_EQ(v1, "a");
    EXPECT_EQ(v2, "b");
    st.close();
  }
  {
    structdb::storage::StorageEngine st2(dir / "d");
    std::string err;
    ASSERT_TRUE(st2.open(&err)) << err;
    std::string v1, v2;
    ASSERT_TRUE(st2.get(k1, &v1));
    ASSERT_TRUE(st2.get(k2, &v2));
    EXPECT_EQ(v1, "a");
    EXPECT_EQ(v2, "b");
    EXPECT_EQ(st2.compaction_merge_count(), 0u);
    st2.close();
  }
}

TEST(StorageEngine, CompactionNewerL0WinsDuplicateKey) {
  const auto dir = temp_dir("compact_dup");
  std::filesystem::remove_all(dir);
  namespace mk = structdb::storage::mdb_keyspace;
  const std::string rk = mk::row_key("dup", "1");
  {
    structdb::storage::StorageEngine st(dir / "d");
    std::string err;
    ASSERT_TRUE(st.open(&err)) << err;
    ASSERT_TRUE(st.put(rk, "first", false));
    ASSERT_TRUE(st.flush_memtable(&err)) << err;
    ASSERT_TRUE(st.put(rk, "second", false));
    ASSERT_TRUE(st.flush_memtable(&err)) << err;
    ASSERT_TRUE(st.compact_merge_two_oldest_l0(&err)) << err;
    std::string v;
    ASSERT_TRUE(st.get(rk, &v));
    EXPECT_EQ(v, "second");
    st.close();
  }
}

TEST(Engine, UndoAutoTruncateAfterFlushFromConfig) {
  const auto dir = temp_dir("undo_auto_trunc_cfg");
  std::filesystem::remove_all(dir);
  namespace mk = structdb::storage::mdb_keyspace;
  const std::string rk = mk::row_key("uat_cfg", "1");
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (dir / "_data").string();
  snap.version = 1;
  snap.undo_auto_truncate_after_flush = true;
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  ASSERT_TRUE(eng.storage()->put(rk, "a", false));
  ASSERT_TRUE(eng.storage()->put(rk, "b", false));
  EXPECT_GT(eng.storage()->undo_log_bytes_on_disk(), 0u);
  ASSERT_TRUE(eng.storage()->flush_memtable(&err)) << err;
  EXPECT_EQ(eng.storage()->undo_log_bytes_on_disk(), 0u);
  std::string v;
  ASSERT_TRUE(eng.storage()->get(rk, &v));
  EXPECT_EQ(v, "b");
  eng.shutdown();
}

TEST(Engine, CompactionWorkerDrainsDeferredL0) {
  const auto dir = temp_dir("eng_compact_worker");
  std::filesystem::remove_all(dir);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (dir / "_data").string();
  snap.version = 1;
  snap.l0_compact_trigger_threshold = 2;
  snap.l0_compact_max_rounds_per_flush = 16;
  snap.l0_compact_defer_after_flush = true;
  snap.enable_compaction_worker = true;
  snap.compaction_worker_queue_depth = 8;
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  ASSERT_TRUE(eng.storage()->compaction_worker_started());
  for (int i = 0; i < 5; ++i) {
    const std::string key = std::string("wk") + std::to_string(i);
    ASSERT_TRUE(eng.storage()->put(key, "v", false));
    ASSERT_TRUE(eng.storage()->flush_memtable(&err)) << err;
  }
  EXPECT_EQ(eng.storage()->manifest().sst_files().size(), 5u);
  ASSERT_TRUE(eng.drain_l0_compaction_queue(16, &err)) << err;
  EXPECT_LE(eng.storage()->manifest().sst_files().size(), 2u);
  eng.shutdown();
}

TEST(Scheduler, PlanEpochRegressionRejected) {
  auto budget = std::make_shared<structdb::scheduler::ResourceBudget>();
  structdb::scheduler::ExecutionScheduler sched(budget);
  auto p5 = structdb::planner::ExecutionPlan::make_linear(5, std::vector<std::string>{"noop"});
  p5.plan_epoch = 5;
  std::string err;
  ASSERT_TRUE(sched.set_active_plan(std::move(p5), &err)) << err;
  auto p3 = structdb::planner::ExecutionPlan::make_linear(3, std::vector<std::string>{"noop"});
  p3.plan_epoch = 3;
  ASSERT_FALSE(sched.set_active_plan(std::move(p3), &err));
}

TEST(GraphExecutor, CooperativeCancel) {
  struct SlowCancelOp final : public structdb::runtime::IOperator {
    std::string_view name() const override { return "slow_cancel"; }
    bool execute(structdb::runtime::OperatorContext& ctx) override {
      if (!ctx.cancel_requested) return true;
      while (!ctx.cancel_requested->load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::microseconds(20));
      }
      return true;
    }
  };

  auto budget = std::make_shared<structdb::scheduler::ResourceBudget>();
  structdb::scheduler::ExecutionScheduler sched(budget);
  structdb::runtime::GraphExecutor ex;
  ex.register_operator("slow_cancel", std::make_shared<SlowCancelOp>());

  std::string err2;
  std::thread worker([&] {
    structdb::planner::ExecutionPlan plan =
        structdb::planner::ExecutionPlan::make_linear(9, std::vector<std::string>{"slow_cancel"});
    plan.plan_epoch = 1;
    ex.execute(std::move(plan), sched, false, &err2);
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  ex.request_cancel();
  worker.join();
  ASSERT_EQ(err2, "cancelled");
}

TEST(Orchestrator, ReplanBumpsEpoch) {
  auto budget = std::make_shared<structdb::scheduler::ResourceBudget>();
  auto sched = std::make_shared<structdb::scheduler::ExecutionScheduler>(budget);
  auto ex = std::make_shared<structdb::runtime::GraphExecutor>();
  struct Noop final : public structdb::runtime::IOperator {
    std::string_view name() const override { return "noop"; }
    bool execute(structdb::runtime::OperatorContext&) override { return true; }
  };
  ex->register_operator("noop", std::make_shared<Noop>());
  structdb::orchestrator::Orchestrator orch(sched, ex, [](std::uint64_t ep) {
    return structdb::planner::ExecutionPlan::make_linear(ep, std::vector<std::string>{"noop"});
  });
  std::string e;
  ASSERT_TRUE(orch.run_default(&e)) << e;
  ASSERT_EQ(sched->active_epoch(), 1u);
  ASSERT_TRUE(orch.replan_and_run(&e)) << e;
  ASSERT_EQ(sched->active_epoch(), 2u);
}

TEST(StorageEngine, GetSeesValueAfterFlush) {
  const auto dir = temp_dir("lsm_get_flush");
  std::filesystem::remove_all(dir);
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  ASSERT_TRUE(st.put("kflush", "v1", false));
  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  std::string v;
  ASSERT_TRUE(st.get("kflush", &v));
  EXPECT_EQ(v, "v1");
  st.close();
}

TEST(StorageEngine, LsmStateAfterFlush) {
  const auto dir = temp_dir("lsm_flush");
  std::filesystem::remove_all(dir);
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  ASSERT_TRUE(st.put("x", "y", false));
  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  ASSERT_FALSE(st.lsm_state().l0_sst_relative_paths.empty());
  ASSERT_EQ(st.lsm_state().last_manifest_version, st.manifest().version());
  st.close();
}

TEST(StorageEngine, FlushWritesSstV3MagicAndBloomFooter) {
  const auto dir = temp_dir("sst_v3_bloom_footer");
  std::filesystem::remove_all(dir);
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  ASSERT_TRUE(st.put("ka", "1", false));
  ASSERT_TRUE(st.put("kb", "2", false));
  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  ASSERT_FALSE(st.lsm_state().l0_sst_relative_paths.empty());
  const auto sst_path = (dir / "d") / st.lsm_state().l0_sst_relative_paths.front();
  std::ifstream in(sst_path, std::ios::binary);
  ASSERT_TRUE(in) << sst_path.string();
  std::vector<char> buf((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
  ASSERT_GE(buf.size(), 8u + 68u);
  EXPECT_EQ(std::string_view(buf.data(), 8), "STDBSST3");
  const std::size_t n = buf.size();
  // File ends with: u32 LE bloom_len (64) + 64-byte bloom bitmap.
  EXPECT_EQ(static_cast<unsigned char>(buf[n - 68]), 64u);
  EXPECT_EQ(static_cast<unsigned char>(buf[n - 67]), 0u);
  EXPECT_EQ(static_cast<unsigned char>(buf[n - 66]), 0u);
  EXPECT_EQ(static_cast<unsigned char>(buf[n - 65]), 0u);
  std::string miss;
  ASSERT_FALSE(st.get("kz_not_in_sst", &miss));
  st.close();
}

TEST(EmbedClient, IdempotencyTokenSkipsReplay) {
  const auto root = temp_dir("idem");
  std::filesystem::remove_all(root);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient c(eng);
  ASSERT_TRUE(c.open(root / "sess", &err)) << err;
  structdb::client::CommandBatch b;
  b.idempotency_token = "once";
  b.puts.push_back({"k", "first"});
  ASSERT_TRUE(c.submit(b, false, &err)) << err;
  b.puts[0].second = "second";
  ASSERT_TRUE(c.submit(b, false, &err)) << err;
  std::string v;
  ASSERT_TRUE(eng.storage()->get("k", &v));
  ASSERT_EQ(v, "first");
  eng.shutdown();
}

TEST(EmbedClient, SessionLogOpenCloseAndRotation) {
  const auto root = temp_dir("sess_log");
  std::filesystem::remove_all(root);
  const auto sess = root / "sdir";
  std::filesystem::create_directories(sess);

  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;

  structdb::client::EmbedClient c(eng);
  ASSERT_TRUE(c.open(sess, &err)) << err;
  const auto logp = c.embed_session_log_path();
  ASSERT_TRUE(std::filesystem::exists(logp));
  auto read_all = [](const std::filesystem::path& p) {
    std::ifstream in(p, std::ios::binary);
    std::ostringstream o;
    o << in.rdbuf();
    return o.str();
  };
  auto count_sub = [](const std::string& s, const std::string& n) {
    std::size_t c = 0;
    for (std::size_t p = 0; p < s.size();) {
      const auto f = s.find(n, p);
      if (f == std::string::npos) break;
      ++c;
      p = f + n.size();
    }
    return c;
  };

  const std::string log1 = read_all(logp);
  EXPECT_NE(log1.find("SESSION_OPEN"), std::string::npos) << log1;
  c.close();

  const std::string log2 = read_all(logp);
  EXPECT_NE(log2.find("SESSION_CLOSE"), std::string::npos) << log2;

  structdb::client::EmbedClient c2(eng);
  ASSERT_TRUE(c2.open(sess, &err)) << err;
  const std::string log3 = read_all(logp);
  EXPECT_GE(count_sub(log3, "SESSION_OPEN"), 2u) << log3;

  {
    std::ofstream junk(logp, std::ios::binary | std::ios::trunc);
    const std::string chunk(65536, 'x');
    for (int i = 0; i < 34; ++i) junk.write(chunk.data(), static_cast<std::streamsize>(chunk.size()));
    ASSERT_TRUE(junk) << "prefill session_log";
  }

  structdb::client::EmbedClient c3(eng);
  ASSERT_TRUE(c3.open(sess, &err)) << err;
  bool saw_arch = false;
  for (const auto& ent : std::filesystem::directory_iterator(logp.parent_path())) {
    if (ent.path().filename().string().rfind("session_log.arch.", 0) == 0) saw_arch = true;
  }
  EXPECT_TRUE(saw_arch);
  const std::string log4 = read_all(logp);
  EXPECT_NE(log4.find("SESSION_OPEN"), std::string::npos) << log4;

  eng.shutdown();
}

TEST(EmbedClient, JournalAndRecover) {
  const auto root = temp_dir("embed");
  std::filesystem::remove_all(root);
  std::filesystem::create_directories(root);

  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;

  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "session", &err)) << err;

  structdb::client::CommandBatch b1;
  b1.idempotency_token = "t1";
  b1.puts.push_back({"a", "1"});
  ASSERT_TRUE(client.submit(b1, false, &err)) << err;

  structdb::client::CommandBatch b2;
  b2.idempotency_token = "t2";
  b2.puts.push_back({"b", "2"});
  ASSERT_TRUE(client.submit(b2, false, &err)) << err;

  ASSERT_TRUE(client.save_checkpoint(&err)) << err;

  std::string va;
  ASSERT_TRUE(eng.storage()->get("a", &va));
  ASSERT_EQ(va, "1");

  eng.shutdown();
}

TEST(EmbedClient, RecoverAfterCrashWithoutSessionCkpt) {
  const auto root = temp_dir("crash_rec");
  std::filesystem::remove_all(root);
  {
    structdb::facade::Engine eng;
    structdb::facade::EngineConfigSnapshot snap;
    snap.data_dir = (root / "_data").string();
    eng.config().update(1, snap);
    std::string err;
    ASSERT_TRUE(eng.startup(&err)) << err;
    structdb::client::EmbedClient c(eng);
    ASSERT_TRUE(c.open(root / "sess", &err)) << err;
    structdb::client::CommandBatch b;
    b.idempotency_token = "crash1";
    b.puts.push_back({"p", "q"});
    ASSERT_TRUE(c.submit(b, false, &err)) << err;
    eng.shutdown();
  }
  structdb::facade::Engine eng2;
  structdb::facade::EngineConfigSnapshot snap2;
  snap2.data_dir = (root / "_data").string();
  eng2.config().update(1, snap2);
  std::string err;
  ASSERT_TRUE(eng2.startup(&err)) << err;
  structdb::client::EmbedClient c2(eng2);
  ASSERT_TRUE(c2.open(root / "sess", &err)) << err;
  std::string v;
  ASSERT_TRUE(eng2.storage()->get("p", &v));
  ASSERT_EQ(v, "q");
  eng2.shutdown();
}

TEST(EmbedClient, MultiKeyBatchFsyncSurvivesRestart) {
  const auto root = temp_dir("embed_mkb");
  std::filesystem::remove_all(root);
  {
    structdb::facade::Engine eng;
    structdb::facade::EngineConfigSnapshot snap;
    snap.data_dir = (root / "_data").string();
    eng.config().update(1, snap);
    std::string err;
    ASSERT_TRUE(eng.startup(&err)) << err;
    structdb::client::EmbedClient c(eng);
    ASSERT_TRUE(c.open(root / "sess", &err)) << err;
    structdb::client::CommandBatch b;
    b.idempotency_token = "mkb1";
    b.puts.push_back({"mka", "1"});
    b.puts.push_back({"mkb", "2"});
    ASSERT_TRUE(c.submit(b, true, &err)) << err;
    eng.shutdown();
  }
  structdb::facade::Engine eng2;
  structdb::facade::EngineConfigSnapshot snap2;
  snap2.data_dir = (root / "_data").string();
  eng2.config().update(1, snap2);
  std::string err;
  ASSERT_TRUE(eng2.startup(&err)) << err;
  structdb::client::EmbedClient c2(eng2);
  ASSERT_TRUE(c2.open(root / "sess", &err)) << err;
  std::string a;
  std::string b;
  ASSERT_TRUE(eng2.storage()->get("mka", &a));
  ASSERT_TRUE(eng2.storage()->get("mkb", &b));
  EXPECT_EQ(a, "1");
  EXPECT_EQ(b, "2");
  eng2.shutdown();
}

TEST(EmbedClient, SessionCkptPersistsCheckpointSeq) {
  const auto root = temp_dir("embed_ckpt_seq");
  std::filesystem::remove_all(root);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient c(eng);
  ASSERT_TRUE(c.open(root / "sess", &err)) << err;
  structdb::client::CommandBatch b;
  b.idempotency_token = "cks1";
  b.puts.push_back({"cksk", "1"});
  ASSERT_TRUE(c.submit(b, false, &err)) << err;
  ASSERT_TRUE(c.save_checkpoint(&err)) << err;
  EXPECT_GE(c.last_engine_checkpoint_seq(), 1u);
  c.close();
  structdb::client::EmbedClient c2(eng);
  ASSERT_TRUE(c2.open(root / "sess", &err)) << err;
  EXPECT_GE(c2.last_engine_checkpoint_seq(), 1u);
  eng.shutdown();
}

TEST(StorageEngine, VersionedUndoUsesSstWhenMemEmpty) {
  const auto dir = temp_dir("undo_sst");
  std::filesystem::remove_all(dir);
  namespace mk = structdb::storage::mdb_keyspace;
  const std::string rk = mk::row_key("utbl", "1");
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  ASSERT_TRUE(st.put(rk, "a", false));
  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  ASSERT_TRUE(st.put(rk, "b", false));
  std::string v;
  ASSERT_TRUE(st.get(rk, &v));
  EXPECT_EQ(v, "b");
  ASSERT_TRUE(st.rollback_one_undo_frame(&err)) << err;
  ASSERT_TRUE(st.get(rk, &v));
  EXPECT_EQ(v, "a");
  st.close();
}

TEST(StorageEngine, EmbedWalBatchSingleRecordReplay) {
  const auto dir = temp_dir("wal_batch_replay");
  std::filesystem::remove_all(dir);
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  const std::vector<std::string> dels;
  const std::vector<std::pair<std::string, std::string>> puts{{"ba", "1"}, {"bb", "2"}};
  ASSERT_TRUE(st.commit_embed_batch(dels, puts, false, &err)) << err;
  st.close();

  structdb::storage::StorageEngine st2(dir / "d");
  ASSERT_TRUE(st2.open(&err)) << err;
  std::string a;
  std::string b;
  ASSERT_TRUE(st2.get("ba", &a));
  ASSERT_TRUE(st2.get("bb", &b));
  EXPECT_EQ(a, "1");
  EXPECT_EQ(b, "2");
  st2.close();
}

TEST(StorageEngine, WalTruncatedTailIgnoresPartialLastRecord) {
  const auto dir = temp_dir("wal_trunc_tail");
  std::filesystem::remove_all(dir);
  const auto wal_path = dir / "d" / "wal.log";
  {
    structdb::storage::StorageEngine st(dir / "d");
    std::string err;
    ASSERT_TRUE(st.open(&err)) << err;
    ASSERT_TRUE(st.put("t1", "one", false));
    ASSERT_TRUE(st.put("t2", "two", false));
    st.close();
  }
  {
    const auto sz = std::filesystem::file_size(wal_path);
    ASSERT_GT(sz, 8u);
    std::filesystem::resize_file(wal_path, sz - 4);
  }
  {
    structdb::storage::StorageEngine st2(dir / "d");
    std::string err;
    ASSERT_TRUE(st2.open(&err)) << err;
    std::string v;
    ASSERT_TRUE(st2.get("t1", &v));
    EXPECT_EQ(v, "one");
    ASSERT_FALSE(st2.get("t2", &v));
    st2.close();
  }
}

TEST(StorageEngine, WalReplayRejectsMalformedCompleteRecord) {
  const auto dir = temp_dir("wal_bad_rec");
  std::filesystem::remove_all(dir);
  const auto wal_path = dir / "d" / "wal.log";
  {
    structdb::storage::StorageEngine st(dir / "d");
    std::string err;
    ASSERT_TRUE(st.open(&err)) << err;
    ASSERT_TRUE(st.put("ok", "v", false));
    st.close();
  }
  {
    std::fstream wf(wal_path, std::ios::binary | std::ios::in | std::ios::out | std::ios::ate);
    ASSERT_TRUE(wf.is_open());
    const std::uint32_t le = 2;
    wf.write(reinterpret_cast<const char*>(&le), sizeof(le));
    wf.write("no", 2);  // no trailing newline for text WAL line format
    ASSERT_TRUE(static_cast<bool>(wf));
  }
  structdb::storage::StorageEngine st2(dir / "d");
  std::string err;
  ASSERT_FALSE(st2.open(&err));
  st2.close();
}

TEST(StorageEngine, WalBatchThenMalformedLineRecordRejectsOpen) {
  const auto dir = temp_dir("wal_batch_then_bad");
  std::filesystem::remove_all(dir);
  const auto wal_path = dir / "d" / "wal.log";
  {
    structdb::storage::StorageEngine st(dir / "d");
    std::string err;
    ASSERT_TRUE(st.open(&err)) << err;
    const std::vector<std::string> dels;
    const std::vector<std::pair<std::string, std::string>> puts{{"z", "9"}};
    ASSERT_TRUE(st.commit_embed_batch(dels, puts, false, &err)) << err;
    st.close();
  }
  {
    std::fstream wf(wal_path, std::ios::binary | std::ios::in | std::ios::out | std::ios::ate);
    ASSERT_TRUE(wf.is_open());
    const std::uint32_t le = 2;
    wf.write(reinterpret_cast<const char*>(&le), sizeof(le));
    wf.write("no", 2);
    ASSERT_TRUE(static_cast<bool>(wf));
  }
  structdb::storage::StorageEngine st2(dir / "d");
  std::string err;
  ASSERT_FALSE(st2.open(&err));
  st2.close();
}

TEST(StorageEngine, WalMultiSegmentRollReplaySurvivesRestart) {
  const auto dir = temp_dir("wal_multi_seg");
  std::filesystem::remove_all(dir);
  {
    structdb::storage::StorageEngine st(dir / "d");
    st.set_wal_segment_roll_max_bytes(80);
    std::string err;
    ASSERT_TRUE(st.open(&err)) << err;
    for (int i = 0; i < 30; ++i) {
      const std::string key = "segk" + std::to_string(i);
      ASSERT_TRUE(st.put(key, std::string(40, static_cast<char>('a' + (i % 26))), false)) << i;
    }
    EXPECT_GE(st.wal_log_segment_count_observed(), 2u);
    std::string v;
    ASSERT_TRUE(st.get("segk5", &v));
    st.close();
  }
  {
    structdb::storage::StorageEngine st2(dir / "d");
    std::string err;
    ASSERT_TRUE(st2.open(&err)) << err;
    std::string v;
    ASSERT_TRUE(st2.get("segk5", &v));
    EXPECT_EQ(v.size(), 40u);
    st2.close();
  }
}

TEST(StorageEngine, WalMultiSegmentTruncatedTailIgnoresPartialLastRecord) {
  const auto dir = temp_dir("wal_mseg_trunc");
  const auto wal_path = dir / "d" / "wal.log";
  std::filesystem::remove_all(dir);
  {
    structdb::storage::StorageEngine st(dir / "d");
    st.set_wal_segment_roll_max_bytes(128);
    std::string err;
    ASSERT_TRUE(st.open(&err)) << err;
    ASSERT_TRUE(st.put("mst1", "one", false));
    for (int i = 0; i < 20; ++i) {
      const std::string key = "msfill" + std::to_string(i);
      ASSERT_TRUE(st.put(key, std::string(40, 'x'), false)) << i;
    }
    ASSERT_TRUE(st.put("mst2", "two", false));
    ASSERT_GE(st.wal_log_segment_count_observed(), 2u);
    st.close();
  }
  {
    ASSERT_TRUE(std::filesystem::exists(wal_path));
    const auto sz = std::filesystem::file_size(wal_path);
    ASSERT_GT(sz, 8u);
    std::filesystem::resize_file(wal_path, sz - 4);
  }
  {
    structdb::storage::StorageEngine st2(dir / "d");
    std::string err;
    ASSERT_TRUE(st2.open(&err)) << err;
    std::string v;
    ASSERT_TRUE(st2.get("mst1", &v));
    EXPECT_EQ(v, "one");
    ASSERT_FALSE(st2.get("mst2", &v));
    st2.close();
  }
}

TEST(StorageEngine, WalMultiSegmentArchiveGcAfterFlushRemovesSealedFiles) {
  const auto dir = temp_dir("wal_gc_arch");
  const auto arch = dir / "d" / "wal" / "archive";
  std::filesystem::remove_all(dir);
  {
    structdb::storage::StorageEngine st(dir / "d");
    st.set_wal_segment_roll_max_bytes(96);
    st.set_wal_auto_trim_prefix_after_flush(true);
    st.set_wal_archive_gc_after_flush(true);
    std::string err;
    ASSERT_TRUE(st.open(&err)) << err;
    for (int i = 0; i < 25; ++i) {
      ASSERT_TRUE(st.put("gck" + std::to_string(i), std::string(32, 'a'), false)) << i;
    }
    ASSERT_GE(st.wal_log_segment_count_observed(), 2u);
    ASSERT_TRUE(std::filesystem::exists(arch));
    ASSERT_FALSE(std::filesystem::is_empty(arch));
    ASSERT_TRUE(st.flush_memtable(&err)) << err;
    EXPECT_EQ(st.wal_log_segment_count_observed(), 1u);
    if (std::filesystem::exists(arch)) {
      EXPECT_TRUE(std::filesystem::is_empty(arch));
    }
    st.close();
  }
  {
    structdb::storage::StorageEngine st2(dir / "d");
    std::string err;
    ASSERT_TRUE(st2.open(&err)) << err;
    std::string v;
    ASSERT_TRUE(st2.get("gck0", &v));
    EXPECT_EQ(v.size(), 32u);
    st2.close();
  }
}

TEST(StorageEngine, WalArchiveGcRequiresAutoTrim) {
  const auto dir = temp_dir("wal_gc_need_trim");
  std::filesystem::remove_all(dir);
  structdb::storage::StorageEngine st(dir / "d");
  st.set_wal_archive_gc_after_flush(true);
  st.set_wal_auto_trim_prefix_after_flush(false);
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  ASSERT_TRUE(st.put("only", "x", false));
  ASSERT_FALSE(st.flush_memtable(&err));
  st.close();
}

TEST(StorageEngine, WalSegmentsV2MissingSealedRejectsOpen) {
  const auto dir = temp_dir("wal_v2_miss");
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir / "d");
  {
    std::ofstream seg(dir / "d" / "wal.segments", std::ios::trunc);
    seg << "2\n2\n1\nwal/archive/000001.log\n";
  }
  {
    std::ofstream w(dir / "d" / "wal.log");
    w << "";
  }
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_FALSE(st.open(&err));
  st.close();
}

#if defined(_WIN32) && defined(STRUCTDB_HAVE_IOCP)
TEST(StorageEngine, WalIocpBackendRoundTrip) {
  const auto dir = temp_dir("wal_iocp_rt");
  std::filesystem::remove_all(dir);
  structdb::storage::StorageEngine st(dir / "d");
  structdb::infra::IoBackendConfig ioc{};
  ioc.kind = structdb::infra::IoBackendKind::IocpAsync;
  st.set_wal_io_backend(ioc);
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  ASSERT_TRUE(st.put("iocpk", "iocpv", true));
  st.close();
  structdb::storage::StorageEngine st2(dir / "d");
  st2.set_wal_io_backend(ioc);
  ASSERT_TRUE(st2.open(&err)) << err;
  std::string v;
  ASSERT_TRUE(st2.get("iocpk", &v));
  EXPECT_EQ(v, "iocpv");
  st2.close();
}
#endif

#if defined(__linux__) && defined(STRUCTDB_HAVE_IO_URING)
TEST(StorageEngine, WalIoUringBackendRoundTrip) {
  const auto dir = temp_dir("wal_uring_rt");
  std::filesystem::remove_all(dir);
  structdb::storage::StorageEngine st(dir / "d");
  structdb::infra::IoBackendConfig cfg{};
  cfg.kind = structdb::infra::IoBackendKind::IoUringAsync;
  st.set_wal_io_backend(cfg);
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  ASSERT_TRUE(st.put("uringk", "uringv", true));
  st.close();
  structdb::storage::StorageEngine st2(dir / "d");
  st2.set_wal_io_backend(cfg);
  ASSERT_TRUE(st2.open(&err)) << err;
  std::string v;
  ASSERT_TRUE(st2.get("uringk", &v));
  EXPECT_EQ(v, "uringv");
  st2.close();
}
#endif

TEST(Engine, Phase24EmbedBypassCounterIncrementsWhenObserved) {
  namespace fs = std::filesystem;
  const auto root = temp_dir("eng_p24_obs");
  fs::remove_all(root);
  fs::create_directories(root);

  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  snap.mdb_chain_rollback_on_mdb_rollback = true;
  snap.observe_embed_bypass_during_mdb_chain_txn = true;
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  eng.set_mdb_chain_txn_active_hint(true);
  ASSERT_TRUE(eng.kv_put("mdb$v2$cat$phase24_obs", "x", false));
  EXPECT_EQ(eng.embed_bypass_kv_put_during_mdb_chain_observed(), 1u);
  ASSERT_TRUE(eng.kv_put("mdb$v2$cat$phase24_obs2", "y", false));
  EXPECT_EQ(eng.embed_bypass_kv_put_during_mdb_chain_observed(), 2u);
  ASSERT_TRUE(eng.kv_put("plain_key", "v", false));
  EXPECT_EQ(eng.embed_bypass_kv_put_during_mdb_chain_observed(), 2u);
  eng.shutdown();
}

TEST(Engine, Phase24StrictRejectsDirectMdbKvPutWhenHinted) {
  namespace fs = std::filesystem;
  const auto root = temp_dir("eng_p24_strict");
  fs::remove_all(root);
  fs::create_directories(root);

  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  snap.mdb_chain_rollback_on_mdb_rollback = true;
  snap.strict_reject_direct_kv_put_during_mdb_chain_txn = true;
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  eng.set_mdb_chain_txn_active_hint(true);
  ASSERT_FALSE(eng.kv_put("mdb$v2$cat$phase24_strict", "x", false));
  ASSERT_TRUE(eng.kv_put("plain_key_strict", "v", false));
  eng.shutdown();
}

TEST(Engine, Phase24MdbBeginMaintainsHintForObservePath) {
  namespace fs = std::filesystem;
  const auto root = temp_dir("eng_p24_mdb");
  fs::remove_all(root);
  fs::create_directories(root);

  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  snap.mdb_chain_rollback_on_mdb_rollback = true;
  snap.observe_embed_bypass_during_mdb_chain_txn = true;
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
  ASSERT_TRUE(run("CREATE TABLE(tp24)").ok);
  ASSERT_TRUE(run("USE(tp24)").ok);
  ASSERT_TRUE(run("DEFATTR(k:string)").ok);
  ASSERT_TRUE(run("BEGIN").ok);
  ASSERT_TRUE(eng.kv_put("mdb$v2$cat$p24int", "z", false));
  EXPECT_GE(eng.embed_bypass_kv_put_during_mdb_chain_observed(), 1u);
  const auto c = eng.embed_bypass_kv_put_during_mdb_chain_observed();
  ASSERT_TRUE(run("ROLLBACK").ok);
  ASSERT_TRUE(eng.kv_put("mdb$v2$cat$p24post", "w", false));
  EXPECT_EQ(eng.embed_bypass_kv_put_during_mdb_chain_observed(), c);
  client.close();
  eng.shutdown();
}

TEST(StorageEngine, Phase31FlushCheckpointManifestVersionMatchesManifest) {
  const auto dir = temp_dir("p31_flush_mv");
  std::filesystem::remove_all(dir);
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  ASSERT_TRUE(st.put("p31_flush_k", "v", false));
  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  structdb::storage::CheckpointState ck{};
  ASSERT_TRUE(st.read_checkpoint_state(&ck));
  EXPECT_EQ(ck.manifest_version, st.manifest().version());
  st.close();
}

TEST(StorageEngine, Phase31CompactCheckpointManifestVersionMatchesManifest) {
  const auto dir = temp_dir("p31_compact_mv");
  std::filesystem::remove_all(dir);
  namespace mk = structdb::storage::mdb_keyspace;
  const std::string k1 = mk::row_key("p31cmp", "1");
  const std::string k2 = mk::row_key("p31cmp", "2");
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  ASSERT_TRUE(st.put(k1, "a", false));
  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  ASSERT_TRUE(st.put(k2, "b", false));
  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  ASSERT_TRUE(st.compact_merge_two_oldest_l0(&err)) << err;
  structdb::storage::CheckpointState ck{};
  ASSERT_TRUE(st.read_checkpoint_state(&ck));
  EXPECT_EQ(ck.manifest_version, st.manifest().version());
  st.close();
}

TEST(Engine, Phase31WalCommittedKvSurvivesColdRestart) {
  namespace fs = std::filesystem;
  const auto root = temp_dir("p31_wal_only");
  fs::remove_all(root);
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  {
    structdb::facade::Engine eng;
    eng.config().update(1, snap);
    std::string err;
    ASSERT_TRUE(eng.startup(&err)) << err;
    ASSERT_TRUE(eng.kv_put("p31_plain_k", "p31_plain_v", false));
    eng.shutdown();
  }
  structdb::facade::Engine eng2;
  eng2.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng2.startup(&err)) << err;
  std::string v;
  ASSERT_TRUE(eng2.kv_get("p31_plain_k", &v));
  EXPECT_EQ(v, "p31_plain_v");
  eng2.shutdown();
}

TEST(EmbedClient, Phase31JournalReplayAfterCrashChainsAfterEngineWal) {
  namespace fs = std::filesystem;
  const auto root = temp_dir("p31_embed_chain");
  fs::remove_all(root);
  {
    structdb::facade::Engine eng;
    structdb::facade::EngineConfigSnapshot snap;
    snap.data_dir = (root / "_data").string();
    eng.config().update(1, snap);
    std::string err;
    ASSERT_TRUE(eng.startup(&err)) << err;
    structdb::client::EmbedClient c(eng);
    ASSERT_TRUE(c.open(root / "sess", &err)) << err;
    structdb::client::CommandBatch b;
    b.idempotency_token = "p31_emb";
    b.puts.push_back({"p31_emb_key", "p31_emb_val"});
    ASSERT_TRUE(c.submit(b, false, &err)) << err;
    eng.shutdown();
  }
  structdb::facade::Engine eng2;
  structdb::facade::EngineConfigSnapshot snap2;
  snap2.data_dir = (root / "_data").string();
  eng2.config().update(1, snap2);
  std::string err;
  ASSERT_TRUE(eng2.startup(&err)) << err;
  structdb::client::EmbedClient c2(eng2);
  ASSERT_TRUE(c2.open(root / "sess", &err)) << err;
  std::string v;
  ASSERT_TRUE(eng2.storage()->get("p31_emb_key", &v));
  EXPECT_EQ(v, "p31_emb_val");
  eng2.shutdown();
}

TEST(Engine, Phase31CorruptSessionTxnDropsTxnLogOnRepl) {
  namespace fs = std::filesystem;
  const auto root = temp_dir("p31_txn_corrupt");
  fs::remove_all(root);
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  structdb::facade::Engine eng;
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient client(eng);
  ASSERT_TRUE(client.open(root / "r31", &err)) << err;
  structdb::client::mdb::MdbInteractiveSession session;
  std::vector<std::string> log;
  ASSERT_TRUE(
      structdb::client::mdb::mdb_repl_execute_line(eng, client, session, "CREATE TABLE(t31)", &log, false, false, nullptr).ok);
  ASSERT_TRUE(structdb::client::mdb::mdb_repl_execute_line(eng, client, session, "USE(t31)", &log, false, false, nullptr).ok);
  ASSERT_TRUE(
      structdb::client::mdb::mdb_repl_execute_line(eng, client, session, "DEFATTR(x:string)", &log, false, false, nullptr).ok);
  ASSERT_TRUE(structdb::client::mdb::mdb_repl_execute_line(eng, client, session, "BEGIN", &log, false, false, nullptr).ok);
  ASSERT_TRUE(structdb::client::mdb::mdb_repl_execute_line(eng, client, session, "INSERT(1,a)", &log, false, false, nullptr).ok);
  const fs::path txn_path = root / "r31" / structdb::client::kEmbedSessionArtifactsDir / "session.txn";
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
  ASSERT_TRUE(client2.open(root / "r31", &err)) << err;
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

TEST(Engine, Phase31ObserveBypassWithSecondEmbedClientAlsoOpen) {
  namespace fs = std::filesystem;
  const auto root = temp_dir("p31_two_embed_obs");
  fs::remove_all(root);
  fs::create_directories(root);

  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  snap.mdb_chain_rollback_on_mdb_rollback = true;
  snap.observe_embed_bypass_during_mdb_chain_txn = true;
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;

  structdb::client::EmbedClient ca(eng);
  structdb::client::EmbedClient cb(eng);
  ASSERT_TRUE(ca.open(root / "sessA", &err)) << err;
  ASSERT_TRUE(cb.open(root / "sessB", &err)) << err;

  structdb::client::mdb::MdbInteractiveSession session;
  std::vector<std::string> log;
  auto run = [&](const char* line) {
    return structdb::client::mdb::mdb_repl_execute_line(eng, ca, session, line, &log, false, false, nullptr);
  };
  ASSERT_TRUE(run("CREATE TABLE(tp31)").ok);
  ASSERT_TRUE(run("USE(tp31)").ok);
  ASSERT_TRUE(run("DEFATTR(k:string)").ok);
  ASSERT_TRUE(run("BEGIN").ok);
  ASSERT_TRUE(eng.kv_put("mdb$v2$cat$p31_two_embed", "z", false));
  EXPECT_GE(eng.embed_bypass_kv_put_during_mdb_chain_observed(), 1u);

  ca.close();
  cb.close();
  eng.shutdown();
}

TEST(EmbedClient, ComplexNestedMdbTxnSavepointEmbedJournalAndRestart) {
  namespace fs = std::filesystem;
  const auto root = temp_dir("embed_nested_complex");
  fs::remove_all(root);
  const auto sess = root / "nested" / "deep" / "sess";
  fs::create_directories(sess);

  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;

  auto read_all = [](const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
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
  const fs::path logp = sess / structdb::client::kEmbedSessionArtifactsDir / "session_log.txt";

  for (int cycle = 0; cycle < 3; ++cycle) {
    structdb::client::EmbedClient c(eng);
    ASSERT_TRUE(c.open(sess, &err)) << err << " cycle=" << cycle;

    for (int epoch = 0; epoch < 2; ++epoch) {
      for (int layer = 0; layer < 3; ++layer) {
        structdb::client::CommandBatch b;
        b.idempotency_token = std::string("idem_c") + std::to_string(cycle) + "_e" + std::to_string(epoch) + "_L" + std::to_string(layer);
        const std::string k =
            std::string("embk_") + std::to_string(cycle) + "_" + std::to_string(epoch) + "_" + std::to_string(layer);
        b.puts.push_back({k, "v"});
        ASSERT_TRUE(c.submit(b, (layer % 2) == 0, &err)) << err << " k=" << k;
      }
    }

    structdb::client::mdb::MdbInteractiveSession mdb;
    structdb::client::mdb::mdb_repl_reset(mdb);
    std::vector<std::string> log;
    auto repl = [&](const std::string& line) {
      err.clear();
      return structdb::client::mdb::mdb_repl_execute_line(eng, c, mdb, line, &log, true, true, &err);
    };

    const std::string tbl = std::string("tn_c") + std::to_string(cycle);
    ASSERT_TRUE(repl("CREATE TABLE(" + tbl + ")").ok) << err;
    ASSERT_TRUE(repl("USE(" + tbl + ")").ok) << err;
    ASSERT_TRUE(repl("DEFATTR(x:string)").ok) << err;
    ASSERT_TRUE(repl("BEGIN").ok) << err;
    ASSERT_TRUE(repl("INSERT(1,before_sp)").ok) << err;
    ASSERT_TRUE(repl("SAVEPOINT sp1").ok) << err;
    ASSERT_TRUE(repl("INSERT(2,in_sp)").ok) << err;
    ASSERT_TRUE(repl("ROLLBACK TO SAVEPOINT sp1").ok) << err;
    ASSERT_TRUE(repl("INSERT(3,after_rts)").ok) << err;
    ASSERT_TRUE(repl("COMMIT").ok) << err;
    log.clear();
    ASSERT_TRUE(repl("COUNT").ok) << err;
    std::string count_line;
    for (auto it = log.rbegin(); it != log.rend(); ++it) {
      if (it->find("[COUNT]") != std::string::npos) {
        count_line = *it;
        break;
      }
    }
    ASSERT_FALSE(count_line.empty()) << "COUNT log missing";
    EXPECT_NE(count_line.find("rows=2"), std::string::npos) << count_line;

    ASSERT_TRUE(c.save_checkpoint(&err)) << err;
    c.close();

    const std::string slog = read_all(logp);
    EXPECT_GE(count_sub(slog, "SESSION_OPEN"), static_cast<std::size_t>(cycle + 1)) << slog;
    EXPECT_GE(count_sub(slog, "SESSION_CLOSE"), static_cast<std::size_t>(cycle + 1)) << slog;
  }

  eng.shutdown();

  structdb::facade::Engine eng2;
  eng2.config().update(1, snap);
  ASSERT_TRUE(eng2.startup(&err)) << err;
  structdb::client::EmbedClient c2(eng2);
  ASSERT_TRUE(c2.open(sess, &err)) << err;

  for (int i = 0; i < 3; ++i) {
    const std::string k = std::string("embk_2_1_") + std::to_string(i);
    std::string v;
    ASSERT_TRUE(eng2.storage()->get(k, &v)) << k;
    EXPECT_EQ(v, "v");
  }

  structdb::client::mdb::MdbInteractiveSession mdb2;
  std::vector<std::string> log2;
  auto repl2 = [&](const std::string& line) {
    err.clear();
    return structdb::client::mdb::mdb_repl_execute_line(eng2, c2, mdb2, line, &log2, false, false, &err);
  };
  for (int t = 0; t < 3; ++t) {
    log2.clear();
    const std::string ut = std::string("USE(tn_c") + std::to_string(t) + ")";
    ASSERT_TRUE(repl2(ut).ok) << err << " use=" << ut;
    log2.clear();
    ASSERT_TRUE(repl2("COUNT").ok) << err << " table=" << t;
    std::string count_line;
    for (auto it = log2.rbegin(); it != log2.rend(); ++it) {
      if (it->find("[COUNT]") != std::string::npos) {
        count_line = *it;
        break;
      }
    }
    ASSERT_FALSE(count_line.empty()) << "COUNT missing t=" << t;
    EXPECT_NE(count_line.find("rows=2"), std::string::npos) << count_line;
  }

  c2.close();
  eng2.shutdown();
}

TEST(StorageEngine, Phase36ConcurrentPutWhileL2ToL3Compact) {
  const auto dir = temp_dir("phase36_mt_l2_l3");
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir / "d");
  auto write_sst_one_kv = [](const std::filesystem::path& p, const std::string& k, const std::string& v) {
    std::ofstream o(p, std::ios::binary);
    auto u32 = [&o](std::uint32_t x) {
      for (int i = 0; i < 4; ++i) o.put(static_cast<char>((x >> (8 * i)) & 0xff));
    };
    u32(static_cast<std::uint32_t>(k.size()));
    o.write(k.data(), static_cast<std::streamsize>(k.size()));
    u32(static_cast<std::uint32_t>(v.size()));
    o.write(v.data(), static_cast<std::streamsize>(v.size()));
  };
  write_sst_one_kv(dir / "d" / "L2-a.sst", "ka", "va");
  write_sst_one_kv(dir / "d" / "L2-b.sst", "kb", "vb");
  {
    std::ofstream m(dir / "d" / "MANIFEST", std::ios::trunc);
    m << "1\nFORMAT2\n2\n2 L2-a.sst\n2 L2-b.sst\n";
  }
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  st.set_l3_compact_output_from_l2_merge(true);

  std::atomic<bool> stop{false};
  std::thread merger([&] {
    while (!stop.load(std::memory_order_relaxed)) {
      std::string e;
      (void)st.compact_merge_two_oldest_l2_to_l3(&e);
      std::this_thread::sleep_for(std::chrono::microseconds(20));
    }
  });
  for (int i = 0; i < 80; ++i) {
    ASSERT_TRUE(st.put(std::string("pk36_") + std::to_string(i), "v", false)) << i;
    if (i % 5 == 4) {
      ASSERT_TRUE(st.flush_memtable(&err)) << err << " i=" << i;
    }
  }
  stop.store(true, std::memory_order_relaxed);
  merger.join();

  std::string v;
  ASSERT_TRUE(st.get("ka", &v));
  EXPECT_EQ(v, "va");
  ASSERT_TRUE(st.get("kb", &v));
  EXPECT_EQ(v, "vb");
  ASSERT_TRUE(st.get("pk36_0", &v));
  EXPECT_EQ(v, "v");
  st.close();
}

TEST(Engine, Phase36FacadeKvPutQueueCapObservedInPressure) {
  const auto dir = temp_dir("p36_kvqueue_pressure");
  std::filesystem::remove_all(dir);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (dir / "_data").string();
  snap.version = 1;
  snap.kv_put_async_queue_depth = 7;
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::storage::StoragePressureSnapshot p{};
  eng.storage_pressure_snapshot(&p);
  EXPECT_EQ(p.facade_kv_put_queue_cap, 7u);
  EXPECT_EQ(p.facade_kv_put_queue_depth, 0u);
  ASSERT_TRUE(eng.kv_put("qx", "qy", false));
  eng.storage_pressure_snapshot(&p);
  EXPECT_EQ(p.facade_kv_put_queue_depth, 0u);
  std::string gv;
  ASSERT_TRUE(eng.kv_get("qx", &gv));
  EXPECT_EQ(gv, "qy");
  eng.shutdown();
}

TEST(Engine, Phase36FacadeKvPutAsyncQueuedPathWiring) {
  const auto dir = temp_dir("p36_kvqueue_many");
  std::filesystem::remove_all(dir);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (dir / "_data").string();
  snap.version = 1;
  snap.kv_put_async_queue_depth = 8;
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  for (int i = 0; i < 40; ++i) {
    ASSERT_TRUE(eng.kv_put(std::string("qq") + std::to_string(i), "z", false)) << i;
  }
  std::string gv;
  ASSERT_TRUE(eng.kv_get("qq0", &gv));
  EXPECT_EQ(gv, "z");
  eng.shutdown();
}

TEST(StorageEngine, Phase37ConcurrentPutWhileL1ToL2Compact) {
  const auto dir = temp_dir("phase37_mt_l1_l2");
  std::filesystem::remove_all(dir);
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  st.set_l1_compact_output_from_l0_merge(true);
  st.set_l2_compact_output_from_l1_merge(true);
  ASSERT_TRUE(st.put("k", "a", false));
  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  ASSERT_TRUE(st.put("k", "b", false));
  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  ASSERT_TRUE(st.compact_merge_two_oldest_l0(&err)) << err;
  ASSERT_TRUE(st.put("k2", "x", false));
  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  ASSERT_TRUE(st.put("k2", "y", false));
  ASSERT_TRUE(st.flush_memtable(&err)) << err;
  ASSERT_TRUE(st.compact_merge_two_oldest_l0(&err)) << err;

  std::atomic<bool> stop{false};
  std::thread merger([&] {
    while (!stop.load(std::memory_order_relaxed)) {
      std::string e;
      (void)st.compact_merge_two_oldest_l1_to_l2(&e);
      std::this_thread::sleep_for(std::chrono::microseconds(20));
    }
  });
  for (int i = 0; i < 80; ++i) {
    ASSERT_TRUE(st.put(std::string("pk37_") + std::to_string(i), "v", false)) << i;
    if (i % 5 == 4) {
      ASSERT_TRUE(st.flush_memtable(&err)) << err << " i=" << i;
    }
  }
  stop.store(true, std::memory_order_relaxed);
  merger.join();

  std::string v;
  ASSERT_TRUE(st.get("k", &v));
  EXPECT_EQ(v, "b");
  ASSERT_TRUE(st.get("k2", &v));
  EXPECT_EQ(v, "y");
  ASSERT_TRUE(st.get("pk37_0", &v));
  EXPECT_EQ(v, "v");
  st.close();
}

TEST(StorageEngine, Phase37ConcurrentPutWhileL3ToL4Compact) {
  const auto dir = temp_dir("phase37_mt_l3_l4");
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir / "d");
  auto write_sst_one_kv = [](const std::filesystem::path& p, const std::string& k, const std::string& v) {
    std::ofstream o(p, std::ios::binary);
    auto u32 = [&o](std::uint32_t x) {
      for (int i = 0; i < 4; ++i) o.put(static_cast<char>((x >> (8 * i)) & 0xff));
    };
    u32(static_cast<std::uint32_t>(k.size()));
    o.write(k.data(), static_cast<std::streamsize>(k.size()));
    u32(static_cast<std::uint32_t>(v.size()));
    o.write(v.data(), static_cast<std::streamsize>(v.size()));
  };
  write_sst_one_kv(dir / "d" / "L3-a.sst", "ka3", "va3");
  write_sst_one_kv(dir / "d" / "L3-b.sst", "kb3", "vb3");
  {
    std::ofstream m(dir / "d" / "MANIFEST", std::ios::trunc);
    m << "1\nFORMAT2\n2\n3 L3-a.sst\n3 L3-b.sst\n";
  }
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  st.set_l4_compact_output_from_l3_merge(true);

  std::atomic<bool> stop{false};
  std::thread merger([&] {
    while (!stop.load(std::memory_order_relaxed)) {
      std::string e;
      (void)st.compact_merge_two_oldest_l3_to_l4(&e);
      std::this_thread::sleep_for(std::chrono::microseconds(20));
    }
  });
  for (int i = 0; i < 80; ++i) {
    ASSERT_TRUE(st.put(std::string("pk37b_") + std::to_string(i), "v", false)) << i;
    if (i % 5 == 4) {
      ASSERT_TRUE(st.flush_memtable(&err)) << err << " i=" << i;
    }
  }
  stop.store(true, std::memory_order_relaxed);
  merger.join();

  std::string v;
  ASSERT_TRUE(st.get("ka3", &v));
  EXPECT_EQ(v, "va3");
  ASSERT_TRUE(st.get("kb3", &v));
  EXPECT_EQ(v, "vb3");
  ASSERT_TRUE(st.get("pk37b_0", &v));
  EXPECT_EQ(v, "v");
  st.close();
}

TEST(StorageEngine, Phase35ConcurrentPutWithDeferredL0Drain) {
  const auto dir = temp_dir("phase35_mt_l0");
  std::filesystem::remove_all(dir);
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  st.set_l0_compact_trigger_threshold(2);
  st.set_l0_compact_defer_after_flush(true);
  std::atomic<bool> stop{false};
  std::thread worker([&] {
    while (!stop.load(std::memory_order_relaxed)) {
      std::string e;
      (void)st.drain_pending_l0_compactions(4, &e);
      std::this_thread::sleep_for(std::chrono::microseconds(30));
    }
  });
  for (int i = 0; i < 100; ++i) {
    ASSERT_TRUE(st.put(std::string("pk") + std::to_string(i), "v", false)) << i;
    if (i % 4 == 3) {
      ASSERT_TRUE(st.flush_memtable(&err)) << err << " i=" << i;
    }
  }
  stop.store(true, std::memory_order_relaxed);
  worker.join();
  ASSERT_TRUE(st.drain_pending_l0_compactions(32, &err)) << err;
  st.close();
}

/// PHASE31 矩阵 F（flush / L0 / L1+ compaction 后 checkpoint `manifest_version` 与 `manifest().version()` 一致）的
/// 多行语义回归；各行独立目录，顺序执行（与并发用例互补）。
TEST(StorageEngine, CompactionConcurrencySemanticMatrix) {
  auto assert_ck_manifest_match = [](structdb::storage::StorageEngine& st) {
    structdb::storage::CheckpointState ck{};
    ASSERT_TRUE(st.read_checkpoint_state(&ck)) << "read_checkpoint_state";
    EXPECT_EQ(ck.manifest_version, st.manifest().version())
        << "matrix_F: checkpoint.manifest_version vs manifest().version()";
  };
  std::string err;

  {
    const auto dir = temp_dir("sem_mtx_flush");
    std::filesystem::remove_all(dir);
    structdb::storage::StorageEngine st(dir / "d");
    ASSERT_TRUE(st.open(&err)) << err;
    ASSERT_TRUE(st.put("mtx_flush_k", "1", false));
    ASSERT_TRUE(st.flush_memtable(&err)) << err;
    assert_ck_manifest_match(st);
    st.close();
  }
  {
    const auto dir = temp_dir("sem_mtx_l0");
    std::filesystem::remove_all(dir);
    structdb::storage::StorageEngine st(dir / "d");
    ASSERT_TRUE(st.open(&err)) << err;
    ASSERT_TRUE(st.put("mtx_l0_a", "a", false));
    ASSERT_TRUE(st.flush_memtable(&err)) << err;
    ASSERT_TRUE(st.put("mtx_l0_b", "b", false));
    ASSERT_TRUE(st.flush_memtable(&err)) << err;
    ASSERT_TRUE(st.compact_merge_two_oldest_l0(&err)) << err;
    assert_ck_manifest_match(st);
    st.close();
  }
  {
    const auto dir = temp_dir("sem_mtx_l1_l2");
    std::filesystem::remove_all(dir);
    structdb::storage::StorageEngine st(dir / "d");
    ASSERT_TRUE(st.open(&err)) << err;
    st.set_l1_compact_output_from_l0_merge(true);
    st.set_l2_compact_output_from_l1_merge(true);
    ASSERT_TRUE(st.put("mtx_l1_k", "a", false));
    ASSERT_TRUE(st.flush_memtable(&err)) << err;
    ASSERT_TRUE(st.put("mtx_l1_k", "b", false));
    ASSERT_TRUE(st.flush_memtable(&err)) << err;
    ASSERT_TRUE(st.compact_merge_two_oldest_l0(&err)) << err;
    ASSERT_TRUE(st.put("mtx_l1_k2", "x", false));
    ASSERT_TRUE(st.flush_memtable(&err)) << err;
    ASSERT_TRUE(st.put("mtx_l1_k2", "y", false));
    ASSERT_TRUE(st.flush_memtable(&err)) << err;
    ASSERT_TRUE(st.compact_merge_two_oldest_l0(&err)) << err;
    ASSERT_TRUE(st.compact_merge_two_oldest_l1_to_l2(&err)) << err;
    assert_ck_manifest_match(st);
    st.close();
  }
  {
    const auto dir = temp_dir("sem_mtx_l2_l3");
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir / "d");
    auto write_sst_one_kv = [](const std::filesystem::path& p, const std::string& k, const std::string& v) {
      std::ofstream o(p, std::ios::binary);
      auto u32 = [&o](std::uint32_t x) {
        for (int i = 0; i < 4; ++i) o.put(static_cast<char>((x >> (8 * i)) & 0xff));
      };
      u32(static_cast<std::uint32_t>(k.size()));
      o.write(k.data(), static_cast<std::streamsize>(k.size()));
      u32(static_cast<std::uint32_t>(v.size()));
      o.write(v.data(), static_cast<std::streamsize>(v.size()));
    };
    write_sst_one_kv(dir / "d" / "L2-ma.sst", "mtx_ka", "va");
    write_sst_one_kv(dir / "d" / "L2-mb.sst", "mtx_kb", "vb");
    {
      std::ofstream m(dir / "d" / "MANIFEST", std::ios::trunc);
      m << "1\nFORMAT2\n2\n2 L2-ma.sst\n2 L2-mb.sst\n";
    }
    structdb::storage::StorageEngine st(dir / "d");
    ASSERT_TRUE(st.open(&err)) << err;
    st.set_l3_compact_output_from_l2_merge(true);
    ASSERT_TRUE(st.compact_merge_two_oldest_l2_to_l3(&err)) << err;
    assert_ck_manifest_match(st);
    st.close();
  }
}

/// 嵌套并发：延后 L0 drain 与 L1→L2 合并两路后台线程 + 主线程 put/flush 交错；结束后排空 L0 并校验矩阵 F 与业务键。
TEST(StorageEngine, ConcurrentNestedL0DrainAndL1MergeWhilePuts) {
  const auto dir = temp_dir("nested_l0_l1_mt");
  std::filesystem::remove_all(dir);
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  st.set_l0_compact_trigger_threshold(2);
  st.set_l0_compact_defer_after_flush(true);
  st.set_l1_compact_output_from_l0_merge(true);
  st.set_l2_compact_output_from_l1_merge(true);
  for (int i = 0; i < 8; ++i) {
    ASSERT_TRUE(st.put(std::string("seed_") + std::to_string(i), "x", false)) << i;
    ASSERT_TRUE(st.flush_memtable(&err)) << err << " seed i=" << i;
  }

  std::atomic<bool> stop{false};
  std::thread drainer([&] {
    while (!stop.load(std::memory_order_relaxed)) {
      std::string e;
      (void)st.drain_pending_l0_compactions(4, &e);
      std::this_thread::sleep_for(std::chrono::microseconds(25));
    }
  });
  std::thread l1merger([&] {
    while (!stop.load(std::memory_order_relaxed)) {
      std::string e;
      (void)st.compact_merge_two_oldest_l1_to_l2(&e);
      std::this_thread::sleep_for(std::chrono::microseconds(30));
    }
  });
  for (int i = 0; i < 120; ++i) {
    ASSERT_TRUE(st.put(std::string("mix_") + std::to_string(i), "v", false)) << i;
    if (i % 3 == 2) {
      ASSERT_TRUE(st.flush_memtable(&err)) << err << " mix i=" << i;
    }
  }
  stop.store(true, std::memory_order_relaxed);
  drainer.join();
  l1merger.join();

  ASSERT_TRUE(st.drain_pending_l0_compactions(64, &err)) << err;
  for (int r = 0; r < 32; ++r) {
    if (!st.compact_merge_two_oldest_l0(&err)) break;
  }

  structdb::storage::CheckpointState ck{};
  ASSERT_TRUE(st.read_checkpoint_state(&ck));
  EXPECT_EQ(ck.manifest_version, st.manifest().version());

  std::string v;
  ASSERT_TRUE(st.get("mix_0", &v));
  EXPECT_EQ(v, "v");
  ASSERT_TRUE(st.get("seed_0", &v));
  EXPECT_EQ(v, "x");
  st.close();
}

// --- Crash / corruption / nested recovery (embed journal + WAL + checkpoint) ---

TEST(EmbedClient, JournalReplayWhenWalEmptyAfterFlush) {
  const auto root = temp_dir("embed_jr_wal_empty");
  std::filesystem::remove_all(root);
  const auto sess = root / "sess";
  {
    structdb::facade::Engine eng;
    structdb::facade::EngineConfigSnapshot snap;
    snap.data_dir = (root / "_data").string();
    snap.wal_auto_trim_prefix_after_flush = true;
    eng.config().update(1, snap);
    std::string err;
    ASSERT_TRUE(eng.startup(&err)) << err;
    structdb::client::EmbedClient c(eng);
    ASSERT_TRUE(c.open(sess, &err)) << err;
    structdb::client::CommandBatch b;
    b.idempotency_token = "jr_wal0";
    b.puts.push_back({"replay_k", "from_journal"});
    ASSERT_TRUE(c.submit(b, false, &err)) << err;
    EXPECT_GT(eng.storage()->wal_log_bytes_on_disk(), 0u);
    ASSERT_TRUE(eng.storage()->flush_memtable(&err)) << err;
    EXPECT_EQ(eng.storage()->wal_log_bytes_on_disk(), 0u);
    c.close();
    eng.shutdown();
  }
  structdb::facade::Engine eng2;
  structdb::facade::EngineConfigSnapshot snap2;
  snap2.data_dir = (root / "_data").string();
  eng2.config().update(2, snap2);
  std::string err;
  ASSERT_TRUE(eng2.startup(&err)) << err;
  structdb::client::EmbedClient c2(eng2);
  ASSERT_TRUE(c2.open(sess, &err)) << err;
  std::string v;
  ASSERT_TRUE(eng2.storage()->get("replay_k", &v));
  EXPECT_EQ(v, "from_journal");
  eng2.shutdown();
}

TEST(EmbedClient, OpenFailsOnJournalBadSeqField) {
  const auto root = temp_dir("embed_j_badseq");
  std::filesystem::remove_all(root);
  const auto art = root / "sess" / structdb::client::kEmbedSessionArtifactsDir;
  std::filesystem::create_directories(art);
  {
    std::ofstream j(art / "session.journal", std::ios::binary | std::ios::trunc);
    j << "not_a_u64\t\t0\t\t0\n";
  }
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient c(eng);
  ASSERT_FALSE(c.open(root / "sess", &err)) << err;
  EXPECT_FALSE(err.empty());
  eng.shutdown();
}

TEST(EmbedClient, OpenFailsOnJournalInvalidPutFieldCount) {
  const auto root = temp_dir("embed_j_badcnt");
  std::filesystem::remove_all(root);
  const auto art = root / "sess" / structdb::client::kEmbedSessionArtifactsDir;
  std::filesystem::create_directories(art);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  ASSERT_EQ(0u, eng.storage()->wal_log_bytes_on_disk())
      << "journal apply path requires empty WAL so recovery does not skip apply_fields";
  {
    std::ofstream j(art / "session.journal", std::ios::binary | std::ios::trunc);
    // seq must be > default last_ack (0), otherwise `seq <= last_ack_` skips apply_fields.
    j << "1\t\t99\ta\tb\t\t0\n";
  }
  structdb::client::EmbedClient c(eng);
  ASSERT_FALSE(c.open(root / "sess", &err)) << err;
  EXPECT_NE(err.find("field count"), std::string::npos) << err;
  eng.shutdown();
}

TEST(EmbedClient, OpenFailsOnTruncatedJournalSecondLine) {
  const auto root = temp_dir("embed_j_trunc2");
  std::filesystem::remove_all(root);
  const auto art = root / "sess" / structdb::client::kEmbedSessionArtifactsDir;
  std::filesystem::create_directories(art);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  ASSERT_EQ(0u, eng.storage()->wal_log_bytes_on_disk());
  {
    std::ofstream j(art / "session.journal", std::ios::binary | std::ios::trunc);
    j << "0\tok\t1\tk0\tv0\t\t0\n";
    j << "1\tok2\t1\tk1";  // truncated: missing value + trailing fields
  }
  structdb::client::EmbedClient c(eng);
  ASSERT_FALSE(c.open(root / "sess", &err)) << err;
  EXPECT_FALSE(err.empty());
  eng.shutdown();
}

TEST(EmbedClient, OpenFailsWhenSessionCkptSecondLineNotU64) {
  const auto root = temp_dir("embed_ckpt_bad_mv");
  std::filesystem::remove_all(root);
  {
    structdb::facade::Engine eng;
    structdb::facade::EngineConfigSnapshot snap;
    snap.data_dir = (root / "_data").string();
    eng.config().update(1, snap);
    std::string err;
    ASSERT_TRUE(eng.startup(&err)) << err;
    structdb::client::EmbedClient c(eng);
    ASSERT_TRUE(c.open(root / "sess", &err)) << err;
    structdb::client::CommandBatch b;
    b.idempotency_token = "ck1";
    b.puts.push_back({"ckk", "1"});
    ASSERT_TRUE(c.submit(b, false, &err)) << err;
    ASSERT_TRUE(c.save_checkpoint(&err)) << err;
    c.close();
    const auto ck = root / "sess" / structdb::client::kEmbedSessionArtifactsDir / "session.ckpt";
    std::ofstream wreck(ck, std::ios::trunc);
    wreck << "0\nnot_manifest_u64\n0\n";
    structdb::client::EmbedClient c2(eng);
    ASSERT_FALSE(c2.open(root / "sess", &err)) << err;
    EXPECT_FALSE(err.empty());
    eng.shutdown();
  }
}

TEST(EmbedClient, OpenFailsWhenSessionCkptThirdLineNotU64) {
  const auto root = temp_dir("embed_ckpt_bad_cs");
  std::filesystem::remove_all(root);
  {
    structdb::facade::Engine eng;
    structdb::facade::EngineConfigSnapshot snap;
    snap.data_dir = (root / "_data").string();
    eng.config().update(1, snap);
    std::string err;
    ASSERT_TRUE(eng.startup(&err)) << err;
    structdb::client::EmbedClient c(eng);
    ASSERT_TRUE(c.open(root / "sess", &err)) << err;
    structdb::client::CommandBatch b;
    b.idempotency_token = "ck2";
    b.puts.push_back({"ckk2", "1"});
    ASSERT_TRUE(c.submit(b, false, &err)) << err;
    ASSERT_TRUE(c.save_checkpoint(&err)) << err;
    c.close();
    const auto ck = root / "sess" / structdb::client::kEmbedSessionArtifactsDir / "session.ckpt";
    std::ofstream wreck(ck, std::ios::trunc);
    wreck << "0\n0\nnot_checkpoint_seq\n";
    structdb::client::EmbedClient c2(eng);
    ASSERT_FALSE(c2.open(root / "sess", &err)) << err;
    EXPECT_FALSE(err.empty());
    eng.shutdown();
  }
}

TEST(EmbedClient, NestedDelThenPutBatchSurvivesJournalReplayAfterWalTrim) {
  const auto root = temp_dir("embed_nested_del_put");
  std::filesystem::remove_all(root);
  const auto sess = root / "sess";
  {
    structdb::facade::Engine eng;
    structdb::facade::EngineConfigSnapshot snap;
    snap.data_dir = (root / "_data").string();
    snap.wal_auto_trim_prefix_after_flush = true;
    eng.config().update(1, snap);
    std::string err;
    ASSERT_TRUE(eng.startup(&err)) << err;
    structdb::client::EmbedClient c(eng);
    ASSERT_TRUE(c.open(sess, &err)) << err;
    structdb::client::CommandBatch seed;
    seed.idempotency_token = "seed";
    seed.puts.push_back({"nest_hold", "keep"});
    ASSERT_TRUE(c.submit(seed, false, &err)) << err;
    structdb::client::CommandBatch mix;
    mix.idempotency_token = "nest_mix";
    mix.dels.push_back("nest_hold");
    mix.puts.push_back({"nest_new", "after_del"});
    ASSERT_TRUE(c.submit(mix, false, &err)) << err;
    ASSERT_TRUE(eng.storage()->flush_memtable(&err)) << err;
    EXPECT_EQ(eng.storage()->wal_log_bytes_on_disk(), 0u);
    c.close();
    eng.shutdown();
  }
  structdb::facade::Engine eng2;
  structdb::facade::EngineConfigSnapshot snap2;
  snap2.data_dir = (root / "_data").string();
  eng2.config().update(2, snap2);
  std::string err;
  ASSERT_TRUE(eng2.startup(&err)) << err;
  structdb::client::EmbedClient c2(eng2);
  ASSERT_TRUE(c2.open(sess, &err)) << err;
  std::string v;
  ASSERT_FALSE(eng2.storage()->get("nest_hold", &v));
  ASSERT_TRUE(eng2.storage()->get("nest_new", &v));
  EXPECT_EQ(v, "after_del");
  eng2.shutdown();
}

TEST(StorageEngine, WalReplayImportRawBatch) {
  const auto dir = temp_dir("wal_replay_import_raw");
  std::filesystem::remove_all(dir);
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  st.set_import_batch_skip_undo(true);
  st.set_import_store_raw_logical(true);
  namespace mk = structdb::storage::mdb_keyspace;
  const std::string key = mk::row_key("t", "1");
  const std::string logical = "1\t1";
  const std::vector<std::string> dels;
  const std::vector<std::pair<std::string, std::string>> puts{{key, logical}};
  ASSERT_TRUE(st.commit_embed_batch(dels, puts, true, &err)) << err;
  st.close();

  structdb::storage::StorageEngine st2(dir / "d");
  ASSERT_TRUE(st2.open(&err)) << err;
  std::string got;
  ASSERT_TRUE(st2.get(key, &got, (std::numeric_limits<std::uint64_t>::max)()));
  EXPECT_EQ(got, logical);
  st2.close();
}

TEST(StorageEngine, WalReplayIgnoresTailByteAfterChunkedMdbEmbedBatch) {
  const auto dir = temp_dir("wal_mdb_chunk_tail");
  std::filesystem::remove_all(dir);
  const auto wal_path = dir / "d" / "wal.log";
  {
    structdb::storage::StorageEngine st(dir / "d");
    std::string err;
    ASSERT_TRUE(st.open(&err)) << err;
    st.set_embed_batch_max_frame_bytes(256);
    std::vector<std::pair<std::string, std::string>> puts;
    for (int i = 0; i < 40; ++i) {
      puts.emplace_back("mdb$v2$row$tail$" + std::to_string(i), std::string(32, 'v'));
    }
    const std::vector<std::string> dels;
    ASSERT_TRUE(st.commit_embed_batch(dels, puts, false, &err)) << err;
    st.close();
  }
  std::string wal_bytes;
  {
    std::ifstream in(wal_path, std::ios::binary);
    ASSERT_TRUE(in.is_open());
    wal_bytes.assign((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
  }
  ASSERT_FALSE(wal_bytes.empty());
  wal_bytes.push_back(static_cast<char>(0x7f));
  {
    std::ofstream out(wal_path, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(out.is_open());
    out.write(wal_bytes.data(), static_cast<std::streamsize>(wal_bytes.size()));
  }
  structdb::storage::StorageEngine st2(dir / "d");
  std::string err;
  ASSERT_TRUE(st2.open(&err)) << err;
  std::string v;
  ASSERT_TRUE(st2.get("mdb$v2$row$tail$0", &v));
  EXPECT_EQ(v, std::string(32, 'v'));
  st2.close();
}

TEST(StorageEngine, WalReplayRejectsCorruptAfterChunkedMdbEmbedBatch) {
  const auto dir = temp_dir("wal_mdb_chunk_corrupt");
  std::filesystem::remove_all(dir);
  const auto wal_path = dir / "d" / "wal.log";
  {
    structdb::storage::StorageEngine st(dir / "d");
    std::string err;
    ASSERT_TRUE(st.open(&err)) << err;
    st.set_embed_batch_max_frame_bytes(256);
    std::vector<std::pair<std::string, std::string>> puts;
    for (int i = 0; i < 24; ++i) {
      puts.emplace_back("mdb$v2$row$bad$" + std::to_string(i), "x");
    }
    const std::vector<std::string> dels;
    ASSERT_TRUE(st.commit_embed_batch(dels, puts, false, &err)) << err;
    st.close();
  }
  {
    std::fstream wf(wal_path, std::ios::binary | std::ios::in | std::ios::out | std::ios::ate);
    ASSERT_TRUE(wf.is_open());
    const std::uint32_t le = 3;
    wf.write(reinterpret_cast<const char*>(&le), sizeof(le));
    wf.write("xx\n", 3);
    ASSERT_TRUE(static_cast<bool>(wf));
  }
  structdb::storage::StorageEngine st2(dir / "d");
  std::string err;
  ASSERT_FALSE(st2.open(&err));
  EXPECT_NE(err.find("missing '='"), std::string::npos) << err;
}

TEST(StorageEngine, WalReplayImportRawBatchAfterAutoSplit) {
  const auto dir = temp_dir("wal_replay_import_raw_split");
  std::filesystem::remove_all(dir);
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  st.set_import_batch_skip_undo(true);
  st.set_import_store_raw_logical(true);
  st.set_embed_batch_max_frame_bytes(256);
  namespace mk = structdb::storage::mdb_keyspace;
  std::vector<std::pair<std::string, std::string>> puts;
  puts.reserve(60);
  for (int i = 0; i < 60; ++i) {
    const std::string id = std::to_string(i);
    puts.emplace_back(mk::row_key("t", id), id + "\t" + id);
  }
  const std::vector<std::string> dels;
  ASSERT_TRUE(st.commit_embed_batch(dels, puts, true, &err)) << err;
  st.close();

  structdb::storage::StorageEngine st2(dir / "d");
  ASSERT_TRUE(st2.open(&err)) << err;
  for (int i = 0; i < 60; ++i) {
    const std::string id = std::to_string(i);
    const std::string key = mk::row_key("t", id);
    std::string got;
    ASSERT_TRUE(st2.get(key, &got, (std::numeric_limits<std::uint64_t>::max)())) << i;
    EXPECT_EQ(got, id + "\t" + id) << i;
  }
  st2.close();
}

TEST(StorageEngine, CommitEmbedBatchAutoSplitByFrameBytes) {
  const auto dir = temp_dir("embed_batch_split");
  std::filesystem::remove_all(dir);
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  st.set_embed_batch_max_frame_bytes(512);
  std::vector<std::pair<std::string, std::string>> puts;
  puts.reserve(80);
  for (int i = 0; i < 80; ++i) {
    puts.emplace_back("mdb$v2$row$split$" + std::to_string(i), std::string(48, 'v'));
  }
  const std::vector<std::string> dels;
  ASSERT_TRUE(st.commit_embed_batch(dels, puts, true, &err)) << err;
  st.close();

  std::ifstream wal(dir / "d" / "wal.log", std::ios::binary);
  ASSERT_TRUE(wal.good());
  int stdbbw1_frames = 0;
  for (;;) {
    std::uint32_t len = 0;
    if (!wal.read(reinterpret_cast<char*>(&len), sizeof(len))) break;
    std::string body(len, '\0');
    if (!wal.read(body.data(), static_cast<std::streamsize>(len))) break;
    if (body.size() >= 7 && body.compare(0, 7, "STDBBW1") == 0) ++stdbbw1_frames;
  }
  EXPECT_GT(stdbbw1_frames, 1) << stdbbw1_frames;
}

TEST(StorageEngine, CommitEmbedBatchEmptyIsNoop) {
  const auto dir = temp_dir("embed_batch_empty");
  std::filesystem::remove_all(dir);
  structdb::storage::StorageEngine st(dir / "d");
  std::string err;
  ASSERT_TRUE(st.open(&err)) << err;
  const std::vector<std::string> dels;
  const std::vector<std::pair<std::string, std::string>> puts;
  ASSERT_TRUE(st.commit_embed_batch(dels, puts, false, &err));
  ASSERT_TRUE(st.commit_embed_batch(dels, puts, true, &err));
  st.close();
}

TEST(EmbedClient, SubmitRejectsTabInIdempotencyToken) {
  const auto root = temp_dir("embed_bad_token");
  std::filesystem::remove_all(root);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient c(eng);
  ASSERT_TRUE(c.open(root / "sess", &err)) << err;
  structdb::client::CommandBatch b;
  b.idempotency_token = "bad\ttab";
  b.puts.push_back({"k", "v"});
  ASSERT_FALSE(c.submit(b, false, &err));
  EXPECT_NE(err.find("unsafe"), std::string::npos) << err;
  c.close();
  eng.shutdown();
}

TEST(Engine, NestedWalThrottleLowBpsMultiPutMonotonicCounters) {
  const auto dir = temp_dir("eng_nested_wal_th");
  std::filesystem::remove_all(dir);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (dir / "_data").string();
  snap.wal_append_max_bytes_per_second = 12000;
  snap.wal_append_burst_bytes = 4096;
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  for (int i = 0; i < 4; ++i) {
    ASSERT_TRUE(eng.kv_put(std::string("nth_") + std::to_string(i), std::string(3000, static_cast<char>('0' + i)), false))
        << i;
  }
  structdb::storage::StoragePressureSnapshot p{};
  eng.storage_pressure_snapshot(&p);
  EXPECT_GE(p.wal_append_frame_bytes_committed_total, 4u * 8u);
  EXPECT_GT(p.wal_append_throttle_sleep_ns_total, 0u);
  eng.shutdown();
}

TEST(EmbedClient, RecoverWhenNotOpenReturnsFalse) {
  const auto root = temp_dir("embed_recover_closed");
  std::filesystem::remove_all(root);
  structdb::facade::Engine eng;
  structdb::facade::EngineConfigSnapshot snap;
  snap.data_dir = (root / "_data").string();
  eng.config().update(1, snap);
  std::string err;
  ASSERT_TRUE(eng.startup(&err)) << err;
  structdb::client::EmbedClient c(eng);
  ASSERT_FALSE(c.recover(&err));
  EXPECT_NE(err.find("session not open"), std::string::npos) << err;
  eng.shutdown();
}

// --- MemTable backend × persistence path (flush vs WAL-only) semantic matrix ---

TEST(StorageEngine, MemTableBackendFlushVsWalOnlyRestartSemanticMatrix) {
  using structdb::storage::MemTableBackend;
  for (const auto backend : {MemTableBackend::Map, MemTableBackend::SkipList}) {
    for (const bool flush_before_close : {true, false}) {
      const char* btag = (backend == MemTableBackend::Map) ? "map" : "skl";
      const char* ptag = flush_before_close ? "flush" : "walonly";
      const std::string dir_name = std::string("mem_mtx_") + btag + "_" + ptag;
      const auto dir = temp_dir(dir_name.c_str());
      std::filesystem::remove_all(dir);
      {
        structdb::storage::StorageEngine st(dir / "d");
        st.set_memtable_backend(backend);
        std::string err;
        ASSERT_TRUE(st.open(&err)) << btag << " " << ptag << " " << err;
        ASSERT_TRUE(st.put("mtx_k", "mtx_v", false)) << btag << " " << ptag;
        ASSERT_TRUE(st.put("mtx_k2", "second", false)) << btag << " " << ptag;
        if (flush_before_close) {
          ASSERT_TRUE(st.flush_memtable(&err)) << btag << " " << ptag << " " << err;
        }
        st.close();
      }
      {
        structdb::storage::StorageEngine st2(dir / "d");
        st2.set_memtable_backend(backend);
        std::string err;
        ASSERT_TRUE(st2.open(&err)) << btag << " " << ptag << " " << err;
        std::string v;
        ASSERT_TRUE(st2.get("mtx_k", &v)) << btag << " " << ptag;
        EXPECT_EQ(v, "mtx_v") << btag << " " << ptag;
        ASSERT_TRUE(st2.get("mtx_k2", &v)) << btag << " " << ptag;
        EXPECT_EQ(v, "second") << btag << " " << ptag;
        st2.close();
      }
    }
  }
}

// --- WAL failure path: a second framed record that fails text-line parse (no '=') ---

TEST(StorageEngine, WalReplayRejectsMissingEqualsLineAfterValidEmbedBatch) {
  const auto dir = temp_dir("wal_bad_line_after_batch");
  std::filesystem::remove_all(dir);
  const auto wal_path = dir / "d" / "wal.log";
  {
    structdb::storage::StorageEngine st(dir / "d");
    std::string err;
    ASSERT_TRUE(st.open(&err)) << err;
    const std::vector<std::string> dels;
    const std::vector<std::pair<std::string, std::string>> puts{{"ok_line", "1"}};
    ASSERT_TRUE(st.commit_embed_batch(dels, puts, false, &err)) << err;
    st.close();
  }
  {
    std::fstream wf(wal_path, std::ios::binary | std::ios::in | std::ios::out | std::ios::ate);
    ASSERT_TRUE(wf.is_open());
    const std::uint32_t le = 3;
    wf.write(reinterpret_cast<const char*>(&le), sizeof(le));
    wf.write("xx\n", 3);
    ASSERT_TRUE(static_cast<bool>(wf));
  }
  structdb::storage::StorageEngine st2(dir / "d");
  std::string err;
  ASSERT_FALSE(st2.open(&err)) << err;
  EXPECT_NE(err.find("missing '='"), std::string::npos) << err;
  st2.close();
}

// Crash-tolerance: bytes after the last complete length-prefixed WAL frame are
// treated as an incomplete tail and ignored (see `wal_replay_from_offset`).
TEST(StorageEngine, WalReplayIgnoresUnframedTrailingByteAfterValidEmbedBatch) {
  const auto dir = temp_dir("wal_unframed_tail_ign");
  std::filesystem::remove_all(dir);
  const auto wal_path = dir / "d" / "wal.log";
  {
    structdb::storage::StorageEngine st(dir / "d");
    std::string err;
    ASSERT_TRUE(st.open(&err)) << err;
    const std::vector<std::string> dels;
    const std::vector<std::pair<std::string, std::string>> puts{{"tail_ok", "1"}};
    ASSERT_TRUE(st.commit_embed_batch(dels, puts, false, &err)) << err;
    st.close();
  }
  {
    std::fstream wf(wal_path, std::ios::binary | std::ios::in | std::ios::out | std::ios::ate);
    ASSERT_TRUE(wf.is_open());
    const char junk = static_cast<char>(0x7f);
    wf.write(&junk, 1);
    ASSERT_TRUE(static_cast<bool>(wf));
  }
  structdb::storage::StorageEngine st2(dir / "d");
  std::string err;
  ASSERT_TRUE(st2.open(&err)) << err;
  std::string v;
  ASSERT_TRUE(st2.get("tail_ok", &v));
  EXPECT_EQ(v, "1");
  st2.close();
}

// --- Nested embed batches + simulated crash (no explicit flush of last op) ---

TEST(EmbedClient, TripleNestedBatchSequenceSurvivesRestartWithoutFinalFlush) {
  const auto root = temp_dir("embed_triple_nested_no_flush");
  std::filesystem::remove_all(root);
  const auto sess = root / "sess";
  {
    structdb::facade::Engine eng;
    structdb::facade::EngineConfigSnapshot snap;
    snap.data_dir = (root / "_data").string();
    eng.config().update(1, snap);
    std::string err;
    ASSERT_TRUE(eng.startup(&err)) << err;
    structdb::client::EmbedClient c(eng);
    ASSERT_TRUE(c.open(sess, &err)) << err;
    structdb::client::CommandBatch b0;
    b0.idempotency_token = "nest0";
    b0.puts.push_back({"layer0", "L0"});
    ASSERT_TRUE(c.submit(b0, false, &err)) << err;
    structdb::client::CommandBatch b1;
    b1.idempotency_token = "nest1";
    b1.puts.push_back({"layer1", "L1"});
    b1.dels.push_back("layer0");
    ASSERT_TRUE(c.submit(b1, false, &err)) << err;
    structdb::client::CommandBatch b2;
    b2.idempotency_token = "nest2";
    b2.puts.push_back({"layer2", "L2"});
    b2.puts.push_back({"layer0", "L0_restored"});
    ASSERT_TRUE(c.submit(b2, false, &err)) << err;
    EXPECT_GT(eng.storage()->wal_log_bytes_on_disk(), 0u);
    c.close();
    eng.shutdown();
  }
  structdb::facade::Engine eng2;
  structdb::facade::EngineConfigSnapshot snap2;
  snap2.data_dir = (root / "_data").string();
  eng2.config().update(2, snap2);
  std::string err;
  ASSERT_TRUE(eng2.startup(&err)) << err;
  structdb::client::EmbedClient c2(eng2);
  ASSERT_TRUE(c2.open(sess, &err)) << err;
  std::string v;
  ASSERT_TRUE(eng2.storage()->get("layer0", &v));
  EXPECT_EQ(v, "L0_restored");
  ASSERT_TRUE(eng2.storage()->get("layer1", &v));
  EXPECT_EQ(v, "L1");
  ASSERT_TRUE(eng2.storage()->get("layer2", &v));
  EXPECT_EQ(v, "L2");
  eng2.shutdown();
}

int main(int argc, char** argv) {
  structdb::infra::install_spdlog_default();
  gtest_capi_init_from_argv(argc, reinterpret_cast<const char* const*>(argv));
  return gtest_capi_run_all();
}
