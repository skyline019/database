#include "cli/modules/txn/coordinator/txn_manager.h"

#include <newdb/page_io.h>
#include <newdb/wal_sync_mode.h>
#include <newdb/wal_manager.h>

#include "test_util.h"

#include <gtest/gtest.h>
#include <chrono>
#include <thread>
#include <cstdlib>
#include <vector>

namespace {
void set_env_value(const char* key, const char* value) {
#if defined(_WIN32)
    _putenv_s(key, value ? value : "");
#else
    if (value == nullptr) {
        unsetenv(key);
    } else {
        setenv(key, value, 1);
    }
#endif
}

int env_int_or(const char* key, const int defv) {
    const char* raw = std::getenv(key);
    if (raw == nullptr || raw[0] == '\0') {
        return defv;
    }
    try {
        const int v = std::stoi(raw);
        return v > 0 ? v : defv;
    } catch (...) {
        return defv;
    }
}

/** Clears hybrid self-test env on scope exit (avoids flakes when a prior EXPECT fails mid-test). */
struct HybridTestEnvGuard {
    ~HybridTestEnvGuard() {
        set_env_value("NEWDB_HYBRID_MIN_DWELL_MS", nullptr);
        set_env_value("NEWDB_HYBRID_TEST_QUEUE_DEPTH", nullptr);
        set_env_value("NEWDB_HYBRID_TEST_RECOVERY_TAIL_MS", nullptr);
        set_env_value("NEWDB_HYBRID_TEST_LOCK_TAIL_MS", nullptr);
    }
};

/** Wait until `min_dwell_ms` has passed on steady_clock (matches coordinator), plus CI margin. */
void sleep_hybrid_dwell_elapsed(int min_dwell_ms) {
    const auto start = std::chrono::steady_clock::now();
    const auto need = std::chrono::milliseconds(min_dwell_ms + 280);
    while (std::chrono::steady_clock::now() - start < need) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
}

bool run_wal_crash_point_and_expect_failure(const std::string& point) {
    const auto dir = newdb::test::unique_temp_subdir(std::string("newdb_txn_wal_crash_matrix_") + point);
    std::filesystem::create_directories(dir);
    newdb::WalManager wm("demodb", dir.string());
    if (!wm.open().ok) {
        return false;
    }
    // Default WAL sync is Full (fsync per flush). Under parallel `ctest`, many tests flushing
    // concurrently can push this matrix past CI timeouts; injection points still hit fflush/write/rotate.
    wm.set_sync_mode(newdb::WalSyncMode::Off);
    wm.set_segment_max_bytes(64); // ensure rotate path is reachable in matrix

    set_env_value("NEWDB_WAL_CRASH_POINT", point.c_str());
    newdb::Row before;
    before.id = 11;
    before.attrs["n"] = "before";
    newdb::Row after;
    after.id = 11;
    after.attrs["n"] = "after";

    bool failed = false;
    if (point.rfind("commit_", 0) == 0) {
        failed = !wm.append_record(100, newdb::WalOp::UPDATE, "users", nullptr, nullptr, &before, &after,
                                   0, 0, 0, 0, 0, 1).ok;
    } else if (point.rfind("flush_", 0) == 0) {
        if (!wm.append_record(100, newdb::WalOp::UPDATE, "users", nullptr, nullptr, &before, &after,
                              0, 0, 0, 0, 0, 1).ok) {
            set_env_value("NEWDB_WAL_CRASH_POINT", nullptr);
            return false;
        }
        failed = !wm.flush().ok;
    } else if (point == "checkpoint_between_begin_end") {
        failed = !wm.checkpoint_and_truncate(wm.current_lsn()).ok;
    } else if (point.rfind("rotate_", 0) == 0) {
        std::vector<std::uint8_t> payload(256, static_cast<std::uint8_t>('x'));
        failed = !wm.append_record(100, newdb::WalOp::INSERT, "users", &after, &payload, nullptr, &after,
                                   0, 0, 0, 0, 0, 2).ok;
    }
    set_env_value("NEWDB_WAL_CRASH_POINT", nullptr);
    return failed;
}
}

TEST(DemoTxnWal, RecoveryIsIdempotentAcrossRestarts) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_txn_wal");
    std::filesystem::create_directories(dir);
    const std::string bin = (dir / "users.bin").string();
    ASSERT_TRUE(newdb::io::create_heap_file(bin.c_str(), {newdb::Row{1, {{"n", "a"}}, {}}}).ok);

    TxnCoordinator tx;
    tx.set_workspace_root(dir.string());
    ASSERT_TRUE(tx.begin("users").isOk());
    tx.recordOperation("INSERT", "users", "2", "", "");
    ASSERT_TRUE(newdb::io::append_row(bin.c_str(), newdb::Row{2, {{"n", "b"}}, {}}).ok);
    tx.flushWAL();

    ASSERT_TRUE(tx.recoverFromWAL());
    {
        const TxnRuntimeStats stats = tx.runtimeStats();
        EXPECT_EQ(stats.wal_recovery_runs, 1u);
        EXPECT_GE(stats.wal_recovery_undo_ops, 1u);
        EXPECT_GE(stats.wal_recovery_last_elapsed_ms, 0u);
    }
    {
        newdb::HeapTable tbl;
        newdb::TableSchema sch;
        sch.primary_key = "id";
        ASSERT_TRUE(newdb::io::load_heap_file(bin.c_str(), "users", sch, tbl).ok);
        EXPECT_EQ(tbl.find_by_id(2), nullptr);
    }

    newdb::WalManager wm("demodb", dir.string());
    ASSERT_TRUE(wm.open().ok);
    std::vector<newdb::WalDecodedRecord> recs1;
    ASSERT_TRUE(wm.read_all_records(newdb::TableSchema{}, recs1).ok);
    std::size_t r1 = 0;
    for (const auto& r : recs1) {
        if (r.op == newdb::WalOp::ROLLBACK) {
            ++r1;
        }
    }

    ASSERT_TRUE(tx.recoverFromWAL());
    {
        const TxnRuntimeStats stats = tx.runtimeStats();
        EXPECT_EQ(stats.wal_recovery_runs, 2u);
        EXPECT_GE(stats.wal_recovery_undo_ops, 1u);
        EXPECT_GE(stats.wal_recovery_last_elapsed_ms, 0u);
    }
    std::vector<newdb::WalDecodedRecord> recs2;
    ASSERT_TRUE(wm.read_all_records(newdb::TableSchema{}, recs2).ok);
    std::size_t r2 = 0;
    for (const auto& r : recs2) {
        if (r.op == newdb::WalOp::ROLLBACK) {
            ++r2;
        }
    }

    EXPECT_EQ(r2, r1);
}

TEST(DemoTxnWal, CommittedTxnSurvivesRecovery) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_txn_wal_committed");
    std::filesystem::create_directories(dir);
    const std::string bin = (dir / "users.bin").string();
    ASSERT_TRUE(newdb::io::create_heap_file(bin.c_str(), {newdb::Row{1, {{"n", "a"}}, {}}}).ok);

    TxnCoordinator tx;
    tx.set_workspace_root(dir.string());
    ASSERT_TRUE(tx.begin("users").isOk());
    tx.recordOperation("INSERT", "users", "2", "", "");
    ASSERT_TRUE(newdb::io::append_row(bin.c_str(), newdb::Row{2, {{"n", "b"}}, {}}).ok);
    ASSERT_TRUE(tx.commit().isOk());
    tx.flushWAL();

    ASSERT_TRUE(tx.recoverFromWAL());
    const TxnRuntimeStats stats = tx.runtimeStats();
    EXPECT_GE(stats.wal_recovery_runs, 1u);
    EXPECT_GE(stats.wal_recovery_last_elapsed_ms, 0u);

    newdb::HeapTable tbl;
    newdb::TableSchema sch;
    sch.primary_key = "id";
    ASSERT_TRUE(newdb::io::load_heap_file(bin.c_str(), "users", sch, tbl).ok);
    ASSERT_NE(tbl.find_by_id(2), nullptr);
}

TEST(DemoTxnWal, FullSyncAndGroupCommitMatrixIsConfigurable) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_txn_wal_profile");
    std::filesystem::create_directories(dir);
    const std::string bin = (dir / "users.bin").string();
    ASSERT_TRUE(newdb::io::create_heap_file(bin.c_str(), {newdb::Row{1, {{"n", "a"}}, {}}}).ok);

    TxnCoordinator tx;
    tx.set_workspace_root(dir.string());
    tx.setWalSyncMode(newdb::WalSyncMode::Full);
    tx.setGroupCommitWindowMs(0);
    tx.setGroupCommitMaxBatchCommits(1);

    ASSERT_TRUE(tx.begin("users").isOk());
    tx.recordOperation("INSERT", "users", "2", "", "");
    ASSERT_TRUE(newdb::io::append_row(bin.c_str(), newdb::Row{2, {{"n", "b"}}, {}}).ok);
    ASSERT_TRUE(tx.commit().isOk());
    tx.flushWAL();

    EXPECT_EQ(tx.walSyncMode(), newdb::WalSyncMode::Full);
    EXPECT_EQ(tx.groupCommitWindowMs(), 0u);
    EXPECT_EQ(tx.groupCommitMaxBatchCommits(), 1u);
    const TxnRuntimeStats stats = tx.runtimeStats();
    EXPECT_GE(stats.wal_group_commit_count, 1u);
    EXPECT_GE(stats.wal_group_commit_batch_commits, 1u);
}

TEST(DemoTxnWal, FullSyncClampsGroupCommitWindowAndBatch) {
    TxnCoordinator tx;
    tx.setWalSyncMode(newdb::WalSyncMode::Full);
    tx.setGroupCommitWindowMs(100);
    tx.setGroupCommitMaxBatchCommits(16);
    EXPECT_EQ(tx.groupCommitWindowMs(), 0u);
    EXPECT_EQ(tx.groupCommitMaxBatchCommits(), 1u);
}

TEST(DemoTxnWal, CheckpointTruncateKeepsRecoveryConsistent) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_txn_wal_checkpoint");
    std::filesystem::create_directories(dir);
    const std::string bin = (dir / "users.bin").string();
    ASSERT_TRUE(newdb::io::create_heap_file(bin.c_str(), {newdb::Row{1, {{"n", "a"}}, {}}}).ok);
    TxnCoordinator tx;
    tx.set_workspace_root(dir.string());
    ASSERT_TRUE(tx.begin("users").isOk());
    tx.recordOperation("INSERT", "users", "2", "", "");
    ASSERT_TRUE(newdb::io::append_row(bin.c_str(), newdb::Row{2, {{"n", "b"}}, {}}).ok);
    ASSERT_TRUE(tx.commit().isOk());
    tx.flushWAL();

    newdb::WalManager wm("demodb", dir.string());
    ASSERT_TRUE(wm.open().ok);
    ASSERT_TRUE(wm.checkpoint_and_truncate(wm.current_lsn()).ok);
    wm.close();

    ASSERT_TRUE(tx.recoverFromWAL());
    newdb::HeapTable tbl;
    newdb::TableSchema sch;
    sch.primary_key = "id";
    ASSERT_TRUE(newdb::io::load_heap_file(bin.c_str(), "users", sch, tbl).ok);
    ASSERT_NE(tbl.find_by_id(2), nullptr);
}

TEST(DemoTxnWal, HybridAdaptiveDwellWindowDebouncesModeFlip) {
    HybridTestEnvGuard hybrid_env_guard;
    const auto dir = newdb::test::unique_temp_subdir("newdb_txn_wal_hybrid_flip");
    std::filesystem::create_directories(dir);
    TxnCoordinator tx;
    tx.set_workspace_root(dir.string());
    tx.setHybridAdaptiveEnabled(true);
    tx.setWalSyncMode(newdb::WalSyncMode::Normal);

    constexpr int kDwellMs = 200;
    set_env_value("NEWDB_HYBRID_MIN_DWELL_MS", "200");
    set_env_value("NEWDB_HYBRID_TEST_QUEUE_DEPTH", "0");
    set_env_value("NEWDB_HYBRID_TEST_RECOVERY_TAIL_MS", "700");
    set_env_value("NEWDB_HYBRID_TEST_LOCK_TAIL_MS", "0");

    tx.flushWAL(); // throughput -> durability (switch #1)
    auto s = tx.runtimeStats();
    EXPECT_EQ(s.hybrid_mode, "durability_mode");
    EXPECT_EQ(s.hybrid_mode_switch_count, 1u);

    set_env_value("NEWDB_HYBRID_TEST_QUEUE_DEPTH", "10");
    set_env_value("NEWDB_HYBRID_TEST_RECOVERY_TAIL_MS", "0");
    tx.flushWAL(); // within dwell, should not switch
    s = tx.runtimeStats();
    EXPECT_EQ(s.hybrid_mode, "durability_mode");
    EXPECT_EQ(s.hybrid_mode_switch_count, 1u);

    sleep_hybrid_dwell_elapsed(kDwellMs);
    tx.flushWAL(); // dwell passed, durability -> throughput (switch #2)
    s = tx.runtimeStats();
    EXPECT_EQ(s.hybrid_mode, "throughput_mode");
    EXPECT_EQ(s.hybrid_mode_switch_count, 2u);
    EXPECT_EQ(s.hybrid_last_switch_reason, "queue_backpressure");

    set_env_value("NEWDB_HYBRID_TEST_QUEUE_DEPTH", "0");
    set_env_value("NEWDB_HYBRID_TEST_RECOVERY_TAIL_MS", "700");
    tx.flushWAL(); // within dwell, should not switch
    s = tx.runtimeStats();
    EXPECT_EQ(s.hybrid_mode, "throughput_mode");
    EXPECT_EQ(s.hybrid_mode_switch_count, 2u);

    sleep_hybrid_dwell_elapsed(kDwellMs);
    tx.flushWAL(); // dwell passed, throughput -> durability (switch #3)
    s = tx.runtimeStats();
    EXPECT_EQ(s.hybrid_mode, "durability_mode");
    EXPECT_EQ(s.hybrid_mode_switch_count, 3u);
    EXPECT_EQ(s.hybrid_last_switch_reason, "recovery_or_lock_tail");
}

TEST(DemoTxnWal, HybridAdaptiveAlternatingSignalsAreCappedByDwellWindow) {
    HybridTestEnvGuard hybrid_env_guard;
    const auto dir = newdb::test::unique_temp_subdir("newdb_txn_wal_hybrid_flip_cap");
    std::filesystem::create_directories(dir);
    TxnCoordinator tx;
    tx.set_workspace_root(dir.string());
    tx.setHybridAdaptiveEnabled(true);
    tx.setWalSyncMode(newdb::WalSyncMode::Normal);

    // Large dwell to ensure high-frequency alternating signals cannot trigger repeated flips.
    set_env_value("NEWDB_HYBRID_MIN_DWELL_MS", "60000");

    // First signal forces durability mode once.
    set_env_value("NEWDB_HYBRID_TEST_QUEUE_DEPTH", "0");
    set_env_value("NEWDB_HYBRID_TEST_RECOVERY_TAIL_MS", "700");
    set_env_value("NEWDB_HYBRID_TEST_LOCK_TAIL_MS", "0");
    tx.flushWAL();
    auto s = tx.runtimeStats();
    ASSERT_EQ(s.hybrid_mode, "durability_mode");
    ASSERT_EQ(s.hybrid_mode_switch_count, 1u);

    const int rounds = env_int_or("NEWDB_HYBRID_SOAK_ROUNDS", 100);
    for (int i = 0; i < rounds; ++i) {
        if ((i % 2) == 0) {
            set_env_value("NEWDB_HYBRID_TEST_QUEUE_DEPTH", "10");
            set_env_value("NEWDB_HYBRID_TEST_RECOVERY_TAIL_MS", "0");
        } else {
            set_env_value("NEWDB_HYBRID_TEST_QUEUE_DEPTH", "0");
            set_env_value("NEWDB_HYBRID_TEST_RECOVERY_TAIL_MS", "700");
        }
        tx.flushWAL();
    }

    s = tx.runtimeStats();
    EXPECT_EQ(s.hybrid_mode, "durability_mode");
    EXPECT_EQ(s.hybrid_mode_switch_count, 1u);
}

TEST(DemoTxnWal, WalV1PayloadCarriesBeforeAfterAndOpSeq) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_txn_wal_v1_payload");
    std::filesystem::create_directories(dir);
    newdb::WalManager wm("demodb", dir.string());
    ASSERT_TRUE(wm.open().ok);

    newdb::Row before;
    before.id = 9;
    before.attrs["n"] = "old";
    newdb::Row after;
    after.id = 9;
    after.attrs["n"] = "new";
    ASSERT_TRUE(wm.append_record(42, newdb::WalOp::UPDATE, "users", nullptr, nullptr, &before, &after,
                                 0, 0, 0, 0, 0, 7).ok);
    ASSERT_TRUE(wm.flush().ok);

    std::vector<newdb::WalDecodedRecord> recs;
    ASSERT_TRUE(wm.read_all_records(newdb::TableSchema{}, recs).ok);
    ASSERT_FALSE(recs.empty());
    const auto& last = recs.back();
    EXPECT_EQ(last.op, newdb::WalOp::UPDATE);
    EXPECT_TRUE(last.has_before_row);
    EXPECT_TRUE(last.has_after_row);
    EXPECT_EQ(last.op_seq_in_txn, 7u);
    EXPECT_EQ(last.before_row.id, 9);
    EXPECT_EQ(last.after_row.id, 9);
    EXPECT_EQ(last.before_row.attrs.at("n"), "old");
    EXPECT_EQ(last.after_row.attrs.at("n"), "new");
}

TEST(DemoTxnWal, WalCrashInjectionPointsReturnFailure) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_txn_wal_crash_inject");
    std::filesystem::create_directories(dir);
    newdb::WalManager wm("demodb", dir.string());
    ASSERT_TRUE(wm.open().ok);
    wm.set_sync_mode(newdb::WalSyncMode::Off);
    set_env_value("NEWDB_WAL_CRASH_POINT", "commit_before_write");
    newdb::Row after;
    after.id = 3;
    after.attrs["n"] = "x";
    EXPECT_FALSE(wm.append_record(1, newdb::WalOp::INSERT, "users", nullptr, nullptr, nullptr, &after,
                                  0, 0, 0, 0, 0, 1).ok);
    set_env_value("NEWDB_WAL_CRASH_POINT", "flush_before");
    EXPECT_FALSE(wm.flush().ok);
    set_env_value("NEWDB_WAL_CRASH_POINT", nullptr);
}

TEST(DemoTxnWal, WalCrashInjectionStrictMatrixAllCombinations) {
    const std::vector<const char*> crash_points = {
        "commit_before_write",
        "commit_after_write",
        "flush_before",
        "flush_after",
        "checkpoint_between_begin_end",
        "rotate_before",
        "rotate_after",
    };

    for (const char* point : crash_points) {
        SCOPED_TRACE(point);
        EXPECT_TRUE(run_wal_crash_point_and_expect_failure(point));
    }
}

TEST(DemoTxnWal, WalCrashInjectionSinglePointFromEnv) {
    const char* point = std::getenv("NEWDB_WAL_CRASH_MATRIX_POINT");
    if (point == nullptr || point[0] == '\0') {
        GTEST_SKIP() << "NEWDB_WAL_CRASH_MATRIX_POINT not set";
    }
    EXPECT_TRUE(run_wal_crash_point_and_expect_failure(point));
}

TEST(DemoTxnWal, SavepointRollbackToKeepsEarlierOps) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_txn_savepoint");
    std::filesystem::create_directories(dir);
    const std::string bin = (dir / "users.bin").string();
    ASSERT_TRUE(newdb::io::create_heap_file(bin.c_str(), {newdb::Row{1, {{"n", "a"}}, {}}}).ok);

    TxnCoordinator tx;
    tx.set_workspace_root(dir.string());
    ASSERT_TRUE(tx.begin("users").isOk());
    tx.recordOperation("INSERT", "users", "2", "", "n=b;");
    ASSERT_TRUE(tx.savepoint("sp1").isOk());
    tx.recordOperation("INSERT", "users", "3", "", "n=c;");
    ASSERT_TRUE(tx.rollbackToSavepoint("sp1").isOk());
    ASSERT_TRUE(tx.commit().isOk());
    tx.flushWAL();
    ASSERT_TRUE(tx.recoverFromWAL());

    newdb::HeapTable tbl;
    newdb::TableSchema sch;
    sch.primary_key = "id";
    ASSERT_TRUE(newdb::io::load_heap_file(bin.c_str(), "users", sch, tbl).ok);
    EXPECT_NE(tbl.find_by_id(2), nullptr);
    EXPECT_EQ(tbl.find_by_id(3), nullptr);
}

TEST(DemoTxnWal, RecoverToLsnSkipsLaterCommittedOps) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_txn_pitr_lsn");
    std::filesystem::create_directories(dir);
    const std::string bin = (dir / "users.bin").string();
    ASSERT_TRUE(newdb::io::create_heap_file(bin.c_str(), {newdb::Row{1, {{"n", "a"}}, {}}}).ok);

    TxnCoordinator tx;
    tx.set_workspace_root(dir.string());
    ASSERT_TRUE(tx.begin("users").isOk());
    tx.recordOperation("INSERT", "users", "2", "", "n=b;");
    ASSERT_TRUE(tx.commit().isOk());
    tx.flushWAL();

    newdb::WalManager wm("demodb", dir.string());
    ASSERT_TRUE(wm.open().ok);
    const std::uint64_t cut_lsn = wm.current_lsn();
    wm.close();

    ASSERT_TRUE(tx.begin("users").isOk());
    tx.recordOperation("INSERT", "users", "3", "", "n=c;");
    ASSERT_TRUE(tx.commit().isOk());
    tx.flushWAL();

    ASSERT_TRUE(tx.recoverToLsn(cut_lsn).isOk());
    newdb::HeapTable tbl;
    newdb::TableSchema sch;
    sch.primary_key = "id";
    ASSERT_TRUE(newdb::io::load_heap_file(bin.c_str(), "users", sch, tbl).ok);
    EXPECT_NE(tbl.find_by_id(2), nullptr);
    EXPECT_EQ(tbl.find_by_id(3), nullptr);
}

TEST(DemoTxnWal, RecoverToTimeSkipsLaterCommittedOps) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_txn_pitr_time");
    std::filesystem::create_directories(dir);
    const std::string bin = (dir / "users.bin").string();
    ASSERT_TRUE(newdb::io::create_heap_file(bin.c_str(), {newdb::Row{1, {{"n", "a"}}, {}}}).ok);

    TxnCoordinator tx;
    tx.set_workspace_root(dir.string());
    ASSERT_TRUE(tx.begin("users").isOk());
    tx.recordOperation("INSERT", "users", "2", "", "n=b;");
    ASSERT_TRUE(tx.commit().isOk());
    tx.flushWAL();

    // Ensure wall-clock advances so WAL record_ts_ms differs.
    std::this_thread::sleep_for(std::chrono::milliseconds(25));
    const std::uint64_t t_cut = newdb::WalManager::wall_clock_ms();
    std::this_thread::sleep_for(std::chrono::milliseconds(25));

    ASSERT_TRUE(tx.begin("users").isOk());
    tx.recordOperation("INSERT", "users", "3", "", "n=c;");
    ASSERT_TRUE(tx.commit().isOk());
    tx.flushWAL();

    ASSERT_TRUE(tx.recoverToTime(t_cut).isOk());
    newdb::HeapTable tbl;
    newdb::TableSchema sch;
    sch.primary_key = "id";
    ASSERT_TRUE(newdb::io::load_heap_file(bin.c_str(), "users", sch, tbl).ok);
    EXPECT_NE(tbl.find_by_id(2), nullptr);
    EXPECT_EQ(tbl.find_by_id(3), nullptr);
}

