#include <gtest/gtest.h>

#include "cli/modules/txn/coordinator/txn_manager.h"
#include "cli/modules/txn/coordinator/write_conflict/lock_key.h"
#include "cli/modules/where/executor/where.h"

#include <newdb/schema.h>

#include <filesystem>
#include <chrono>
#include <thread>

namespace {
std::filesystem::path unique_temp_subdir(const char* tag) {
    namespace fs = std::filesystem;
    std::error_code ec;
    fs::path base = fs::temp_directory_path(ec);
    if (ec) base = fs::current_path(ec);
    const auto now = std::to_string(
        static_cast<long long>(std::chrono::steady_clock::now().time_since_epoch().count()));
    fs::path dir = base / (std::string("newdb_") + tag + "_" + now);
    fs::create_directories(dir, ec);
    return dir;
}
} // namespace

// Bounded single-thread stress: fresh `TxnCoordinator` per step on one workspace, alternating disjoint
// key bands (simulates interleaved writers without relying on OS file-lock thread semantics). See
// `TXN_ISOLATION_AND_LOCKING.md` / assessment §6 for boundedTxn stress scope.
TEST(TxnWriteConflict, AlternatingCoordinatorsSameWorkspaceBoundedStress) {
    namespace fs = std::filesystem;
    const fs::path ws = unique_temp_subdir("txn_stress_alt_ws");

    constexpr int kSteps = 80;
    for (int i = 0; i < kSteps; ++i) {
        TxnCoordinator c;
        c.set_workspace_root(ws.string());
        ASSERT_TRUE(c.begin("users").isOk()) << "step " << i;
        std::string reason;
        const int key = (i % 2 == 0) ? i : (100000 + i);
        ASSERT_TRUE(c.tryReserveWriteKey("users", key, &reason)) << reason << " step " << i;
        ASSERT_TRUE(c.commit().isOk()) << "step " << i;
    }

    std::error_code ec;
    fs::remove_all(ws, ec);
}

TEST(TxnWriteConflict, SameTableSameIdRejectedAcrossActiveTransactions) {
    namespace fs = std::filesystem;
    const fs::path ws = unique_temp_subdir("txn_conflict");

    TxnCoordinator a;
    a.set_workspace_root(ws.string());
    TxnCoordinator b;
    b.set_workspace_root(ws.string());

    ASSERT_TRUE(a.begin("ta").isOk());
    ASSERT_TRUE(b.begin("tb").isOk());

    std::string reason;
    EXPECT_TRUE(a.tryReserveWriteKey("users", 7, &reason));
    EXPECT_FALSE(b.tryReserveWriteKey("users", 7, &reason));
    EXPECT_NE(reason.find("write conflict"), std::string::npos);
    EXPECT_GE(b.writeConflictCount(), 1u);
    EXPECT_NE(b.runtimeStats().write_conflict_last_sample.find("table=users"), std::string::npos);
    EXPECT_NE(b.runtimeStats().write_conflict_last_sample.find("tag=reject"), std::string::npos);

    ASSERT_TRUE(a.commit().isOk());
    EXPECT_TRUE(b.tryReserveWriteKey("users", 7, &reason));

    ASSERT_TRUE(b.commit().isOk());

    std::error_code ec;
    fs::remove_all(ws, ec);
}

TEST(TxnWriteConflict, SameTableConcurrentBeginRespectsProcessScopedLockSemantics) {
    namespace fs = std::filesystem;
    const fs::path ws = unique_temp_subdir("txn_conflict_same_table_lock");

    TxnCoordinator a;
    a.set_workspace_root(ws.string());
    TxnCoordinator b;
    b.set_workspace_root(ws.string());

    ASSERT_TRUE(a.begin("users").isOk());
    const auto b_begin = b.begin("users");
#if defined(_WIN32)
    EXPECT_TRUE(b_begin.isErr());
#else
    // POSIX fcntl lock is process-scoped; in single-process tests second begin may succeed.
    EXPECT_TRUE(b_begin.isOk() || b_begin.isErr());
#endif
    if (b_begin.isErr()) {
        EXPECT_GE(b.runtimeStats().txn_begin_lock_conflict_count, 1u);
        EXPECT_GE(b.runtimeStats().file_lock_acquire_fail_count, 1u);
    }

    // Regardless of file-lock behavior, write-intent owner must still prevent same-key concurrent writes.
    std::string reason;
    EXPECT_TRUE(a.tryReserveWriteKey("users", 99, &reason));
    if (b_begin.isOk()) {
        EXPECT_FALSE(b.tryReserveWriteKey("users", 99, &reason));
    }

    ASSERT_TRUE(a.commit().isOk());
    if (b_begin.isOk()) {
        ASSERT_TRUE(b.commit().isOk());
    }

    std::error_code ec;
    fs::remove_all(ws, ec);
}

TEST(TxnWriteConflict, DifferentTableSameIdAllowed) {
    namespace fs = std::filesystem;
    const fs::path ws = unique_temp_subdir("txn_conflict_diff_table");

    TxnCoordinator a;
    a.set_workspace_root(ws.string());
    TxnCoordinator b;
    b.set_workspace_root(ws.string());

    ASSERT_TRUE(a.begin("users").isOk());
    ASSERT_TRUE(b.begin("orders").isOk());

    std::string reason;
    EXPECT_TRUE(a.tryReserveWriteKey("users", 7, &reason));
    EXPECT_TRUE(b.tryReserveWriteKey("orders", 7, &reason));
    EXPECT_EQ(b.writeConflictCount(), 0u);

    ASSERT_TRUE(a.commit().isOk());
    ASSERT_TRUE(b.commit().isOk());

    std::error_code ec;
    fs::remove_all(ws, ec);
}

TEST(TxnWriteConflict, RollbackReleasesWriteIntent) {
    namespace fs = std::filesystem;
    const fs::path ws = unique_temp_subdir("txn_conflict_rollback_release");

    TxnCoordinator a;
    a.set_workspace_root(ws.string());

    ASSERT_TRUE(a.begin("users").isOk());
    std::string reason;
    EXPECT_TRUE(a.tryReserveWriteKey("users", 42, &reason));
    ASSERT_TRUE(a.rollback().isOk());

    TxnCoordinator b2;
    b2.set_workspace_root(ws.string());
    ASSERT_TRUE(b2.begin("users").isOk());
    EXPECT_TRUE(b2.tryReserveWriteKey("users", 42, &reason));
    ASSERT_TRUE(b2.commit().isOk());

    std::error_code ec;
    fs::remove_all(ws, ec);
}

TEST(TxnWriteConflict, WaitPolicyCanAcquireAfterPeerCommit) {
    namespace fs = std::filesystem;
    const fs::path ws = unique_temp_subdir("txn_conflict_wait_success");

    TxnCoordinator a;
    a.set_workspace_root(ws.string());
    TxnCoordinator b;
    b.set_workspace_root(ws.string());

    ASSERT_TRUE(a.begin("users").isOk());
    ASSERT_TRUE(b.begin("orders").isOk());
    ASSERT_TRUE(a.tryReserveWriteKey("users", 88, nullptr));

    b.setWriteConflictPolicy(WriteConflictPolicy::Wait);
    b.setWriteConflictWaitTimeoutMs(300);

    std::thread releaser([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(80));
        ASSERT_TRUE(a.commit().isOk());
    });

    const auto t0 = std::chrono::steady_clock::now();
    std::string reason;
    const bool ok = b.tryReserveWriteKey("users", 88, &reason);
    const auto waited_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
    releaser.join();

    EXPECT_TRUE(ok);
    EXPECT_GE(waited_ms, 40);
    EXPECT_GE(b.writeConflictWaitCount(), 1u);
    EXPECT_EQ(b.writeConflictWaitTimeoutCount(), 0u);

    ASSERT_TRUE(b.commit().isOk());
    std::error_code ec;
    fs::remove_all(ws, ec);
}

TEST(TxnWriteConflict, WaitPolicyTimeoutReturnsConflict) {
    namespace fs = std::filesystem;
    const fs::path ws = unique_temp_subdir("txn_conflict_wait_timeout");

    TxnCoordinator a;
    a.set_workspace_root(ws.string());
    TxnCoordinator b;
    b.set_workspace_root(ws.string());

    ASSERT_TRUE(a.begin("users").isOk());
    ASSERT_TRUE(b.begin("orders").isOk());
    ASSERT_TRUE(a.tryReserveWriteKey("users", 99, nullptr));

    b.setWriteConflictPolicy(WriteConflictPolicy::Wait);
    b.setWriteConflictWaitTimeoutMs(60);

    std::string reason;
    const auto t0 = std::chrono::steady_clock::now();
    const bool ok = b.tryReserveWriteKey("users", 99, &reason);
    const auto waited_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();

    EXPECT_FALSE(ok);
    EXPECT_NE(reason.find("timeout"), std::string::npos);
    EXPECT_GE(waited_ms, 40);
    EXPECT_GE(b.writeConflictCount(), 1u);
    EXPECT_GE(b.writeConflictWaitTimeoutCount(), 1u);

    ASSERT_TRUE(a.commit().isOk());
    ASSERT_TRUE(b.commit().isOk());
    std::error_code ec;
    fs::remove_all(ws, ec);
}

TEST(LockKey, RowPkWriteIntentMatchesLegacyStorageKey) {
    EXPECT_EQ(LockKey::row_pk_write_intent("users", 7).to_storage_key(), "users#7");
}

TEST(LockKey, ExtendedSerializersAreStable) {
    const auto r = LockKey::range_write_intent("users", "age", "1", "9");
    EXPECT_EQ(r.to_storage_key(), "v2|range|users|age|1|9");
    const auto p = LockKey::predicate_write_intent("users", "age", "x>5");
    EXPECT_EQ(p.to_storage_key(), "v2|pred|users|age|x>5");
    const auto ix = LockKey::index_eq_write_intent("users", "email_idx", "a@b");
    EXPECT_EQ(ix.to_storage_key(), "v2|idx|users|email_idx|a@b");
}

TEST(TxnWriteConflict, BatchSortedReleasesEarlierKeysOnMidBatchFailure) {
    namespace fs = std::filesystem;
    const fs::path ws = unique_temp_subdir("txn_batch_sorted_partial");

    TxnCoordinator a;
    a.set_workspace_root(ws.string());
    TxnCoordinator b;
    b.set_workspace_root(ws.string());

    ASSERT_TRUE(a.begin("ta").isOk());
    ASSERT_TRUE(b.begin("tb").isOk());
    std::string reason;
    EXPECT_TRUE(a.tryReserveWriteKey("users", 2, &reason));

    std::vector<int> ids = {3, 1, 2};
    EXPECT_FALSE(b.tryReserveWriteKeysBatchSorted("users", ids, &reason));
    EXPECT_TRUE(b.tryReserveWriteKey("users", 1, &reason));
    EXPECT_TRUE(b.tryReserveWriteKey("users", 3, &reason));

    ASSERT_TRUE(a.commit().isOk());
    ASSERT_TRUE(b.commit().isOk());

    std::error_code ec;
    fs::remove_all(ws, ec);
}

TEST(TxnWriteConflict, PredicateFingerprintConflictsAcrossTransactions) {
    namespace fs = std::filesystem;
    const fs::path ws = unique_temp_subdir("txn_pred_fp");

    TxnCoordinator a;
    a.set_workspace_root(ws.string());
    TxnCoordinator b;
    b.set_workspace_root(ws.string());

    ASSERT_TRUE(a.begin("ta").isOk());
    ASSERT_TRUE(b.begin("tb").isOk());

    std::vector<WhereCond> conds;
    WhereCond c0;
    c0.attr = "age";
    c0.op = CondOp::Ge;
    c0.value = "10";
    c0.logic_with_prev = "";
    conds.push_back(c0);
    WhereCond c1;
    c1.attr = "age";
    c1.op = CondOp::Le;
    c1.value = "20";
    c1.logic_with_prev = "AND";
    conds.push_back(c1);

    const std::string fp = where_predicate_fingerprint_for_write_intent(conds);
    const LockKey pk = LockKey::predicate_write_intent("users", "_where", fp);
    std::string reason;
    EXPECT_TRUE(a.tryReserveWriteLockKey(pk, &reason));
    EXPECT_FALSE(b.tryReserveWriteLockKey(pk, &reason));
    ASSERT_TRUE(a.commit().isOk());
    EXPECT_TRUE(b.tryReserveWriteLockKey(pk, &reason));
    ASSERT_TRUE(b.commit().isOk());

    std::error_code ec;
    fs::remove_all(ws, ec);
}

TEST(WhereCoarseLockDerivation, ClosedIntRangeForIntColumn) {
    newdb::TableSchema schema;
    schema.attrs = {newdb::AttrMeta{"age", newdb::AttrType::Int}};
    std::vector<WhereCond> conds;
    WhereCond a;
    a.attr = "age";
    a.op = CondOp::Ge;
    a.value = "3";
    a.logic_with_prev = "";
    WhereCond b;
    b.attr = "age";
    b.op = CondOp::Le;
    b.value = "9";
    b.logic_with_prev = "AND";
    conds.push_back(a);
    conds.push_back(b);
    const auto r = where_try_derive_closed_int_range(schema, conds);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->column, "age");
    EXPECT_EQ(r->begin_inclusive, "3");
    EXPECT_EQ(r->end_inclusive, "9");
}

TEST(WhereCoarseLockDerivation, NoRangeWhenMixedColumns) {
    newdb::TableSchema schema;
    schema.attrs = {newdb::AttrMeta{"age", newdb::AttrType::Int},
                    newdb::AttrMeta{"dept", newdb::AttrType::String}};
    std::vector<WhereCond> conds;
    WhereCond a;
    a.attr = "age";
    a.op = CondOp::Ge;
    a.value = "1";
    a.logic_with_prev = "";
    WhereCond b;
    b.attr = "dept";
    b.op = CondOp::Le;
    b.value = "x";
    b.logic_with_prev = "AND";
    conds.push_back(a);
    conds.push_back(b);
    EXPECT_FALSE(where_try_derive_closed_int_range(schema, conds).has_value());
}

TEST(TxnWriteConflict, IndexEqSameStorageKeyRejectedAcrossActiveTransactions) {
    namespace fs = std::filesystem;
    const fs::path ws = unique_temp_subdir("txn_idx_eq_cross");

    TxnCoordinator a;
    a.set_workspace_root(ws.string());
    TxnCoordinator b;
    b.set_workspace_root(ws.string());

    ASSERT_TRUE(a.begin("ta").isOk());
    ASSERT_TRUE(b.begin("tb").isOk());

    const LockKey lk = LockKey::index_eq_write_intent("users", "email", "a@b");
    std::string reason;
    EXPECT_TRUE(a.tryReserveWriteLockKey(lk, &reason));
    EXPECT_FALSE(b.tryReserveWriteLockKey(lk, &reason));
    EXPECT_NE(reason.find("write conflict"), std::string::npos);

    ASSERT_TRUE(a.commit().isOk());
    EXPECT_TRUE(b.tryReserveWriteLockKey(lk, &reason));
    ASSERT_TRUE(b.commit().isOk());

    std::error_code ec;
    fs::remove_all(ws, ec);
}

TEST(TxnWriteConflict, RangeSameStorageKeyRejectedAcrossActiveTransactions) {
    namespace fs = std::filesystem;
    const fs::path ws = unique_temp_subdir("txn_range_cross");

    TxnCoordinator a;
    a.set_workspace_root(ws.string());
    TxnCoordinator b;
    b.set_workspace_root(ws.string());

    ASSERT_TRUE(a.begin("ta").isOk());
    ASSERT_TRUE(b.begin("tb").isOk());

    const LockKey rk = LockKey::range_write_intent("users", "age", "1", "10");
    std::string reason;
    EXPECT_TRUE(a.tryReserveWriteLockKey(rk, &reason));
    EXPECT_FALSE(b.tryReserveWriteLockKey(rk, &reason));

    ASSERT_TRUE(a.commit().isOk());
    EXPECT_TRUE(b.tryReserveWriteLockKey(rk, &reason));
    ASSERT_TRUE(b.commit().isOk());

    std::error_code ec;
    fs::remove_all(ws, ec);
}

TEST(TxnWriteConflict, RangeAndPredicateLockCountsFirstReserveOnly) {
    namespace fs = std::filesystem;
    const fs::path ws = unique_temp_subdir("txn_lock_ext");
    TxnCoordinator c;
    c.set_workspace_root(ws.string());
    ASSERT_TRUE(c.begin("users").isOk());
    std::string reason;
    const auto rk = LockKey::range_write_intent("users", "age", "1", "10");
    ASSERT_TRUE(c.tryReserveWriteLockKey(rk, &reason)) << reason;
    EXPECT_EQ(c.runtimeStats().lock_key_range_count, 1u);
    ASSERT_TRUE(c.tryReserveWriteLockKey(rk, &reason));
    EXPECT_EQ(c.runtimeStats().lock_key_range_count, 1u);
    const auto pk = LockKey::predicate_write_intent("users", "age", "expr1");
    ASSERT_TRUE(c.tryReserveWriteLockKey(pk, &reason));
    EXPECT_EQ(c.runtimeStats().lock_key_predicate_count, 1u);
    ASSERT_TRUE(c.commit().isOk());
    std::error_code ec;
    fs::remove_all(ws, ec);
}

