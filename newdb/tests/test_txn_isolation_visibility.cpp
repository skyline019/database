#include <gtest/gtest.h>

#include <newdb/heap_table.h>
#include <newdb/mvcc.h>
#include <newdb/row.h>
#include <newdb/schema.h>
#include <newdb/wal_manager.h>

#include "cli/modules/txn/coordinator/txn_manager.h"
#include "test_util.h"

#include <atomic>
#include <barrier>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

#if !defined(_WIN32)
#include <stdlib.h>
#endif

namespace {

newdb::TableSchema isolation_schema() {
    newdb::TableSchema s;
    s.primary_key = "id";
    s.attrs = {newdb::AttrMeta{"v", newdb::AttrType::String}};
    return s;
}

newdb::HeapTable make_visibility_table() {
    newdb::HeapTable t;
    t.name = "users";
    t.rows = {
        newdb::Row{1, {{"v", "before"}}, {}},
        newdb::Row{2, {{"v", "during"}}, {}},
        newdb::Row{3, {{"v", "after"}}, {}},
        newdb::Row{4, {{"v", "deleted-before"}}, {}},
    };
    t.row_meta = {
        newdb::RecordMetadata{/*created_lsn=*/10, /*deleted_lsn=*/0, /*txn_id=*/1, /*is_tombstone=*/false},
        newdb::RecordMetadata{/*created_lsn=*/20, /*deleted_lsn=*/0, /*txn_id=*/2, /*is_tombstone=*/false},
        newdb::RecordMetadata{/*created_lsn=*/30, /*deleted_lsn=*/0, /*txn_id=*/3, /*is_tombstone=*/false},
        newdb::RecordMetadata{/*created_lsn=*/5, /*deleted_lsn=*/15, /*txn_id=*/4, /*is_tombstone=*/true},
    };
    return t;
}

class ScopedWalDir {
public:
    explicit ScopedWalDir(const std::string& tag)
        : dir_(newdb::test::unique_temp_subdir(tag)) {
        std::filesystem::create_directories(dir_);
    }

    ScopedWalDir(const ScopedWalDir&) = delete;
    ScopedWalDir& operator=(const ScopedWalDir&) = delete;

    ~ScopedWalDir() {
        std::error_code ec;
        std::filesystem::remove_all(dir_, ec);
    }

    const std::filesystem::path& path() const { return dir_; }

private:
    std::filesystem::path dir_;
};

} // namespace

TEST(TxnIsolationVisibility, SnapshotFiltersRowsCreatedAfterSnapshotAndDeletedBeforeSnapshot) {
    newdb::HeapTable t = make_visibility_table();
    const newdb::TableSchema schema = isolation_schema();

    newdb::MVCCSnapshot snap;
    snap.snapshot_lsn = 20;
    snap.committed_txn_lsn = {
        {1, 10},
        {2, 20},
        {3, 30},
        {4, 5},
    };

    t.set_snapshot(snap);
    t.rebuild_indexes(schema);

    EXPECT_NE(t.find_by_id(1), nullptr);
    EXPECT_NE(t.find_by_id(2), nullptr);
    EXPECT_EQ(t.find_by_id(3), nullptr);
    EXPECT_EQ(t.find_by_id(4), nullptr);
}

TEST(TxnIsolationVisibility, NoDirtyReadForActiveCreatorTransaction) {
    newdb::HeapTable t = make_visibility_table();
    const newdb::TableSchema schema = isolation_schema();

    newdb::MVCCSnapshot snap;
    snap.snapshot_lsn = 25;
    snap.active_txns.insert(2);
    snap.committed_txn_lsn = {
        {1, 10},
        {3, 30},
        {4, 5},
    };

    t.set_snapshot(snap);
    t.rebuild_indexes(schema);

    EXPECT_NE(t.find_by_id(1), nullptr);
    EXPECT_EQ(t.find_by_id(2), nullptr);
    EXPECT_EQ(t.find_by_id(3), nullptr);
    EXPECT_EQ(t.find_by_id(4), nullptr);
}

TEST(TxnIsolationVisibility, SnapshotRepeatableReadKeepsOriginalReadView) {
    ScopedWalDir wal_dir("newdb_txn_snapshot_repeatable");
    newdb::WalManager wal("snapshot_repeatable", wal_dir.path().string());
    ASSERT_TRUE(wal.open().ok);

    newdb::MVCCManager mvcc(wal);
    const std::uint64_t txn1 = mvcc.begin_transaction();
    mvcc.commit_transaction(txn1, 10);

    newdb::MVCCSnapshot txn_snapshot = mvcc.create_snapshot(/*read_committed=*/false);
    // `create_snapshot` binds `snapshot_lsn` to WAL high-water; this unit test does not append WAL.
    // Pin the read view to the first commit LSN to exercise repeatable-read visibility against `row_meta`.
    txn_snapshot.snapshot_lsn = 10u;

    const std::uint64_t txn2 = mvcc.begin_transaction();
    mvcc.commit_transaction(txn2, 20);

    EXPECT_EQ(txn_snapshot.snapshot_lsn, 10u);
    EXPECT_TRUE(txn_snapshot.committed_txn_lsn.find(txn2) == txn_snapshot.committed_txn_lsn.end());

    newdb::HeapTable t;
    t.name = "users";
    t.rows = {
        newdb::Row{1, {{"v", "before"}}, {}},
        newdb::Row{2, {{"v", "after"}}, {}},
    };
    t.row_meta = {
        newdb::RecordMetadata{/*created_lsn=*/10, /*deleted_lsn=*/0, /*txn_id=*/txn1, /*is_tombstone=*/false},
        newdb::RecordMetadata{/*created_lsn=*/20, /*deleted_lsn=*/0, /*txn_id=*/txn2, /*is_tombstone=*/false},
    };
    t.set_snapshot(txn_snapshot);
    t.rebuild_indexes(isolation_schema());

    EXPECT_NE(t.find_by_id(1), nullptr);
    EXPECT_EQ(t.find_by_id(2), nullptr);

    wal.close();
}

TEST(TxnIsolationVisibility, ReadCommittedUsesFreshStatementSnapshot) {
    ScopedWalDir wal_dir("newdb_txn_read_committed_fresh");
    newdb::WalManager wal("read_committed_fresh", wal_dir.path().string());
    ASSERT_TRUE(wal.open().ok);

    newdb::MVCCManager mvcc(wal);
    const std::uint64_t txn1 = mvcc.begin_transaction();
    mvcc.commit_transaction(txn1, 10);

    newdb::MVCCSnapshot first_statement = mvcc.create_snapshot(/*read_committed=*/true);
    first_statement.snapshot_lsn = 10u;

    const std::uint64_t txn2 = mvcc.begin_transaction();
    mvcc.commit_transaction(txn2, 20);

    newdb::MVCCSnapshot second_statement = mvcc.create_snapshot(/*read_committed=*/true);
    second_statement.snapshot_lsn = 20u;

    EXPECT_EQ(first_statement.snapshot_lsn, 10u);
    EXPECT_EQ(second_statement.snapshot_lsn, 20u);
    EXPECT_TRUE(first_statement.committed_txn_lsn.find(txn2) == first_statement.committed_txn_lsn.end());
    EXPECT_NE(second_statement.committed_txn_lsn.find(txn2), second_statement.committed_txn_lsn.end());

    newdb::HeapTable t;
    t.name = "users";
    t.rows = {
        newdb::Row{1, {{"v", "before"}}, {}},
        newdb::Row{2, {{"v", "after"}}, {}},
    };
    t.row_meta = {
        newdb::RecordMetadata{/*created_lsn=*/10, /*deleted_lsn=*/0, /*txn_id=*/txn1, /*is_tombstone=*/false},
        newdb::RecordMetadata{/*created_lsn=*/20, /*deleted_lsn=*/0, /*txn_id=*/txn2, /*is_tombstone=*/false},
    };

    t.set_snapshot(first_statement);
    t.rebuild_indexes(isolation_schema());
    EXPECT_NE(t.find_by_id(1), nullptr);
    EXPECT_EQ(t.find_by_id(2), nullptr);

    t.set_snapshot(second_statement);
    t.rebuild_indexes(isolation_schema());
    EXPECT_NE(t.find_by_id(1), nullptr);
    EXPECT_NE(t.find_by_id(2), nullptr);

    wal.close();
}

TEST(TxnIsolationVisibility, InterleavedCommitRcRefreshesSnapshotStaysPinned) {
    // Models two logical "sessions" after a peer commit at LSN 20: RC refreshes per statement so the
    // second statement sees the new row; Snapshot keeps the transaction-scoped read view at LSN 10.
    ScopedWalDir wal_dir("newdb_txn_interleaved_rc_snap");
    newdb::WalManager wal("interleaved_rc_snap", wal_dir.path().string());
    ASSERT_TRUE(wal.open().ok);

    newdb::MVCCManager mvcc(wal);
    const std::uint64_t txn1 = mvcc.begin_transaction();
    mvcc.commit_transaction(txn1, 10);
    newdb::MVCCSnapshot snap_tx = mvcc.create_snapshot(/*read_committed=*/false);
    snap_tx.snapshot_lsn = 10u;

    const std::uint64_t txn2 = mvcc.begin_transaction();
    mvcc.commit_transaction(txn2, 20);
    EXPECT_TRUE(snap_tx.committed_txn_lsn.find(txn2) == snap_tx.committed_txn_lsn.end());

    newdb::HeapTable t;
    t.name = "users";
    t.rows = {
        newdb::Row{1, {{"v", "before"}}, {}},
        newdb::Row{2, {{"v", "after"}}, {}},
    };
    t.row_meta = {
        newdb::RecordMetadata{/*created_lsn=*/10, /*deleted_lsn=*/0, /*txn_id=*/txn1, /*is_tombstone=*/false},
        newdb::RecordMetadata{/*created_lsn=*/20, /*deleted_lsn=*/0, /*txn_id=*/txn2, /*is_tombstone=*/false},
    };

    t.set_snapshot(snap_tx);
    t.rebuild_indexes(isolation_schema());
    EXPECT_NE(t.find_by_id(1), nullptr);
    EXPECT_EQ(t.find_by_id(2), nullptr);

    newdb::MVCCSnapshot rc_stmt2 = mvcc.create_snapshot(/*read_committed=*/true);
    rc_stmt2.snapshot_lsn = 20u;
    EXPECT_NE(rc_stmt2.committed_txn_lsn.find(txn2), rc_stmt2.committed_txn_lsn.end());

    t.set_snapshot(rc_stmt2);
    t.rebuild_indexes(isolation_schema());
    EXPECT_NE(t.find_by_id(1), nullptr);
    EXPECT_NE(t.find_by_id(2), nullptr);

    wal.close();
}

TEST(TxnIsolationVisibility, MultithreadLocalTablesSnapshotVsRcMatchesBaseline) {
    constexpr int kThreads = 4;
    constexpr int kIters = 80;
    const std::uint64_t txn1 = 1;
    const std::uint64_t txn2 = 2;
    newdb::MVCCSnapshot snap_tx{};
    snap_tx.snapshot_lsn = 10u;
    snap_tx.committed_txn_lsn[txn1] = 10u;
    newdb::MVCCSnapshot rc2{};
    rc2.snapshot_lsn = 20u;
    rc2.committed_txn_lsn[txn1] = 10u;
    rc2.committed_txn_lsn[txn2] = 20u;

    std::barrier sync(static_cast<std::ptrdiff_t>(kThreads));
    std::atomic<int> errors{0};
    auto worker = [&] {
        for (int i = 0; i < kIters; ++i) {
            sync.arrive_and_wait();
            newdb::HeapTable t;
            t.name = "users";
            t.rows = {
                newdb::Row{1, {{"v", "before"}}, {}},
                newdb::Row{2, {{"v", "after"}}, {}},
            };
            t.row_meta = {
                newdb::RecordMetadata{/*created_lsn=*/10, /*deleted_lsn=*/0, /*txn_id=*/txn1, /*is_tombstone=*/false},
                newdb::RecordMetadata{/*created_lsn=*/20, /*deleted_lsn=*/0, /*txn_id=*/txn2, /*is_tombstone=*/false},
            };
            t.set_snapshot(snap_tx);
            t.rebuild_indexes(isolation_schema());
            if (t.find_by_id(1) == nullptr || t.find_by_id(2) != nullptr) {
                errors.fetch_add(1, std::memory_order_relaxed);
            }
            t.set_snapshot(rc2);
            t.rebuild_indexes(isolation_schema());
            if (t.find_by_id(1) == nullptr || t.find_by_id(2) == nullptr) {
                errors.fetch_add(1, std::memory_order_relaxed);
            }
            sync.arrive_and_wait();
        }
    };
    std::vector<std::thread> threads;
    threads.reserve(static_cast<std::size_t>(kThreads));
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back(worker);
    }
    for (auto& th : threads) {
        th.join();
    }
    EXPECT_EQ(errors.load(std::memory_order_relaxed), 0);
}

// High iteration count to stress the same MVCC invariants (bounded; same pattern as barrier smoke).
TEST(TxnIsolationVisibility, MultithreadLocalTablesHighLoadStressBounded) {
    constexpr int kThreads = 4;
    constexpr int kIters = 500;
    const std::uint64_t txn1 = 1;
    const std::uint64_t txn2 = 2;
    newdb::MVCCSnapshot snap_tx{};
    snap_tx.snapshot_lsn = 10u;
    snap_tx.committed_txn_lsn[txn1] = 10u;
    newdb::MVCCSnapshot rc2{};
    rc2.snapshot_lsn = 20u;
    rc2.committed_txn_lsn[txn1] = 10u;
    rc2.committed_txn_lsn[txn2] = 20u;

    std::barrier sync(static_cast<std::ptrdiff_t>(kThreads));
    std::atomic<int> errors{0};
    auto worker = [&] {
        for (int i = 0; i < kIters; ++i) {
            sync.arrive_and_wait();
            newdb::HeapTable t;
            t.name = "users";
            t.rows = {
                newdb::Row{1, {{"v", "before"}}, {}},
                newdb::Row{2, {{"v", "after"}}, {}},
            };
            t.row_meta = {
                newdb::RecordMetadata{/*created_lsn=*/10, /*deleted_lsn=*/0, /*txn_id=*/txn1, /*is_tombstone=*/false},
                newdb::RecordMetadata{/*created_lsn=*/20, /*deleted_lsn=*/0, /*txn_id=*/txn2, /*is_tombstone=*/false},
            };
            t.set_snapshot(snap_tx);
            t.rebuild_indexes(isolation_schema());
            if (t.find_by_id(1) == nullptr || t.find_by_id(2) != nullptr) {
                errors.fetch_add(1, std::memory_order_relaxed);
            }
            t.set_snapshot(rc2);
            t.rebuild_indexes(isolation_schema());
            if (t.find_by_id(1) == nullptr || t.find_by_id(2) == nullptr) {
                errors.fetch_add(1, std::memory_order_relaxed);
            }
            sync.arrive_and_wait();
        }
    };
    std::vector<std::thread> threads;
    threads.reserve(static_cast<std::size_t>(kThreads));
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back(worker);
    }
    for (auto& th : threads) {
        th.join();
    }
    EXPECT_EQ(errors.load(std::memory_order_relaxed), 0);
}

TEST(TxnIsolationVisibility, IsolationLevelConfigDocumentsReadViewPolicy) {
    TxnCoordinator c;
    EXPECT_EQ(c.txnIsolationLevel(), TxnIsolationLevel::Snapshot);

    c.setTxnIsolationLevel(TxnIsolationLevel::ReadCommitted);
    EXPECT_EQ(c.txnIsolationLevel(), TxnIsolationLevel::ReadCommitted);

    c.setTxnIsolationLevel(TxnIsolationLevel::Snapshot);
    EXPECT_EQ(c.txnIsolationLevel(), TxnIsolationLevel::Snapshot);
}

TEST(TxnIsolationVisibility, ReadCommittedActiveTxnPublishesStatementSnapshotLsn) {
    ScopedWalDir wal_dir("newdb_txn_rc_stmt_lsn");
    const auto root = wal_dir.path();
    {
        std::ofstream f(root / "users.bin");
        f.put('x');
    }
    TxnCoordinator txn;
    txn.set_workspace_root(root.string());
    txn.setTxnIsolationLevel(TxnIsolationLevel::ReadCommitted);
    ASSERT_TRUE(txn.begin("users").isOk());

    newdb::HeapTable table;
    table.name = "users";
    txn.syncHeapReadSnapshotForQuery(table);
    const TxnRuntimeStats st = txn.runtimeStats();
    EXPECT_EQ(st.last_snapshot_source, "statement");
    EXPECT_GT(st.statement_snapshot_lsn, 0u);
    EXPECT_EQ(st.transaction_snapshot_lsn, 0u);
    EXPECT_GE(st.txn_snapshot_refresh_count, 1u);

    ASSERT_TRUE(txn.commit().isOk());
}

TEST(TxnIsolationVisibility, SnapshotActiveTxnPinsTransactionSnapshotLsn) {
    ScopedWalDir wal_dir("newdb_txn_snap_pin_lsn");
    const auto root = wal_dir.path();
    {
        std::ofstream f(root / "users.bin");
        f.put('x');
    }
    TxnCoordinator txn;
    txn.set_workspace_root(root.string());
    txn.setTxnIsolationLevel(TxnIsolationLevel::Snapshot);
    ASSERT_TRUE(txn.begin("users").isOk());

    newdb::HeapTable table;
    table.name = "users";
    txn.syncHeapReadSnapshotForQuery(table);
    const TxnRuntimeStats st = txn.runtimeStats();
    EXPECT_EQ(st.last_snapshot_source, "txn");
    EXPECT_GT(st.transaction_snapshot_lsn, 0u);
    EXPECT_EQ(st.statement_snapshot_lsn, 0u);
    EXPECT_GE(st.txn_snapshot_pinned_count, 1u);
    ASSERT_TRUE(table.active_snapshot.has_value());
    EXPECT_EQ(table.active_snapshot->snapshot_lsn, st.transaction_snapshot_lsn);

    ASSERT_TRUE(txn.commit().isOk());
}

TEST(TxnIsolationVisibility, SyncHeapReadSnapshotIncrementsDisabledCounterWhenReadpathOff) {
    TxnCoordinator txn;
    newdb::HeapTable table;
    table.name = "t";
#if defined(_WIN32)
    ASSERT_EQ(0, _putenv_s("NEWDB_TXN_ISOLATION_READPATH", "0"));
#else
    ASSERT_EQ(0, setenv("NEWDB_TXN_ISOLATION_READPATH", "0", 1));
#endif
    txn.syncHeapReadSnapshotForQuery(table);
    const TxnRuntimeStats s = txn.runtimeStats();
    EXPECT_GE(s.txn_readpath_disabled_count, 1u);
    EXPECT_EQ(s.last_snapshot_source, "disabled");
    EXPECT_FALSE(table.active_snapshot.has_value());
#if defined(_WIN32)
    (void)_putenv("NEWDB_TXN_ISOLATION_READPATH=");
#else
    unsetenv("NEWDB_TXN_ISOLATION_READPATH");
#endif
}
