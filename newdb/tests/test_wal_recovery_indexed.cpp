#include <gtest/gtest.h>

#include <newdb/wal_manager.h>
#include <newdb/heap_table.h>
#include <newdb/wal/wal_recovery_pipeline.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <chrono>
#include <cstdlib>
#include <string>

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

#if defined(_WIN32)
void set_test_env(const char* key, const char* val) {
    (void)_putenv_s(key, val);
}
void unset_test_env(const char* key) {
    (void)_putenv_s(key, "");
}
#else
void set_test_env(const char* key, const char* val) {
    setenv(key, val, 1);
}
void unset_test_env(const char* key) {
    unsetenv(key);
}
#endif

struct ScopedEnv {
    const char* key;
    ScopedEnv(const char* k, const char* val) : key(k) {
        set_test_env(k, val);
    }
    ScopedEnv(const ScopedEnv&) = delete;
    ScopedEnv& operator=(const ScopedEnv&) = delete;
    ~ScopedEnv() {
        unset_test_env(key);
    }
};
} // namespace

TEST(WalRecoveryIndexed, CheckpointPayloadRaisesReplayStartAndSkipsRecordsWhenOptIn) {
    namespace fs = std::filesystem;
    const fs::path dir = unique_temp_subdir("wal_cp_replay");
    const std::string db = "recovery_cp";

    unset_test_env("NEWDB_RECOVER_ENABLE_OFFSET_SEEK");
    unset_test_env("NEWDB_RECOVER_USE_CHECKPOINT_LSN");

    newdb::WalManager wal(db, dir.string());
    wal.set_segment_max_bytes(512);
    ASSERT_TRUE(wal.open().ok);

    newdb::Row r;
    r.attrs["name"] = "u";
    r.attrs["dept"] = "ENG";
    r.attrs["age"] = "30";
    r.attrs["salary"] = "100";

    for (int wave = 0; wave < 2; ++wave) {
        if (wave == 1) {
            ASSERT_TRUE(wal.checkpoint(wal.current_lsn()).ok);
            ASSERT_TRUE(wal.flush().ok);
        }
        for (int i = 0; i < 25; ++i) {
            const int id = wave * 50 + i + 1;
            r.id = id;
            ASSERT_TRUE(wal.begin_transaction(static_cast<std::uint64_t>(id)).ok);
            ASSERT_TRUE(wal.append_record(static_cast<std::uint64_t>(id), newdb::WalOp::INSERT, "users", &r).ok);
            ASSERT_TRUE(wal.commit_transaction(static_cast<std::uint64_t>(id)).ok);
        }
    }
    ASSERT_TRUE(wal.flush().ok);

    auto recover_once = [&]() {
        newdb::HeapTable table;
        table.name = "users";
        newdb::TableSchema schema;
        newdb::WalRecoveryStats st{};
        EXPECT_TRUE(wal.recover(&table, schema, &st).ok);
        return st;
    };

    // Baseline: disable checkpoint replay start so we compare against full WAL scan.
    newdb::WalRecoveryStats baseline{};
    {
        ScopedEnv e_off("NEWDB_RECOVER_USE_CHECKPOINT_LSN", "0");
        baseline = recover_once();
    }
    ASSERT_GT(baseline.records_read, 0u);

    // With default checkpoint replay + offset seek, fewer records should be decoded from disk.
    ScopedEnv e_seek("NEWDB_RECOVER_ENABLE_OFFSET_SEEK", "1");
    const auto opt = recover_once();

    EXPECT_GT(opt.last_complete_checkpoint_lsn, 0u);
    EXPECT_GE(opt.replay_start_lsn, opt.last_complete_checkpoint_lsn);
    EXPECT_GT(baseline.records_read, opt.records_read);
    EXPECT_GT(opt.seek_skipped_records, 0u);

    wal.close();
    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST(WalRecoveryIndexed, IncompleteCheckpointWithUncommittedInsertNotVisible) {
    namespace fs = std::filesystem;
    const fs::path dir = unique_temp_subdir("wal_cp_uncommitted");
    const std::string db = "recovery_uncommitted";

    newdb::WalManager wal(db, dir.string());
    ASSERT_TRUE(wal.open().ok);
    ASSERT_TRUE(wal.checkpoint_begin(1).ok);
    ASSERT_TRUE(wal.begin_transaction(7).ok);
    newdb::Row r;
    r.id = 1;
    r.attrs["name"] = "ghost";
    r.attrs["dept"] = "ENG";
    r.attrs["age"] = "30";
    r.attrs["salary"] = "100";
    ASSERT_TRUE(wal.append_record(7, newdb::WalOp::INSERT, "users", &r).ok);
    // Intentionally no commit_transaction: redo must not surface uncommitted row.
    ASSERT_TRUE(wal.flush().ok);

    newdb::HeapTable table;
    table.name = "users";
    newdb::TableSchema schema;
    schema.primary_key = "id";
    schema.attrs = {
        newdb::AttrMeta{"name", newdb::AttrType::String},
        newdb::AttrMeta{"dept", newdb::AttrType::String},
        newdb::AttrMeta{"age", newdb::AttrType::String},
        newdb::AttrMeta{"salary", newdb::AttrType::String},
    };
    newdb::WalRecoveryStats st{};
    ASSERT_TRUE(wal.recover(&table, schema, &st).ok);
    EXPECT_EQ(table.logical_row_count(), 0u);
    EXPECT_GE(st.checkpoint_midpoint_recovery_count, 1u);
    EXPECT_GE(st.incomplete_checkpoint_count, 1u);

    wal.close();
    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST(WalRecoveryIndexed, IncompleteCheckpointBeginWithoutEndCountsMidpoint) {
    namespace fs = std::filesystem;
    const fs::path dir = unique_temp_subdir("wal_cp_mid");
    const std::string db = "recovery_mid";

    newdb::WalManager wal(db, dir.string());
    ASSERT_TRUE(wal.open().ok);
    ASSERT_TRUE(wal.checkpoint_begin(1).ok);
    ASSERT_TRUE(wal.flush().ok);

    newdb::HeapTable table;
    table.name = "t";
    newdb::TableSchema schema;
    newdb::WalRecoveryStats stats{};
    ASSERT_TRUE(wal.recover(&table, schema, &stats).ok);
    EXPECT_GE(stats.checkpoint_midpoint_recovery_count, 1u);
    EXPECT_GE(stats.incomplete_checkpoint_count, 1u);
    EXPECT_NE(stats.recovery_policy.find("incomplete_checkpoint_tail"), std::string::npos);

    wal.close();
    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST(WalRecoveryIndexed, CheckpointBetweenBeginEndReplaysCommittedInsert) {
    namespace fs = std::filesystem;
    const fs::path dir = unique_temp_subdir("wal_cp_between");
    const std::string db = "recovery_between";

    newdb::WalManager wal(db, dir.string());
    wal.set_segment_max_bytes(512);
    ASSERT_TRUE(wal.open().ok);

    newdb::Row r;
    r.id = 1;
    r.attrs["name"] = "x";
    r.attrs["dept"] = "ENG";
    r.attrs["age"] = "30";
    r.attrs["salary"] = "100";

    ASSERT_TRUE(wal.checkpoint_begin(1).ok);
    ASSERT_TRUE(wal.begin_transaction(1).ok);
    ASSERT_TRUE(wal.append_record(1, newdb::WalOp::INSERT, "users", &r).ok);
    ASSERT_TRUE(wal.commit_transaction(1).ok);
    ASSERT_TRUE(wal.checkpoint_end(1).ok);
    ASSERT_TRUE(wal.flush().ok);

    newdb::HeapTable table;
    table.name = "users";
    newdb::TableSchema schema;
    schema.primary_key = "id";
    schema.attrs = {
        newdb::AttrMeta{"name", newdb::AttrType::String},
        newdb::AttrMeta{"dept", newdb::AttrType::String},
        newdb::AttrMeta{"age", newdb::AttrType::String},
        newdb::AttrMeta{"salary", newdb::AttrType::String},
    };
    newdb::WalRecoveryStats st{};
    ASSERT_TRUE(wal.recover(&table, schema, &st).ok);
    ASSERT_GE(table.logical_row_count(), 1u);

    wal.close();
    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST(WalRecoveryIndexed, OrphanCheckpointEndWithoutBeginStillAdvancesLastComplete) {
    namespace fs = std::filesystem;
    const fs::path dir = unique_temp_subdir("wal_cp_orphan_end");
    const std::string db = "recovery_orphan_end";

    newdb::WalManager wal(db, dir.string());
    ASSERT_TRUE(wal.open().ok);
    // No matching BEGIN: recover_track_checkpoint treats END with depth==0 as advancing boundary.
    ASSERT_TRUE(wal.checkpoint_end(42).ok);
    ASSERT_TRUE(wal.flush().ok);

    newdb::WalRecoveryStats st{};
    newdb::HeapTable table;
    table.name = "t";
    newdb::TableSchema schema;
    ASSERT_TRUE(wal.recover(&table, schema, &st).ok);
    EXPECT_GE(st.last_complete_checkpoint_lsn, 1u);
    EXPECT_EQ(st.checkpoint_midpoint_recovery_count, 0u);

    wal.close();
    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST(WalRecoveryIndexed, TwoCheckpointBeginsWithoutEndCountsDoubleMidpoint) {
    namespace fs = std::filesystem;
    const fs::path dir = unique_temp_subdir("wal_cp_double_mid");
    const std::string db = "recovery_double_mid";

    newdb::WalManager wal(db, dir.string());
    ASSERT_TRUE(wal.open().ok);
    ASSERT_TRUE(wal.checkpoint_begin(1).ok);
    ASSERT_TRUE(wal.checkpoint_begin(2).ok);
    ASSERT_TRUE(wal.flush().ok);

    newdb::WalRecoveryStats st{};
    newdb::HeapTable table;
    table.name = "t";
    newdb::TableSchema schema;
    ASSERT_TRUE(wal.recover(&table, schema, &st).ok);
    EXPECT_GE(st.checkpoint_midpoint_recovery_count, 2u);

    wal.close();
    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST(WalRecoveryIndexed, MinReplayLsnCanRaiseStartAboveCheckpoint) {
    namespace fs = std::filesystem;
    const fs::path dir = unique_temp_subdir("wal_min_lsn");
    const std::string db = "recovery_min_lsn";

    unset_test_env("NEWDB_RECOVER_MIN_LSN");
    unset_test_env("NEWDB_RECOVER_USE_CHECKPOINT_LSN");

    newdb::WalManager wal(db, dir.string());
    wal.set_segment_max_bytes(512);
    ASSERT_TRUE(wal.open().ok);

    newdb::Row r;
    r.attrs["name"] = "u";
    r.attrs["dept"] = "ENG";
    r.attrs["age"] = "30";
    r.attrs["salary"] = "100";

    // Empty WAL may have current_lsn==0; snapshot 0 keeps last_complete_checkpoint_lsn at 0.
    const std::uint64_t cp_snap = std::max<std::uint64_t>(1, wal.current_lsn());
    ASSERT_TRUE(wal.checkpoint(cp_snap).ok);
    for (int i = 0; i < 5; ++i) {
        r.id = i + 1;
        ASSERT_TRUE(wal.begin_transaction(static_cast<std::uint64_t>(i + 1)).ok);
        ASSERT_TRUE(wal.append_record(static_cast<std::uint64_t>(i + 1), newdb::WalOp::INSERT, "users", &r).ok);
        ASSERT_TRUE(wal.commit_transaction(static_cast<std::uint64_t>(i + 1)).ok);
    }
    ASSERT_TRUE(wal.flush().ok);

    newdb::WalRecoveryStats st0{};
    {
        ScopedEnv e_off("NEWDB_RECOVER_USE_CHECKPOINT_LSN", "0");
        newdb::HeapTable table;
        table.name = "users";
        newdb::TableSchema schema;
        ASSERT_TRUE(wal.recover(&table, schema, &st0).ok);
    }
    ASSERT_GT(st0.last_complete_checkpoint_lsn, 0u);

    std::uint64_t high_min = st0.last_complete_checkpoint_lsn + 1;
    {
        ScopedEnv e_min("NEWDB_RECOVER_MIN_LSN", std::to_string(high_min).c_str());
        newdb::WalRecoveryStats st1{};
        newdb::HeapTable table;
        table.name = "users";
        newdb::TableSchema schema;
        ASSERT_TRUE(wal.recover(&table, schema, &st1).ok);
        EXPECT_GE(st1.replay_start_lsn, high_min);
    }

    wal.close();
    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST(WalRecoveryIndexed, MultiSegmentWalWithTrailingIncompleteCheckpoint) {
    namespace fs = std::filesystem;
    const fs::path dir = unique_temp_subdir("wal_multi_seg_cp");
    const std::string db = "recovery_multi_seg_cp";

    newdb::WalManager wal(db, dir.string());
    wal.set_segment_max_bytes(128);
    ASSERT_TRUE(wal.open().ok);

    newdb::Row r;
    r.attrs["name"] = "u";
    r.attrs["dept"] = "ENG";
    r.attrs["age"] = "30";
    r.attrs["salary"] = "100";

    for (int i = 0; i < 35; ++i) {
        r.id = i + 1;
        ASSERT_TRUE(wal.begin_transaction(static_cast<std::uint64_t>(i + 1)).ok);
        ASSERT_TRUE(wal.append_record(static_cast<std::uint64_t>(i + 1), newdb::WalOp::INSERT, "users", &r).ok);
        ASSERT_TRUE(wal.commit_transaction(static_cast<std::uint64_t>(i + 1)).ok);
    }
    ASSERT_TRUE(wal.checkpoint_begin(999).ok);
    ASSERT_TRUE(wal.flush().ok);

    newdb::HeapTable table;
    table.name = "users";
    newdb::TableSchema schema;
    schema.primary_key = "id";
    schema.attrs = {
        newdb::AttrMeta{"name", newdb::AttrType::String},
        newdb::AttrMeta{"dept", newdb::AttrType::String},
        newdb::AttrMeta{"age", newdb::AttrType::String},
        newdb::AttrMeta{"salary", newdb::AttrType::String},
    };
    newdb::WalRecoveryStats st{};
    ASSERT_TRUE(wal.recover(&table, schema, &st).ok);
    EXPECT_GE(table.logical_row_count(), 35u);
    EXPECT_GE(st.checkpoint_midpoint_recovery_count, 1u);
    EXPECT_GE(st.indexed_segments, 1u);

    wal.close();
    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST(WalRecoveryIndexed, CollectsSegmentIndexStats) {
    namespace fs = std::filesystem;
    const fs::path dir = unique_temp_subdir("wal_recovery_idx");
    const std::string db = "recovery_idx";

    newdb::WalManager wal(db, dir.string());
    wal.set_segment_max_bytes(512);
    ASSERT_TRUE(wal.open().ok);

    newdb::Row r;
    r.id = 1;
    r.attrs["name"] = "u1";
    r.attrs["dept"] = "ENG";
    r.attrs["age"] = "30";
    r.attrs["salary"] = "100";
    for (int i = 0; i < 40; ++i) {
        r.id = i + 1;
        ASSERT_TRUE(wal.begin_transaction(static_cast<std::uint64_t>(i + 1)).ok);
        ASSERT_TRUE(wal.append_record(static_cast<std::uint64_t>(i + 1), newdb::WalOp::INSERT, "users", &r).ok);
        ASSERT_TRUE(wal.commit_transaction(static_cast<std::uint64_t>(i + 1)).ok);
    }
    ASSERT_TRUE(wal.flush().ok);

    newdb::HeapTable table;
    table.name = "users";
    newdb::TableSchema schema;
    newdb::WalRecoveryStats stats;
    ASSERT_TRUE(wal.recover(&table, schema, &stats).ok);
    EXPECT_GT(stats.indexed_segments, 0u);
    EXPECT_GT(stats.indexed_records, 0u);
    EXPECT_GT(stats.indexed_offsets, 0u);
    EXPECT_GT(stats.scanned_segments, 0u);

    const auto last = wal.last_recovery_stats();
    ASSERT_TRUE(last.has_value());
    EXPECT_EQ(last->indexed_segments, stats.indexed_segments);

    wal.close();
    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST(WalRecoveryIndexed, UncommittedTxnDiscardedAndRecoveryPolicySet) {
    namespace fs = std::filesystem;
    const fs::path dir = unique_temp_subdir("wal_uncommitted");
    const std::string db = "recovery_uc";
    unset_test_env("NEWDB_RECOVER_ENABLE_OFFSET_SEEK");
    unset_test_env("NEWDB_RECOVER_USE_CHECKPOINT_LSN");
    unset_test_env("NEWDB_RECOVER_MIN_LSN");

    newdb::WalManager wal(db, dir.string());
    wal.set_segment_max_bytes(4096);
    ASSERT_TRUE(wal.open().ok);

    newdb::Row r;
    r.attrs["name"] = "u";
    r.attrs["dept"] = "ENG";
    r.attrs["age"] = "30";
    r.attrs["salary"] = "100";
    r.id = 1;
    ASSERT_TRUE(wal.begin_transaction(1).ok);
    ASSERT_TRUE(wal.append_record(1, newdb::WalOp::INSERT, "users", &r).ok);
    ASSERT_TRUE(wal.commit_transaction(1).ok);
    r.id = 2;
    ASSERT_TRUE(wal.begin_transaction(2).ok);
    ASSERT_TRUE(wal.append_record(2, newdb::WalOp::INSERT, "users", &r).ok);
    ASSERT_TRUE(wal.flush().ok);

    newdb::HeapTable table;
    table.name = "users";
    newdb::TableSchema schema;
    schema.primary_key = "id";
    schema.attrs = {
        newdb::AttrMeta{"name", newdb::AttrType::String},
        newdb::AttrMeta{"dept", newdb::AttrType::String},
        newdb::AttrMeta{"age", newdb::AttrType::String},
        newdb::AttrMeta{"salary", newdb::AttrType::String},
    };
    newdb::WalRecoveryStats stats{};
    ASSERT_TRUE(wal.recover(&table, schema, &stats).ok);
    EXPECT_GE(stats.uncommitted_txn_discarded_count, 1u);
    EXPECT_GE(stats.recovery_uncommitted_records_ignored, 1u);
    EXPECT_EQ(stats.recovery_uncommitted_records_ignored, stats.uncommitted_txn_discarded_count);
    EXPECT_FALSE(stats.recovery_policy.empty());

    wal.close();
    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST(WalRecoveryIndexed, CheckpointTruncatePruneDisabledSkipsTruncate) {
    namespace fs = std::filesystem;
    const fs::path dir = unique_temp_subdir("wal_cp_prune0");
    const std::string db = "wal_prune0";

    newdb::WalManager wal(db, dir.string());
    ASSERT_TRUE(wal.open().ok);
    ASSERT_TRUE(wal.begin_transaction(1).ok);
    newdb::Row r;
    r.id = 1;
    r.attrs["k"] = "v";
    ASSERT_TRUE(wal.append_record(1, newdb::WalOp::INSERT, "t", &r).ok);
    ASSERT_TRUE(wal.commit_transaction(1).ok);
    ASSERT_TRUE(wal.flush().ok);
    ASSERT_GT(wal.wal_file_size_bytes(), 0u);

    {
        ScopedEnv e("NEWDB_WAL_CHECKPOINT_PRUNE", "0");
        ASSERT_TRUE(wal.checkpoint_and_truncate(std::max<std::uint64_t>(1, wal.current_lsn())).ok);
    }
    const auto tr = wal.last_checkpoint_truncate_trace();
    ASSERT_TRUE(tr.has_value());
    EXPECT_TRUE(tr->skipped_prune_disabled);
    EXPECT_FALSE(tr->truncate_executed);
    EXPECT_GT(wal.wal_file_size_bytes(), 0u);

    wal.close();
    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST(WalRecoveryIndexed, SegmentIndexCountsPartialTailStopOnCleanEof) {
    namespace fs = std::filesystem;
    const fs::path dir = unique_temp_subdir("wal_partial_tail_eof");
    const std::string db = "recovery_partial_eof";

    newdb::WalManager wal(db, dir.string());
    ASSERT_TRUE(wal.open().ok);
    newdb::Row r;
    r.id = 1;
    r.attrs["name"] = "ok";
    r.attrs["dept"] = "ENG";
    r.attrs["age"] = "30";
    r.attrs["salary"] = "100";
    ASSERT_TRUE(wal.begin_transaction(1).ok);
    ASSERT_TRUE(wal.append_record(1, newdb::WalOp::INSERT, "users", &r).ok);
    ASSERT_TRUE(wal.commit_transaction(1).ok);
    ASSERT_TRUE(wal.flush().ok);

    newdb::HeapTable table;
    table.name = "users";
    newdb::TableSchema schema;
    schema.primary_key = "id";
    schema.attrs = {
        newdb::AttrMeta{"name", newdb::AttrType::String},
        newdb::AttrMeta{"dept", newdb::AttrType::String},
        newdb::AttrMeta{"age", newdb::AttrType::String},
        newdb::AttrMeta{"salary", newdb::AttrType::String},
    };
    newdb::WalRecoveryStats st{};
    ASSERT_TRUE(wal.recover(&table, schema, &st).ok);
    EXPECT_GE(st.segment_index_partial_tail_stops, 1u);
    EXPECT_EQ(st.segment_index_bad_header_stops, 0u);
    EXPECT_GE(table.logical_row_count(), 1u);

    wal.close();
    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST(WalRecoveryIndexed, BadWalMagicTailAfterCommitStillRecoversCommittedRow) {
    namespace fs = std::filesystem;
    const fs::path dir = unique_temp_subdir("wal_bad_magic_tail");
    const std::string db = "recovery_bad_magic_tail";

    std::string wal_path;
    {
        newdb::WalManager wal(db, dir.string());
        ASSERT_TRUE(wal.open().ok);
        newdb::Row r;
        r.id = 42;
        r.attrs["name"] = "v";
        r.attrs["dept"] = "ENG";
        r.attrs["age"] = "30";
        r.attrs["salary"] = "100";
        ASSERT_TRUE(wal.begin_transaction(1).ok);
        ASSERT_TRUE(wal.append_record(1, newdb::WalOp::INSERT, "users", &r).ok);
        ASSERT_TRUE(wal.commit_transaction(1).ok);
        ASSERT_TRUE(wal.flush().ok);
        wal_path = wal.wal_path();
        wal.close();
    }
    {
        std::ofstream tail(wal_path, std::ios::binary | std::ios::app);
        ASSERT_TRUE(tail.good());
        char buf[24]{};
        tail.write(buf, sizeof(buf));
    }

    newdb::WalManager wal2(db, dir.string());
    newdb::HeapTable table;
    table.name = "users";
    newdb::TableSchema schema;
    schema.primary_key = "id";
    schema.attrs = {
        newdb::AttrMeta{"name", newdb::AttrType::String},
        newdb::AttrMeta{"dept", newdb::AttrType::String},
        newdb::AttrMeta{"age", newdb::AttrType::String},
        newdb::AttrMeta{"salary", newdb::AttrType::String},
    };
    newdb::WalRecoveryStats st{};
    ASSERT_TRUE(wal2.recover(&table, schema, &st).ok);
    EXPECT_GE(st.segment_index_bad_header_stops + st.segment_index_partial_tail_stops, 1u);
    ASSERT_GE(table.logical_row_count(), 1u);
    const newdb::Row* got = table.find_by_id(42);
    ASSERT_NE(got, nullptr);
    EXPECT_EQ(got->attrs.at("name"), "v");

    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST(WalRecoveryIndexed, CheckpointTruncateStrictFailsWithOpenCheckpointBracket) {
    namespace fs = std::filesystem;
    const fs::path dir = unique_temp_subdir("wal_cp_strict");
    const std::string db = "wal_strict";

    newdb::WalManager wal(db, dir.string());
    ASSERT_TRUE(wal.open().ok);
    ASSERT_TRUE(wal.checkpoint_begin(1).ok);
    ASSERT_TRUE(wal.flush().ok);

    {
        ScopedEnv e("NEWDB_WAL_CHECKPOINT_STRICT", "1");
        EXPECT_FALSE(wal.checkpoint_and_truncate(2).ok);
    }
    const auto tr = wal.last_checkpoint_truncate_trace();
    ASSERT_TRUE(tr.has_value());
    EXPECT_TRUE(tr->failed_strict_incomplete_bracket);
    EXPECT_NE(tr->tail_checkpoint_bracket_depth, 0u);
    EXPECT_FALSE(tr->truncate_executed);

    wal.close();
    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST(WalRecoveryIndexed, MultiSegmentMiddleCorruptedReplay) {
    namespace fs = std::filesystem;
    const fs::path dir = unique_temp_subdir("wal_mid_corrupt");
    const std::string db = "recovery_mid_corrupt";

    std::vector<std::string> seg_paths;
    {
        newdb::WalManager wal(db, dir.string());
        wal.set_segment_max_bytes(96);
        ASSERT_TRUE(wal.open().ok);
        newdb::Row r;
        r.attrs["name"] = "u";
        r.attrs["dept"] = "ENG";
        r.attrs["age"] = "30";
        r.attrs["salary"] = "100";
        for (int i = 0; i < 60; ++i) {
            r.id = i + 1;
            const auto txn = static_cast<std::uint64_t>(i + 1);
            ASSERT_TRUE(wal.begin_transaction(txn).ok);
            ASSERT_TRUE(wal.append_record(txn, newdb::WalOp::INSERT, "users", &r).ok);
            ASSERT_TRUE(wal.commit_transaction(txn).ok);
        }
        ASSERT_TRUE(wal.flush().ok);
        std::error_code ec;
        for (const auto& entry : fs::directory_iterator(dir, ec)) {
            if (!entry.is_regular_file()) {
                continue;
            }
            const std::string fname = entry.path().filename().string();
            if (fname.find(".wal") == std::string::npos) {
                continue;
            }
            if (fname.find(".wal_lsn") != std::string::npos ||
                fname.find(".walsync") != std::string::npos) {
                continue;
            }
            seg_paths.push_back(entry.path().string());
        }
        wal.close();
    }
    std::sort(seg_paths.begin(), seg_paths.end());
    ASSERT_GE(seg_paths.size(), 3u);
    {
        std::fstream f(seg_paths[1], std::ios::in | std::ios::out | std::ios::binary);
        ASSERT_TRUE(f.good());
        f.seekg(0, std::ios::end);
        const auto sz = f.tellg();
        ASSERT_GE(sz, std::streamoff{8});
        f.seekp(static_cast<std::streamoff>(sz) - 8);
        char flip[8];
        f.read(flip, sizeof(flip));
        f.clear();
        for (auto& c : flip) {
            c = static_cast<char>(static_cast<unsigned char>(c) ^ 0xFFu);
        }
        f.seekp(static_cast<std::streamoff>(sz) - 8);
        f.write(flip, sizeof(flip));
    }

    newdb::WalManager wal2(db, dir.string());
    newdb::HeapTable table;
    table.name = "users";
    newdb::TableSchema schema;
    schema.primary_key = "id";
    schema.attrs = {
        newdb::AttrMeta{"name", newdb::AttrType::String},
        newdb::AttrMeta{"dept", newdb::AttrType::String},
        newdb::AttrMeta{"age", newdb::AttrType::String},
        newdb::AttrMeta{"salary", newdb::AttrType::String},
    };
    newdb::WalRecoveryStats st{};
    (void)wal2.recover(&table, schema, &st);
    EXPECT_GE(st.checksum_failures + st.segment_index_partial_tail_stops + st.segment_index_bad_header_stops,
              1u);
    EXPECT_GE(st.apply_count, 1u);
    EXPECT_GE(table.logical_row_count(), 1u);

    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST(WalRecoveryIndexed, CrossSegmentUncommittedTxnDangling) {
    namespace fs = std::filesystem;
    const fs::path dir = unique_temp_subdir("wal_cross_seg_uc");
    const std::string db = "recovery_cross_seg_uc";

    newdb::WalManager wal(db, dir.string());
    wal.set_segment_max_bytes(96);
    ASSERT_TRUE(wal.open().ok);
    newdb::Row r;
    r.attrs["name"] = "open";
    r.attrs["dept"] = "ENG";
    r.attrs["age"] = "1";
    r.attrs["salary"] = "0";
    const std::uint64_t txn = 7777;
    ASSERT_TRUE(wal.begin_transaction(txn).ok);
    for (int i = 0; i < 12; ++i) {
        r.id = 1000 + i;
        ASSERT_TRUE(wal.append_record(txn, newdb::WalOp::INSERT, "users", &r).ok);
    }
    ASSERT_TRUE(wal.flush().ok);
    wal.close();

    newdb::WalManager wal2(db, dir.string());
    newdb::HeapTable table;
    table.name = "users";
    newdb::TableSchema schema;
    schema.primary_key = "id";
    schema.attrs = {
        newdb::AttrMeta{"name", newdb::AttrType::String},
        newdb::AttrMeta{"dept", newdb::AttrType::String},
        newdb::AttrMeta{"age", newdb::AttrType::String},
        newdb::AttrMeta{"salary", newdb::AttrType::String},
    };
    newdb::WalRecoveryStats st{};
    ASSERT_TRUE(wal2.recover(&table, schema, &st).ok);
    EXPECT_GE(st.uncommitted_txn_discarded_count, 1u);
    EXPECT_EQ(st.apply_count, 0u);
    EXPECT_EQ(table.logical_row_count(), 0u);

    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST(WalRecoveryIndexed, CheckpointBeginNoEndKeepsCpDepth) {
    namespace fs = std::filesystem;
    const fs::path dir = unique_temp_subdir("wal_cp_begin_no_end");
    const std::string db = "recovery_cp_begin_no_end";

    newdb::WalManager wal(db, dir.string());
    ASSERT_TRUE(wal.open().ok);
    ASSERT_TRUE(wal.checkpoint_begin(42).ok);
    ASSERT_TRUE(wal.flush().ok);

    newdb::HeapTable table;
    table.name = "users";
    newdb::TableSchema schema;
    schema.primary_key = "id";
    newdb::WalRecoveryStats st{};
    ASSERT_TRUE(wal.recover(&table, schema, &st).ok);
    EXPECT_GE(st.incomplete_checkpoint_count, 1u);
    EXPECT_GE(st.checkpoint_midpoint_recovery_count, 1u);

    wal.close();
    std::error_code ec;
    fs::remove_all(dir, ec);
}

TEST(WalRecoveryPipeline, CaptureScanStatsAndSummary) {
    namespace fs = std::filesystem;
    const fs::path dir = unique_temp_subdir("wal_rec_pipe");
    newdb::WalManager wal("dbpipe", dir.string());
    ASSERT_TRUE(wal.open().ok);
    newdb::WalRecoveryStats st{};
    ASSERT_TRUE(wal.capture_recovery_scan_stats(&st).ok);
    const auto sum = newdb::wal_recovery::summarize_recovery_stats(st);
    (void)sum;
    newdb::wal_recovery::WalRecordReader reader(wal);
    (void)reader;
    wal.close();
    std::error_code ec;
    fs::remove_all(dir, ec);
}
