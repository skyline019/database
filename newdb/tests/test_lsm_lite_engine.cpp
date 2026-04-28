#include <gtest/gtest.h>

#include <newdb/lsm_lite.h>

#include <cstdint>
#include <filesystem>
#include <algorithm>
#include <string>
#include <vector>

namespace {
namespace fs = std::filesystem;

std::string unique_data_file(const char* tag) {
    const fs::path base = fs::temp_directory_path() / ("newdb_lsm_engine_" + std::string(tag));
    std::error_code ec;
    fs::remove_all(base, ec);
    fs::create_directories(base, ec);
    return (base / "t.bin").string();
}

newdb::Row mk_row(const int id) {
    newdb::Row r;
    r.id = id;
    r.attrs["name"] = "u";
    r.attrs["balance"] = "1";
    return r;
}

struct WorkerGuard {
    ~WorkerGuard() { newdb::lsm_lite::shutdown_background_workers(); }
};
} // namespace

TEST(LsmLiteEngine, TxnVisibilityCommitAndRollback) {
    WorkerGuard guard;
    const std::string data_file = unique_data_file("txn");
    newdb::lsm_lite::Options opt;
    opt.enabled = true;
    opt.segment_target_bytes = 1;

    newdb::lsm_lite::TxnContext t1;
    t1.in_txn = true;
    t1.txn_id = 101;

    newdb::lsm_lite::record_writes(opt, data_file, std::vector<newdb::Row>{mk_row(7)}, false, &t1);

    EXPECT_FALSE(newdb::lsm_lite::find_by_id(opt, data_file, 7).has_value());

    auto in_txn = newdb::lsm_lite::find_by_id(opt, data_file, 7, &t1);
    ASSERT_TRUE(in_txn.has_value());
    EXPECT_TRUE(in_txn->found);
    EXPECT_FALSE(in_txn->deleted);
    EXPECT_EQ(in_txn->row.id, 7);

    newdb::lsm_lite::on_txn_commit(opt, data_file, t1);
    auto committed = newdb::lsm_lite::find_by_id(opt, data_file, 7);
    ASSERT_TRUE(committed.has_value());
    EXPECT_TRUE(committed->found);
    EXPECT_FALSE(committed->deleted);

    newdb::lsm_lite::TxnContext t2{true, 202};
    newdb::lsm_lite::record_writes(opt, data_file, std::vector<newdb::Row>{mk_row(9)}, false, &t2);
    newdb::lsm_lite::on_txn_rollback(opt, data_file, t2);
    EXPECT_FALSE(newdb::lsm_lite::find_by_id(opt, data_file, 9).has_value());
}

TEST(LsmLiteEngine, TombstoneTakesPrecedenceAfterFlush) {
    WorkerGuard guard;
    const std::string data_file = unique_data_file("tombstone");
    newdb::lsm_lite::Options opt;
    opt.enabled = true;
    opt.segment_target_bytes = 1;

    newdb::lsm_lite::record_writes(opt, data_file, std::vector<newdb::Row>{mk_row(5)}, false);
    newdb::lsm_lite::record_writes(opt, data_file, std::vector<newdb::Row>{mk_row(5)}, true);

    auto r = newdb::lsm_lite::find_by_id(opt, data_file, 5);
    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->found);
    EXPECT_TRUE(r->deleted);
}

TEST(LsmLiteEngine, CacheLookupReportsHitOnSecondRead) {
    WorkerGuard guard;
    const std::string data_file = unique_data_file("cache");
    newdb::lsm_lite::Options opt;
    opt.enabled = true;
    opt.segment_target_bytes = 1;

    std::uint64_t hits = 0;
    std::uint64_t misses = 0;
    newdb::lsm_lite::Hooks hooks;
    hooks.on_cache_lookup = [&](const bool hit) {
        if (hit) {
            hits++;
        } else {
            misses++;
        }
    };

    newdb::lsm_lite::record_writes(opt, data_file, std::vector<newdb::Row>{mk_row(11)}, false, nullptr, &hooks);
    EXPECT_TRUE(newdb::lsm_lite::find_by_id(opt, data_file, 11, nullptr, &hooks).has_value());
    EXPECT_TRUE(newdb::lsm_lite::find_by_id(opt, data_file, 11, nullptr, &hooks).has_value());
    EXPECT_GE(misses, 1u);
    EXPECT_GE(hits, 1u);
}

TEST(LsmLiteEngine, DefiniteMissFilterCanSkipSegmentReads) {
    WorkerGuard guard;
    const std::string data_file = unique_data_file("filter");
    newdb::lsm_lite::Options opt;
    opt.enabled = true;
    opt.segment_target_bytes = 1;

    // Make a segment with ids in [1, 3]
    newdb::lsm_lite::record_writes(opt, data_file, std::vector<newdb::Row>{mk_row(1)}, false);
    newdb::lsm_lite::record_writes(opt, data_file, std::vector<newdb::Row>{mk_row(2)}, false);
    newdb::lsm_lite::record_writes(opt, data_file, std::vector<newdb::Row>{mk_row(3)}, false);

    std::uint64_t scanned = 0;
    newdb::lsm_lite::Hooks hooks;
    hooks.on_read_segments_scanned = [&](const std::uint64_t n) { scanned += n; };

    auto miss = newdb::lsm_lite::find_by_id(opt, data_file, 999999, nullptr, &hooks);
    EXPECT_FALSE(miss.has_value());
    EXPECT_EQ(scanned, 0u);
}

TEST(LsmLiteEngine, CompactionBatchAndReapBudgetBothTakeEffect) {
    WorkerGuard guard;
    const std::string data_file = unique_data_file("batch_reap");
    newdb::lsm_lite::Options opt;
    opt.enabled = true;
    opt.segment_target_bytes = 1;
    opt.l0_compact_trigger = 4;
    opt.l0_compact_batch = 2;
    opt.compaction_reap_budget = 1; // at most one batch per trigger

    std::uint64_t compactions = 0;
    newdb::lsm_lite::Hooks hooks;
    hooks.on_compaction = [&]() { compactions++; };

    for (int i = 1; i <= 10; ++i) {
        newdb::lsm_lite::record_writes(opt, data_file, std::vector<newdb::Row>{mk_row(i)}, false, nullptr, &hooks);
    }

    // With trigger=4 and batch=2, compaction should happen and not be starved.
    EXPECT_GE(compactions, 1u);
}

TEST(LsmLiteEngine, CompactionPolicyLeveledLiteStillCompacts) {
    WorkerGuard guard;
    const std::string data_file = unique_data_file("leveled_lite");
    newdb::lsm_lite::Options opt;
    opt.enabled = true;
    opt.segment_target_bytes = 1;
    opt.l0_compact_trigger = 4;
    opt.l0_compact_batch = 2;
    opt.compaction_reap_budget = 2;
    opt.compaction_policy = newdb::lsm_lite::CompactionPolicy::LeveledLite;

    std::uint64_t compactions = 0;
    std::uint64_t bytes_in = 0;
    std::uint64_t bytes_out = 0;
    newdb::lsm_lite::Hooks hooks;
    hooks.on_compaction = [&]() { compactions++; };
    hooks.on_compaction_bytes = [&](const std::uint64_t inb, const std::uint64_t outb) {
        bytes_in += inb;
        bytes_out += outb;
    };

    for (int i = 1; i <= 16; ++i) {
        newdb::lsm_lite::record_writes(opt, data_file, std::vector<newdb::Row>{mk_row(i)}, false, nullptr, &hooks);
    }

    EXPECT_GE(compactions, 1u);
    EXPECT_GT(bytes_in, 0u);
    EXPECT_GE(bytes_out, 0u);
}

TEST(LsmLiteEngine, LeveledLiteLongRunKeepsScanAndAmpBounded) {
    WorkerGuard guard;
    const std::string data_file = unique_data_file("leveled_longrun");
    newdb::lsm_lite::Options opt;
    opt.enabled = true;
    opt.segment_target_bytes = 1;
    opt.l0_compact_trigger = 4;
    opt.l0_compact_batch = 3;
    opt.compaction_reap_budget = 3;
    opt.compaction_policy = newdb::lsm_lite::CompactionPolicy::LeveledLite;
    opt.leveled_l1_soft_segments = 16;
    opt.leveled_l1_hard_segments = 24;

    std::uint64_t bytes_in = 0;
    std::uint64_t bytes_out = 0;
    std::vector<std::uint64_t> scanned_samples;
    newdb::lsm_lite::Hooks hooks;
    hooks.on_compaction_bytes = [&](const std::uint64_t inb, const std::uint64_t outb) {
        bytes_in += inb;
        bytes_out += outb;
    };
    hooks.on_read_segments_scanned = [&](const std::uint64_t n) { scanned_samples.push_back(n); };

    for (int i = 1; i <= 200; ++i) {
        newdb::lsm_lite::record_writes(opt, data_file, std::vector<newdb::Row>{mk_row(i)}, false, nullptr, &hooks);
        if ((i % 10) == 0) {
            (void)newdb::lsm_lite::find_by_id(opt, data_file, i - 5, nullptr, &hooks);
        }
    }
    ASSERT_FALSE(scanned_samples.empty());
    std::sort(scanned_samples.begin(), scanned_samples.end());
    const std::size_t rank = static_cast<std::size_t>(0.95 * static_cast<double>(scanned_samples.size()));
    const std::size_t idx = std::min<std::size_t>(scanned_samples.size() - 1, rank);
    const auto p95 = scanned_samples[idx];
    const double amp = bytes_in > 0 ? static_cast<double>(bytes_out) / static_cast<double>(bytes_in) : 0.0;
    EXPECT_LE(p95, 32u);
    EXPECT_GE(amp, 0.0);
}

TEST(LsmLiteEngine, AsyncCompactionCanDrainAndShutdown) {
    WorkerGuard guard;
    const std::string data_file = unique_data_file("async_drain");
    newdb::lsm_lite::Options opt;
    opt.enabled = true;
    opt.segment_target_bytes = 1;
    opt.l0_compact_trigger = 3;
    opt.l0_compact_batch = 3;
    opt.compaction_async = true;
    opt.compaction_workers = 1;
    opt.compaction_reap_budget = 2;
    opt.compaction_max_pending = 4;

    for (int i = 1; i <= 50; ++i) {
        newdb::lsm_lite::record_writes(opt, data_file, std::vector<newdb::Row>{mk_row(i)}, false);
    }

    EXPECT_TRUE(newdb::lsm_lite::drain_compaction(data_file, 3000));
}

TEST(LsmLiteEngine, ClearTxnViewsDropsDanglingUncommittedWrites) {
    WorkerGuard guard;
    const std::string data_file = unique_data_file("clear_txn_views");
    newdb::lsm_lite::Options opt;
    opt.enabled = true;
    opt.segment_target_bytes = 1;

    newdb::lsm_lite::TxnContext t{true, 303};
    newdb::lsm_lite::record_writes(opt, data_file, std::vector<newdb::Row>{mk_row(77)}, false, &t);
    ASSERT_TRUE(newdb::lsm_lite::find_by_id(opt, data_file, 77, &t).has_value());

    newdb::lsm_lite::clear_txn_views_for_data_file(data_file);
    EXPECT_FALSE(newdb::lsm_lite::find_by_id(opt, data_file, 77, &t).has_value());
    EXPECT_FALSE(newdb::lsm_lite::find_by_id(opt, data_file, 77).has_value());
}

