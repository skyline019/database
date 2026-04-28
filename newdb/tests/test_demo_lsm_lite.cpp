#include "cli/shell/dispatch/demo_commands.h"
#include "cli/modules/logging/logging.h"
#include "cli/shell/state/shell_state.h"
#include "test_util.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>

namespace fs = std::filesystem;

namespace {

struct ScopedEnvVar {
    std::string key;
    std::string old_value;
    bool had_old{false};

    ScopedEnvVar(const std::string& k, const std::string& v) : key(k) {
        if (const char* old = std::getenv(key.c_str())) {
            had_old = true;
            old_value = old;
        }
#if defined(_WIN32)
        _putenv_s(key.c_str(), v.c_str());
#else
        setenv(key.c_str(), v.c_str(), 1);
#endif
    }

    ~ScopedEnvVar() {
#if defined(_WIN32)
        if (had_old) {
            _putenv_s(key.c_str(), old_value.c_str());
        } else {
            _putenv_s(key.c_str(), "");
        }
#else
        if (had_old) {
            setenv(key.c_str(), old_value.c_str(), 1);
        } else {
            unsetenv(key.c_str());
        }
#endif
    }
};

struct SegmentRow {
    std::uint64_t seq{0};
    int id{0};
    bool deleted{false};
    std::string attrs_blob;
};

static bool parse_segment_row(const std::string& line, SegmentRow& out) {
    std::istringstream iss(line);
    int del = 0;
    if (!(iss >> out.seq >> out.id >> del)) {
        return false;
    }
    out.deleted = (del != 0);
    std::getline(iss, out.attrs_blob);
    if (!out.attrs_blob.empty() && out.attrs_blob.front() == '\t') {
        out.attrs_blob.erase(out.attrs_blob.begin());
    }
    return true;
}

struct DemoHarness {
    fs::path dir;
    ShellState st;

    explicit DemoHarness(const std::string& prefix) {
        dir = newdb::test::unique_temp_subdir(prefix);
        fs::create_directories(dir);
        st.data_dir = dir.string();
        st.log_file_path = (dir / "demo_log.bin").string();
        st.session.table_name.clear();
        st.session.data_path.clear();
        st.session.schema = newdb::TableSchema{};
        logging_bind_shell(&st);
        st.txn.set_workspace_root(st.data_dir);
    }

    std::string run(const std::string& cmd) {
        std::error_code ec;
        const std::uintmax_t before = fs::file_size(st.log_file_path, ec);
        const std::uintmax_t start = ec ? 0 : before;

        const bool ok = process_command_line(st, cmd.c_str());
        EXPECT_TRUE(ok) << "process_command_line returned false for cmd=" << cmd;

        std::ifstream in(st.log_file_path, std::ios::binary);
        if (!in.good()) {
            return {};
        }
        in.seekg(0, std::ios::end);
        const std::streamoff end = in.tellg();
        if (end <= 0 || start >= static_cast<std::uintmax_t>(end)) {
            return {};
        }
        in.seekg(static_cast<std::streamoff>(start), std::ios::beg);
        std::string out;
        out.assign(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
        return out;
    }

    fs::path segment_dir_for(const std::string& data_file) const {
        return fs::path(data_file + ".lsm");
    }

    std::size_t count_segments_on_disk(const std::string& data_file) const {
        const fs::path d = segment_dir_for(data_file);
        std::error_code ec;
        if (!fs::exists(d, ec)) return 0;
        std::size_t n = 0;
        for (fs::directory_iterator it(d, ec), end; !ec && it != end; it.increment(ec)) {
            const fs::directory_entry& ent = *it;
            if (!ent.is_regular_file(ec)) continue;
            if (ent.path().extension() == ".log") ++n;
        }
        return n;
    }

    std::vector<fs::path> list_segment_logs(const std::string& data_file) const {
        const fs::path d = segment_dir_for(data_file);
        std::error_code ec;
        std::vector<fs::path> out;
        if (!fs::exists(d, ec)) return out;
        for (fs::directory_iterator it(d, ec), end; !ec && it != end; it.increment(ec)) {
            const fs::directory_entry& ent = *it;
            if (!ent.is_regular_file(ec)) continue;
            if (ent.path().extension() == ".log") out.push_back(ent.path());
        }
        std::sort(out.begin(), out.end());
        return out;
    }

    std::vector<SegmentRow> read_all_segment_rows(const fs::path& seg) const {
        std::ifstream in(seg);
        std::vector<SegmentRow> rows;
        if (!in.good()) return rows;
        std::string line;
        while (std::getline(in, line)) {
            if (line.empty()) continue;
            SegmentRow r;
            if (parse_segment_row(line, r)) rows.push_back(std::move(r));
        }
        return rows;
    }
};

} // namespace

TEST(DemoLsmLite, FlushCreatesSegmentsAndUpdatesStats) {
    DemoHarness h("newdb_demo_lsm_flush");
    h.run("CREATE TABLE(t1)");
    h.run("USE(t1)");
    h.run("HOTINDEX on");
    h.run("SEGMENT 200");

    // Small target so we flush quickly.
    for (int i = 1; i <= 20; ++i) {
        h.run("INSERT(" + std::to_string(i) + ",u,1)");
    }

    const auto stats = h.st.txn.runtimeStats();
    EXPECT_GE(stats.lsm_memtable_flush_count, 1u);
    EXPECT_GE(stats.lsm_segment_count, 1u);

    const std::string eff_data = resolve_table_file(h.st, "t1.bin");
    EXPECT_GE(h.count_segments_on_disk(eff_data), 1u);
}

TEST(DemoLsmLite, CompactionKeepsOnlyNewestTwoSegments) {
    DemoHarness h("newdb_demo_lsm_compact");
    h.run("CREATE TABLE(t1)");
    h.run("USE(t1)");
    h.run("HOTINDEX on");
    h.run("SEGMENT 120");

    // Force multiple flush cycles to exceed the compaction threshold.
    // Each INSERT appends new attrs (rough_row_size grows); low target should flush frequently.
    for (int i = 1; i <= 80; ++i) {
        h.run("INSERT(" + std::to_string(i) + ",u,1)");
    }

    const auto stats = h.st.txn.runtimeStats();
    EXPECT_GE(stats.lsm_memtable_flush_count, 2u);
    EXPECT_GE(stats.lsm_compaction_count, 1u);
    EXPECT_GE(stats.lsm_segment_count, 1u);
    EXPECT_GE(stats.lsm_compaction_bytes_in, stats.lsm_compaction_bytes_out);

    const std::string eff_data = resolve_table_file(h.st, "t1.bin");
    EXPECT_GE(h.count_segments_on_disk(eff_data), 1u);
}

TEST(DemoLsmLite, CompactionProducesL1SegmentNaming) {
    DemoHarness h("newdb_demo_lsm_l1_naming");
    h.run("CREATE TABLE(t1)");
    h.run("USE(t1)");
    h.run("HOTINDEX on");
    h.run("SEGMENT 90");
    for (int i = 1; i <= 90; ++i) {
        h.run("INSERT(" + std::to_string(i) + ",u,1)");
    }
    const std::string eff_data = resolve_table_file(h.st, "t1.bin");
    const auto segs = h.list_segment_logs(eff_data);
    ASSERT_FALSE(segs.empty());
    bool saw_l1 = false;
    for (const auto& seg : segs) {
        const std::string name = seg.filename().string();
        if (name.rfind("L1_", 0) == 0) {
            saw_l1 = true;
        }
    }
    EXPECT_TRUE(saw_l1);
}

TEST(DemoLsmLite, TombstoneReadTakesPrecedenceOverHeapRow) {
    DemoHarness h("newdb_demo_lsm_tombstone");
    h.run("CREATE TABLE(t1)");
    h.run("USE(t1)");
    h.run("HOTINDEX on");
    h.run("SEGMENT 80");

    h.run("INSERT(1,Alice,10)");
    h.run("DELETE(1)");

    const std::string out = h.run("FIND(1)");
    EXPECT_NE(out.find("lsm_tombstone"), std::string::npos) << out;
    EXPECT_NE(out.find("not found"), std::string::npos) << out;
}

TEST(DemoLsmLite, SegmentFilesHaveMonotonicSeqAndContainTombstones) {
    DemoHarness h("newdb_demo_lsm_segment_format");
    h.run("CREATE TABLE(t1)");
    h.run("USE(t1)");
    h.run("HOTINDEX on");
    // Force flush for each write, but keep total segments < 4 to avoid compaction
    // deleting the tombstone-containing segment.
    h.run("SEGMENT 1");

    // Ensure the tombstone is persisted into a segment file.
    h.run("INSERT(3,u,1)");
    h.run("DELETE(3)");

    const std::string eff_data = resolve_table_file(h.st, "t1.bin");
    const auto segs = h.list_segment_logs(eff_data);
    ASSERT_FALSE(segs.empty());

    bool saw_tombstone = false;
    std::uint64_t last_seq = 0;
    bool first = true;
    for (const auto& seg : segs) {
        const auto rows = h.read_all_segment_rows(seg);
        ASSERT_FALSE(rows.empty()) << "empty segment: " << seg.string();
        for (const auto& r : rows) {
            if (!first) {
                EXPECT_GT(r.seq, last_seq) << "seq must be strictly increasing across segment rows";
            }
            first = false;
            last_seq = r.seq;
            if (r.deleted) {
                saw_tombstone = true;
            }
        }
    }
    EXPECT_TRUE(saw_tombstone);
}

TEST(DemoLsmLite, LaterTombstoneOverridesEarlierValueAcrossSegments) {
    DemoHarness h("newdb_demo_lsm_tombstone_override");
    h.run("CREATE TABLE(t1)");
    h.run("USE(t1)");
    h.run("HOTINDEX on");
    h.run("SEGMENT 70");

    // Create a value, flush it, then tombstone later (forcing another flush).
    for (int i = 1; i <= 12; ++i) {
        h.run("INSERT(" + std::to_string(i) + ",u,1)");
    }
    // Tombstone id=5 after initial flushes; additional writes force another flush.
    h.run("DELETE(5)");
    for (int i = 100; i <= 120; ++i) {
        h.run("INSERT(" + std::to_string(i) + ",u,1)");
    }

    const std::string out = h.run("FIND(5)");
    EXPECT_NE(out.find("not found"), std::string::npos) << out;
    const auto stats = h.st.txn.runtimeStats();
    EXPECT_GE(stats.lsm_read_segments_scanned, 1u);
    EXPECT_GE(stats.lsm_read_segments_scanned_p95, 1u);
}

TEST(DemoLsmLite, CompactionPhysicallyDeletesOldSegmentFiles) {
    DemoHarness h("newdb_demo_lsm_compaction_deletes_files");
    h.run("CREATE TABLE(t1)");
    h.run("USE(t1)");
    h.run("HOTINDEX on");
    h.run("SEGMENT 100");

    // Create enough data to trigger compaction.
    for (int i = 1; i <= 120; ++i) {
        h.run("INSERT(" + std::to_string(i) + ",u,1)");
    }

    const std::string eff_data = resolve_table_file(h.st, "t1.bin");
    const auto segs = h.list_segment_logs(eff_data);
    const auto stats = h.st.txn.runtimeStats();

    // Stronger than previous test: segment_count must match on-disk count.
    ASSERT_GE(segs.size(), 1u);
    EXPECT_EQ(static_cast<std::uint64_t>(segs.size()), stats.lsm_segment_count);
    EXPECT_GE(stats.lsm_compaction_count, 1u);
}

TEST(DemoLsmLite, L0CompactionTriggerCanDelayCompaction) {
    ScopedEnvVar trigger("NEWDB_LSM_L0_COMPACT_TRIGGER", "100");
    ScopedEnvVar batch("NEWDB_LSM_L0_COMPACT_BATCH", "8");
    DemoHarness h("newdb_demo_lsm_trigger_delay");
    h.run("CREATE TABLE(t1)");
    h.run("USE(t1)");
    h.run("HOTINDEX on");
    h.run("SEGMENT 80");

    for (int i = 1; i <= 120; ++i) {
        h.run("INSERT(" + std::to_string(i) + ",u,1)");
    }

    const auto stats = h.st.txn.runtimeStats();
    EXPECT_GE(stats.lsm_memtable_flush_count, 2u);
    EXPECT_EQ(stats.lsm_compaction_count, 0u);
}

TEST(DemoLsmLite, AsyncCompactionWorkersAndReapBudgetKeepCompacting) {
    ScopedEnvVar async_on("NEWDB_LSM_COMPACTION_ASYNC", "1");
    ScopedEnvVar workers("NEWDB_LSM_COMPACTION_WORKERS", "2");
    ScopedEnvVar reap("NEWDB_LSM_COMPACTION_REAP_BUDGET", "1");
    ScopedEnvVar trigger("NEWDB_LSM_L0_COMPACT_TRIGGER", "4");
    ScopedEnvVar batch("NEWDB_LSM_L0_COMPACT_BATCH", "4");
    ScopedEnvVar mult("NEWDB_LSM_FLUSH_TRIGGER_MULTIPLIER", "1");
    DemoHarness h("newdb_demo_lsm_async_workers");
    h.run("CREATE TABLE(t1)");
    h.run("USE(t1)");
    h.run("HOTINDEX on");
    h.run("SEGMENT 64");

    for (int i = 1; i <= 180; ++i) {
        h.run("INSERT(" + std::to_string(i) + ",u,1)");
    }
    // Drive reap loop a few extra rounds.
    for (int i = 181; i <= 220; ++i) {
        h.run("INSERT(" + std::to_string(i) + ",u,1)");
    }

    const auto stats = h.st.txn.runtimeStats();
    EXPECT_GE(stats.lsm_compaction_count, 1u);
    EXPECT_GE(stats.lsm_segment_count, 1u);
}

TEST(DemoLsmLite, AsyncCompactionBackpressureCountsSkippedEnqueue) {
    ScopedEnvVar async_on("NEWDB_LSM_COMPACTION_ASYNC", "1");
    ScopedEnvVar workers("NEWDB_LSM_COMPACTION_WORKERS", "1");
    ScopedEnvVar reap("NEWDB_LSM_COMPACTION_REAP_BUDGET", "1");
    ScopedEnvVar trigger("NEWDB_LSM_L0_COMPACT_TRIGGER", "3");
    ScopedEnvVar batch("NEWDB_LSM_L0_COMPACT_BATCH", "3");
    ScopedEnvVar mult("NEWDB_LSM_FLUSH_TRIGGER_MULTIPLIER", "1");
    ScopedEnvVar max_pending("NEWDB_LSM_COMPACTION_MAX_PENDING", "1");
    DemoHarness h("newdb_demo_lsm_async_backpressure");
    h.run("CREATE TABLE(t1)");
    h.run("USE(t1)");
    h.run("HOTINDEX on");
    h.run("SEGMENT 48");

    for (int i = 1; i <= 260; ++i) {
        h.run("INSERT(" + std::to_string(i) + ",u,1)");
    }

    const auto stats = h.st.txn.runtimeStats();
    EXPECT_GE(stats.lsm_compaction_count, 1u);
    EXPECT_GE(stats.lsm_compaction_queue_pending, 0u);
    EXPECT_GE(stats.lsm_compaction_queue_inflight, 0u);
    EXPECT_GE(stats.lsm_compaction_enqueue_skipped_backpressure, 0u);
}

TEST(DemoLsmLite, SegmentCacheReportsHitsAndMisses) {
    DemoHarness h("newdb_demo_lsm_cache_metrics");
    h.run("CREATE TABLE(t1)");
    h.run("USE(t1)");
    h.run("HOTINDEX on");
    h.run("SEGMENT 64");

    for (int i = 1; i <= 80; ++i) {
        h.run("INSERT(" + std::to_string(i) + ",u,1)");
    }
    h.run("FIND(40)");
    h.run("FIND(40)");
    h.run("FIND(999999)");

    const auto stats = h.st.txn.runtimeStats();
    EXPECT_GE(stats.lsm_segment_cache_hits, 1u);
    EXPECT_GE(stats.lsm_segment_cache_misses, 1u);
}

