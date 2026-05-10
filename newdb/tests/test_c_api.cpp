#include <newdb/c_api.h>

#include <filesystem>
#include <gtest/gtest.h>
#include <cstring>
#include <string>
#include <thread>
#include <vector>
#include <fstream>
#include <sstream>
#include <string_view>

TEST(CApi, VersionAndAbiAreExposed) {
    const std::string expected =
        std::string("newdb-c-api/") +
        std::to_string(NEWDB_C_API_VERSION_MAJOR) + "." +
        std::to_string(NEWDB_C_API_VERSION_MINOR) + "." +
        std::to_string(NEWDB_C_API_VERSION_PATCH);
    EXPECT_EQ(std::string(newdb_version_string()), expected);
    EXPECT_GE(newdb_api_version_major(), 0);
    EXPECT_GE(newdb_api_version_minor(), 0);
    EXPECT_GE(newdb_api_version_patch(), 0);
    EXPECT_EQ(newdb_abi_version(), NEWDB_C_API_ABI_VERSION);
    EXPECT_EQ(newdb_negotiate_abi(NEWDB_C_API_ABI_VERSION), 1);
    EXPECT_EQ(newdb_negotiate_abi(NEWDB_C_API_ABI_VERSION + 1), 0);
}

TEST(CApi, ErrorCodeStringsAreStable) {
    EXPECT_STREQ(newdb_error_code_string(NEWDB_OK), "ok");
    EXPECT_STREQ(newdb_error_code_string(NEWDB_ERR_INVALID_ARGUMENT), "invalid_argument");
    EXPECT_STREQ(newdb_error_code_string(NEWDB_ERR_INVALID_HANDLE), "invalid_handle");
    EXPECT_STREQ(newdb_error_code_string(NEWDB_ERR_LOG_IO), "log_io");
    EXPECT_STREQ(newdb_error_code_string(NEWDB_ERR_SESSION_TERMINATED), "session_terminated");
    EXPECT_STREQ(newdb_error_code_string(NEWDB_ERR_BACKEND_UNAVAILABLE), "backend_unavailable");
}

TEST(CApi, LastErrorTracksInvalidArguments) {
    char out[64] = {};
    EXPECT_EQ(newdb_session_execute(nullptr, "COUNT", out, sizeof(out)), 0);
    EXPECT_EQ(newdb_session_last_error(nullptr), NEWDB_ERR_INVALID_HANDLE);

    newdb_session_handle h = newdb_session_create("", "users", "");
    ASSERT_NE(h, nullptr);
    EXPECT_EQ(newdb_session_set_table(h, nullptr), 0);
    EXPECT_EQ(newdb_session_last_error(h), NEWDB_ERR_INVALID_ARGUMENT);
    newdb_session_destroy(h);
}

TEST(CApi, SessionExecuteTerminationIsDistinguished) {
    newdb_session_handle h = newdb_session_create("", "users", "");
    ASSERT_NE(h, nullptr);
    char out[128] = {};
    EXPECT_EQ(newdb_session_execute(h, "EXIT", out, sizeof(out)), 0);
    EXPECT_EQ(newdb_session_last_error(h), NEWDB_ERR_SESSION_TERMINATED);
    newdb_session_destroy(h);
}

TEST(CApi, ConcurrentIndependentHandlesSmoke) {
    namespace fs = std::filesystem;
    const fs::path root = fs::temp_directory_path() / "newdb_c_api_concurrency";
    fs::create_directories(root);

    constexpr int kThreads = 4;
    std::vector<int> ok(kThreads, 0);
    std::vector<std::thread> workers;
    workers.reserve(kThreads);

    for (int i = 0; i < kThreads; ++i) {
        workers.emplace_back([&, i]() {
            const fs::path dir = root / ("run_" + std::to_string(i));
            fs::create_directories(dir);
            newdb_session_handle h = newdb_session_create(dir.string().c_str(), "users", "");
            if (h == nullptr) return;
            char out[128] = {};
            if (newdb_session_execute(h, "COUNT", out, sizeof(out)) != 1) {
                newdb_session_destroy(h);
                return;
            }
            if (newdb_session_last_error(h) == NEWDB_OK) {
                ok[i] = 1;
            }
            newdb_session_destroy(h);
        });
    }
    for (auto& t : workers) t.join();
    for (int i = 0; i < kThreads; ++i) {
        EXPECT_EQ(ok[i], 1) << "thread " << i << " did not complete with NEWDB_OK";
    }
    fs::remove_all(root);
}

TEST(CApi, BusinessMismatchReturnsExecutionFailedWithCapiPrefix) {
    namespace fs = std::filesystem;
    const fs::path root = fs::temp_directory_path() / "newdb_c_api_mismatch";
    fs::create_directories(root);

    newdb_session_handle h = newdb_session_create(root.string().c_str(), "users", "");
    ASSERT_NE(h, nullptr);

    char out[512] = {};
    EXPECT_EQ(newdb_session_execute(h, "CREATE TABLE(t1)", out, sizeof(out)), 1);
    EXPECT_EQ(newdb_session_execute(h, "USE(t1)", out, sizeof(out)), 1);
    EXPECT_EQ(newdb_session_execute(h, "DEFATTR(name:string,age:int)", out, sizeof(out)), 1);
    EXPECT_EQ(newdb_session_execute(h, "INSERT(1,Alice,20)", out, sizeof(out)), 1);

    std::memset(out, 0, sizeof(out));
    EXPECT_EQ(newdb_session_execute(h, "UPDATE(1,Alice,a)", out, sizeof(out)), 0);
    EXPECT_EQ(newdb_session_last_error(h), NEWDB_ERR_EXECUTION_FAILED);
    EXPECT_NE(std::string(out).find("[CAPI_ERROR] code=execution_failed"), std::string::npos);
    EXPECT_NE(std::string(out).find("[UPDATE] attribute 'age' expects int, got 'a'"), std::string::npos);

    newdb_session_destroy(h);
    fs::remove_all(root);
}

TEST(CApi, InsertMismatchReturnsExecutionFailedWithCapiPrefix) {
    namespace fs = std::filesystem;
    const fs::path root = fs::temp_directory_path() / "newdb_c_api_insert_mismatch";
    fs::create_directories(root);

    newdb_session_handle h = newdb_session_create(root.string().c_str(), "users", "");
    ASSERT_NE(h, nullptr);

    char out[512] = {};
    EXPECT_EQ(newdb_session_execute(h, "CREATE TABLE(t1)", out, sizeof(out)), 1);
    EXPECT_EQ(newdb_session_execute(h, "USE(t1)", out, sizeof(out)), 1);
    EXPECT_EQ(newdb_session_execute(h, "DEFATTR(name:string,age:int)", out, sizeof(out)), 1);

    std::memset(out, 0, sizeof(out));
    EXPECT_EQ(newdb_session_execute(h, "INSERT(1,Alice,a)", out, sizeof(out)), 0);
    EXPECT_EQ(newdb_session_last_error(h), NEWDB_ERR_EXECUTION_FAILED);
    EXPECT_NE(std::string(out).find("[CAPI_ERROR] code=execution_failed"), std::string::npos);
    EXPECT_NE(std::string(out).find("[INSERT] attribute 'age' expects int, got 'a'"), std::string::npos);

    newdb_session_destroy(h);
    fs::remove_all(root);
}

TEST(CApi, SetAttrMismatchReturnsExecutionFailedWithCapiPrefix) {
    namespace fs = std::filesystem;
    const fs::path root = fs::temp_directory_path() / "newdb_c_api_setattr_mismatch";
    fs::create_directories(root);

    newdb_session_handle h = newdb_session_create(root.string().c_str(), "users", "");
    ASSERT_NE(h, nullptr);

    char out[512] = {};
    EXPECT_EQ(newdb_session_execute(h, "CREATE TABLE(t1)", out, sizeof(out)), 1);
    EXPECT_EQ(newdb_session_execute(h, "USE(t1)", out, sizeof(out)), 1);
    EXPECT_EQ(newdb_session_execute(h, "DEFATTR(name:string,age:int)", out, sizeof(out)), 1);
    EXPECT_EQ(newdb_session_execute(h, "INSERT(1,Alice,20)", out, sizeof(out)), 1);

    std::memset(out, 0, sizeof(out));
    EXPECT_EQ(newdb_session_execute(h, "SETATTR(1,age,a)", out, sizeof(out)), 0);
    EXPECT_EQ(newdb_session_last_error(h), NEWDB_ERR_EXECUTION_FAILED);
    EXPECT_NE(std::string(out).find("[CAPI_ERROR] code=execution_failed"), std::string::npos);
    EXPECT_NE(std::string(out).find("[SETATTR] attribute 'age' expects int, got 'a'"), std::string::npos);

    newdb_session_destroy(h);
    fs::remove_all(root);
}

namespace {
[[nodiscard]] std::string trim_trailing_newlines(std::string s) {
    while (!s.empty() && (s.back() == '\n' || s.back() == '\r')) {
        s.pop_back();
    }
    return s;
}

[[nodiscard]] std::string last_line_starting_with(std::string_view tail, std::string_view prefix) {
    std::string s(tail);
    std::vector<std::string> lines;
    std::istringstream iss(s);
    std::string line;
    while (std::getline(iss, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        lines.push_back(std::move(line));
    }
    for (auto it = lines.rbegin(); it != lines.rend(); ++it) {
        if (it->size() >= prefix.size() &&
            std::string_view(*it).substr(0, prefix.size()) == prefix) {
            return *it;
        }
    }
    return {};
}

[[nodiscard]] std::string last_json_object_line_from_log_tail(std::string_view tail) {
    std::string s(tail);
    std::vector<std::string> lines;
    std::istringstream iss(s);
    std::string line;
    while (std::getline(iss, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        lines.push_back(std::move(line));
    }
    for (auto it = lines.rbegin(); it != lines.rend(); ++it) {
        if (!it->empty() && (*it)[0] == '{') {
            return *it;
        }
    }
    return {};
}
} // namespace

TEST(CApi, RuntimeStatsAndShowTuningJsonAreStructured) {
    namespace fs = std::filesystem;
    const fs::path root = fs::temp_directory_path() / "newdb_c_api_stats";
    fs::create_directories(root);

    newdb_session_handle h1 = newdb_session_create(root.string().c_str(), "users", "");
    newdb_session_handle h2 = newdb_session_create(root.string().c_str(), "users", "");
    ASSERT_NE(h1, nullptr);
    ASSERT_NE(h2, nullptr);

    constexpr std::size_t kLargeOut = 512 * 1024;
    std::vector<char> large_out(kLargeOut);
    char* out = large_out.data();

    std::memset(out, 0, kLargeOut);
    EXPECT_EQ(newdb_session_execute(h1, "CREATE TABLE(t1)", out, kLargeOut), 1);
    EXPECT_EQ(newdb_session_execute(h1, "USE(t1)", out, kLargeOut), 1);
    EXPECT_EQ(newdb_session_execute(h2, "USE(t1)", out, kLargeOut), 1);
    EXPECT_EQ(newdb_session_execute(h2, "TXNISOLATION read_committed", out, kLargeOut), 1);

    EXPECT_EQ(newdb_session_execute(h1, "BEGIN(t1)", out, kLargeOut), 1);
    EXPECT_EQ(newdb_session_execute(h1, "INSERT(1,Alice,10)", out, kLargeOut), 1);

    std::memset(out, 0, kLargeOut);
    EXPECT_EQ(newdb_session_runtime_stats(h2, out, kLargeOut), 1);
    const std::string stats_json(trim_trailing_newlines(std::string(out)));
    EXPECT_NE(stats_json.find("\"txn_isolation\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"vacuum_trigger_count\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"vacuum_execute_count\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"vacuum_cooldown_skip_count\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"vacuum_compact_success_count\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"vacuum_compact_failure_count\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"vacuum_compact_bytes_reclaimed\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"vacuum_compact_last_elapsed_ms\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"vacuum_queue_depth\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"vacuum_queue_depth_peak\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"write_conflicts\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"write_conflict_wait_count\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"write_conflict_wait_timeout_count\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"lock_wait_ms_total\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"lock_wait_max_ms\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"lock_deadlock_detect_count\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"lock_deadlock_victim_count\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"txn_begin_lock_conflicts\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"wal_compact_count\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"wal_recovery_runs\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"wal_recovery_undo_ops\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"wal_recovery_last_elapsed_ms\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"wal_recovery_analyze_ms\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"wal_recovery_undo_ms\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"wal_recovery_finalize_ms\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"wal_recovery_records_scanned\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"wal_recovery_dangling_txns\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"wal_recovery_redo_ms\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"wal_recovery_checkpoint_begin_count\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"wal_recovery_checkpoint_end_count\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"wal_recovery_undo_chain_fallback_txns\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"wal_recovery_policy\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"file_lock_same_process_reuse_count\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"wal_group_commit_count\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"wal_group_commit_batch_commits\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"wal_group_commit_pending_commits\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"txn_commit_count\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"txn_commit_p95_ms\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"wal_bytes_since_start\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"wal_bytes_per_commit_avg\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"lock_wait_p95_ms\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"scheduler_throttle_count\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"hot_index_enabled\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"segment_target_bytes\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"lsm_memtable_flush_count\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"lsm_compaction_count\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"lsm_segment_count\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"lsm_memtable_bytes\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"lsm_read_segments_scanned\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"lsm_read_segments_scanned_p95\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"lsm_compaction_bytes_in\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"lsm_compaction_bytes_out\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"wal_adaptive_enabled\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"group_commit_window_ms\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"group_commit_max_batch_commits\":"), std::string::npos);

    std::memset(out, 0, kLargeOut);
    EXPECT_EQ(newdb_session_execute(h2, "SHOW TUNING JSON", out, kLargeOut), 1);
    const std::string tuning_json(last_json_object_line_from_log_tail(out));
    EXPECT_EQ(tuning_json, stats_json);

    std::memset(out, 0, kLargeOut);
    EXPECT_EQ(newdb_session_execute(h2, "SHOW STATUS JSON", out, kLargeOut), 1);
    EXPECT_EQ(last_json_object_line_from_log_tail(out), stats_json);

    EXPECT_NE(tuning_json.find("\"txn_isolation\":"), std::string::npos);
    EXPECT_NE(tuning_json.find("\"txn_isolation\":\"read_committed\""), std::string::npos);
    EXPECT_NE(tuning_json.find("\"vacuum_trigger_count\":"), std::string::npos);
    EXPECT_NE(tuning_json.find("\"vacuum_execute_count\":"), std::string::npos);
    EXPECT_NE(tuning_json.find("\"vacuum_cooldown_skip_count\":"), std::string::npos);
    EXPECT_NE(tuning_json.find("\"vacuum_compact_success_count\":"), std::string::npos);
    EXPECT_NE(tuning_json.find("\"vacuum_compact_failure_count\":"), std::string::npos);
    EXPECT_NE(tuning_json.find("\"vacuum_compact_bytes_reclaimed\":"), std::string::npos);
    EXPECT_NE(tuning_json.find("\"vacuum_compact_last_elapsed_ms\":"), std::string::npos);
    EXPECT_NE(tuning_json.find("\"vacuum_queue_depth\":"), std::string::npos);
    EXPECT_NE(tuning_json.find("\"vacuum_queue_depth_peak\":"), std::string::npos);
    EXPECT_NE(tuning_json.find("\"write_conflicts\":"), std::string::npos);
    EXPECT_NE(tuning_json.find("\"lock_wait_ms_total\":"), std::string::npos);
    EXPECT_NE(tuning_json.find("\"lock_wait_max_ms\":"), std::string::npos);
    EXPECT_NE(tuning_json.find("\"lock_deadlock_detect_count\":"), std::string::npos);
    EXPECT_NE(tuning_json.find("\"lock_deadlock_victim_count\":"), std::string::npos);
    EXPECT_NE(tuning_json.find("\"txn_begin_lock_conflicts\":"), std::string::npos);
    EXPECT_NE(tuning_json.find("\"wal_compact_count\":"), std::string::npos);
    EXPECT_NE(tuning_json.find("\"wal_recovery_runs\":"), std::string::npos);
    EXPECT_NE(tuning_json.find("\"wal_recovery_undo_ops\":"), std::string::npos);
    EXPECT_NE(tuning_json.find("\"wal_recovery_last_elapsed_ms\":"), std::string::npos);
    EXPECT_NE(tuning_json.find("\"wal_recovery_analyze_ms\":"), std::string::npos);
    EXPECT_NE(tuning_json.find("\"wal_recovery_undo_ms\":"), std::string::npos);
    EXPECT_NE(tuning_json.find("\"wal_recovery_finalize_ms\":"), std::string::npos);
    EXPECT_NE(tuning_json.find("\"wal_recovery_records_scanned\":"), std::string::npos);
    EXPECT_NE(tuning_json.find("\"wal_recovery_undo_chain_fallback_txns\":"), std::string::npos);
    // NOTE: tail fields may be truncated in fixed C API buffers on long JSON payloads.
    // NOTE: SHOW TUNING JSON can become long with extended counters; keep
    // strict assertions on core gate fields above and avoid tail-field
    // truncation sensitivity in fixed-size C API buffers.

    EXPECT_EQ(newdb_session_execute(h1, "ROLLBACK", out, kLargeOut), 1);

    newdb_session_destroy(h1);
    newdb_session_destroy(h2);
    fs::remove_all(root);
}

TEST(CApi, ShowStorageFastpathTailMatchesEmitterContract) {
    namespace fs = std::filesystem;
    const fs::path root = fs::temp_directory_path() / "newdb_c_api_show_storage";
    fs::create_directories(root);

    newdb_session_handle h = newdb_session_create(root.string().c_str(), "users", "");
    ASSERT_NE(h, nullptr);

    constexpr std::size_t kBuf = 32 * 1024;
    std::vector<char> buf(kBuf);
    char* out = buf.data();

    EXPECT_EQ(newdb_session_execute(h, "CREATE TABLE(t1)", out, kBuf), 1);
    EXPECT_EQ(newdb_session_execute(h, "USE(t1)", out, kBuf), 1);

    std::memset(out, 0, kBuf);
    EXPECT_EQ(newdb_session_execute(h, "SHOW STORAGE", out, kBuf), 1);
    const std::string line1 = last_line_starting_with(std::string(out), "[STORAGE]");
    EXPECT_FALSE(line1.empty());
    EXPECT_NE(line1.find("demodb.wal bytes="), std::string::npos);
    EXPECT_NE(line1.find("demodb.wal_lsn="), std::string::npos);
    EXPECT_NE(line1.find("total *.bin files="), std::string::npos);
    EXPECT_NE(line1.find(root.string()), std::string::npos);

    // Second call: workspace summary can change byte totals between invocations; only assert format.
    std::memset(out, 0, kBuf);
    EXPECT_EQ(newdb_session_execute(h, "SHOW STORAGE", out, kBuf), 1);
    const std::string line2 = last_line_starting_with(std::string(out), "[STORAGE]");
    EXPECT_FALSE(line2.empty());
    EXPECT_NE(line2.find("demodb.wal bytes="), std::string::npos);
    EXPECT_EQ(line1.substr(0, line1.rfind(" bytes=")), line2.substr(0, line2.rfind(" bytes=")));

    newdb_session_destroy(h);
    fs::remove_all(root);
}

TEST(CApi, RuntimeSnapshotAppendsJsonl) {
    namespace fs = std::filesystem;
    const fs::path root = fs::temp_directory_path() / "newdb_c_api_snapshot";
    fs::create_directories(root);
    const fs::path jsonl = root / "runtime_stats.jsonl";

    newdb_session_handle h = newdb_session_create(root.string().c_str(), "users", "");
    ASSERT_NE(h, nullptr);
    char out[256] = {};

    EXPECT_EQ(newdb_session_execute(h, "CREATE TABLE(t1)", out, sizeof(out)), 1);
    EXPECT_EQ(newdb_session_execute(h, "USE(t1)", out, sizeof(out)), 1);
    EXPECT_EQ(newdb_session_execute(h, "TXNISOLATION read_committed", out, sizeof(out)), 1);
    EXPECT_EQ(newdb_session_execute(h, "BEGIN(t1)", out, sizeof(out)), 1);
    EXPECT_EQ(newdb_session_execute(h, "INSERT(1,Alice,10)", out, sizeof(out)), 1);

    EXPECT_EQ(newdb_session_append_runtime_snapshot(h, jsonl.string().c_str(), "after_insert"), 1);
    EXPECT_EQ(newdb_session_append_runtime_snapshot(h, jsonl.string().c_str(), "after_insert_2"), 1);

    std::ifstream in(jsonl.string());
    ASSERT_TRUE(in.good());
    std::string l1;
    std::string l2;
    ASSERT_TRUE(static_cast<bool>(std::getline(in, l1)));
    ASSERT_TRUE(static_cast<bool>(std::getline(in, l2)));
    EXPECT_NE(l1.find("\"schema_version\":\"newdb.runtime_stats.v1\""), std::string::npos);
    EXPECT_NE(l1.find("\"label\":\"after_insert\""), std::string::npos);
    EXPECT_NE(l1.find("\"stats\":{"), std::string::npos);
    EXPECT_NE(l1.find("\"txn_isolation\":\"read_committed\""), std::string::npos);
    EXPECT_NE(l1.find("\"vacuum_compact_success_count\":"), std::string::npos);
    EXPECT_NE(l1.find("\"vacuum_compact_failure_count\":"), std::string::npos);
    EXPECT_NE(l1.find("\"vacuum_compact_bytes_reclaimed\":"), std::string::npos);
    EXPECT_NE(l1.find("\"vacuum_compact_last_elapsed_ms\":"), std::string::npos);
    EXPECT_NE(l1.find("\"vacuum_queue_depth\":"), std::string::npos);
    EXPECT_NE(l1.find("\"vacuum_queue_depth_peak\":"), std::string::npos);
    EXPECT_NE(l1.find("\"lock_wait_ms_total\":"), std::string::npos);
    EXPECT_NE(l1.find("\"lock_wait_max_ms\":"), std::string::npos);
    EXPECT_NE(l1.find("\"lock_deadlock_detect_count\":"), std::string::npos);
    EXPECT_NE(l1.find("\"lock_deadlock_victim_count\":"), std::string::npos);
    EXPECT_NE(l1.find("\"txn_begin_lock_conflicts\":"), std::string::npos);
    EXPECT_NE(l1.find("\"wal_compact_count\":"), std::string::npos);
    EXPECT_NE(l1.find("\"wal_recovery_runs\":"), std::string::npos);
    EXPECT_NE(l1.find("\"wal_recovery_undo_ops\":"), std::string::npos);
    EXPECT_NE(l1.find("\"wal_recovery_last_elapsed_ms\":"), std::string::npos);
    EXPECT_NE(l1.find("\"wal_recovery_analyze_ms\":"), std::string::npos);
    EXPECT_NE(l1.find("\"wal_recovery_undo_ms\":"), std::string::npos);
    EXPECT_NE(l1.find("\"wal_recovery_finalize_ms\":"), std::string::npos);
    EXPECT_NE(l1.find("\"wal_recovery_records_scanned\":"), std::string::npos);
    EXPECT_NE(l1.find("\"wal_recovery_dangling_txns\":"), std::string::npos);
    EXPECT_NE(l1.find("\"wal_recovery_undo_chain_fallback_txns\":"), std::string::npos);
    EXPECT_NE(l1.find("\"wal_group_commit_count\":"), std::string::npos);
    EXPECT_NE(l1.find("\"scheduler_throttle_count\":"), std::string::npos);
    EXPECT_NE(l1.find("\"lsm_memtable_flush_count\":"), std::string::npos);
    EXPECT_NE(l1.find("\"lsm_compaction_count\":"), std::string::npos);
    EXPECT_NE(l1.find("\"lsm_segment_count\":"), std::string::npos);
    EXPECT_NE(l1.find("\"lsm_memtable_bytes\":"), std::string::npos);
    EXPECT_NE(l1.find("\"lsm_read_segments_scanned\":"), std::string::npos);
    EXPECT_NE(l1.find("\"lsm_read_segments_scanned_p95\":"), std::string::npos);
    EXPECT_NE(l1.find("\"lsm_compaction_bytes_in\":"), std::string::npos);
    EXPECT_NE(l1.find("\"lsm_compaction_bytes_out\":"), std::string::npos);
    EXPECT_NE(l1.find("\"txn_commit_count\":"), std::string::npos);
    EXPECT_NE(l1.find("\"txn_commit_p95_ms\":"), std::string::npos);
    EXPECT_NE(l1.find("\"wal_bytes_since_start\":"), std::string::npos);
    EXPECT_NE(l1.find("\"heap_decode_slot_calls\":"), std::string::npos);
    EXPECT_NE(l1.find("\"vacuum_priority_score\":"), std::string::npos);
    EXPECT_NE(l1.find("\"vacuum_health_bonus_last\":"), std::string::npos);
    EXPECT_NE(l1.find("\"compact_debt_bytes\":"), std::string::npos);
    EXPECT_NE(l1.find("\"page_cache_hits\":"), std::string::npos);
    EXPECT_NE(l1.find("\"memory_budget_max_bytes\":"), std::string::npos);
    EXPECT_NE(l1.find("\"memory_budget_used_bytes\":"), std::string::npos);
    EXPECT_NE(l1.find("\"memory_budget_reject_count\":"), std::string::npos);
    EXPECT_NE(l1.find("\"memory_budget_bytes_evicted_total\":"), std::string::npos);
    EXPECT_NE(l1.find("\"memory_budget_sidecar_load_skipped_total\":"), std::string::npos);
    EXPECT_NE(l1.find("\"transaction_snapshot_lsn\":"), std::string::npos);
    EXPECT_NE(l1.find("\"statement_snapshot_lsn\":"), std::string::npos);
    EXPECT_NE(l1.find("\"table_storage_health_tier\":"), std::string::npos);
    EXPECT_NE(l1.find("\"txn_snapshot_refresh_count\":"), std::string::npos);
    EXPECT_NE(l1.find("\"last_snapshot_source\":"), std::string::npos);
    EXPECT_NE(l1.find("\"scheduler_throttle_count\":"), std::string::npos);
    EXPECT_NE(l2.find("\"schema_version\":\"newdb.runtime_stats.v1\""), std::string::npos);
    EXPECT_NE(l2.find("\"label\":\"after_insert_2\""), std::string::npos);
    in.close();

    EXPECT_EQ(newdb_session_execute(h, "ROLLBACK", out, sizeof(out)), 1);
    newdb_session_destroy(h);
    fs::remove_all(root);
}

