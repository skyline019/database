#include <newdb/c_api.h>

#include <filesystem>
#include <gtest/gtest.h>
#include <cstring>
#include <string>
#include <thread>
#include <vector>
#include <fstream>

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

TEST(CApi, RuntimeStatsAndShowTuningJsonAreStructured) {
    namespace fs = std::filesystem;
    const fs::path root = fs::temp_directory_path() / "newdb_c_api_stats";
    fs::create_directories(root);

    newdb_session_handle h1 = newdb_session_create(root.string().c_str(), "users", "");
    newdb_session_handle h2 = newdb_session_create(root.string().c_str(), "users", "");
    ASSERT_NE(h1, nullptr);
    ASSERT_NE(h2, nullptr);

    char out[1024] = {};
    EXPECT_EQ(newdb_session_execute(h1, "CREATE TABLE(t1)", out, sizeof(out)), 1);
    EXPECT_EQ(newdb_session_execute(h1, "USE(t1)", out, sizeof(out)), 1);
    EXPECT_EQ(newdb_session_execute(h2, "USE(t1)", out, sizeof(out)), 1);

    EXPECT_EQ(newdb_session_execute(h1, "BEGIN(t1)", out, sizeof(out)), 1);
    EXPECT_EQ(newdb_session_execute(h1, "INSERT(1,Alice,10)", out, sizeof(out)), 1);

    std::memset(out, 0, sizeof(out));
    EXPECT_EQ(newdb_session_runtime_stats(h2, out, sizeof(out)), 1);
    const std::string stats_json(out);
    EXPECT_NE(stats_json.find("\"vacuum_trigger_count\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"vacuum_execute_count\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"vacuum_cooldown_skip_count\":"), std::string::npos);
    EXPECT_NE(stats_json.find("\"write_conflicts\":"), std::string::npos);

    std::memset(out, 0, sizeof(out));
    EXPECT_EQ(newdb_session_execute(h2, "SHOW TUNING JSON", out, sizeof(out)), 1);
    const std::string tuning_json(out);
    EXPECT_NE(tuning_json.find("\"vacuum_trigger_count\":"), std::string::npos);
    EXPECT_NE(tuning_json.find("\"vacuum_execute_count\":"), std::string::npos);
    EXPECT_NE(tuning_json.find("\"vacuum_cooldown_skip_count\":"), std::string::npos);
    EXPECT_NE(tuning_json.find("\"write_conflicts\":"), std::string::npos);

    EXPECT_EQ(newdb_session_execute(h1, "ROLLBACK", out, sizeof(out)), 1);

    newdb_session_destroy(h1);
    newdb_session_destroy(h2);
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
    EXPECT_NE(l2.find("\"schema_version\":\"newdb.runtime_stats.v1\""), std::string::npos);
    EXPECT_NE(l2.find("\"label\":\"after_insert_2\""), std::string::npos);
    in.close();

    EXPECT_EQ(newdb_session_execute(h, "ROLLBACK", out, sizeof(out)), 1);
    newdb_session_destroy(h);
    fs::remove_all(root);
}

