#include <newdb/c_api.h>
#include <newdb/engine_session_handle.h>

#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <cstring>
#include <string>

#if defined(NEWDB_C_API_SLIM) && NEWDB_C_API_SLIM

TEST(CApiSlim, VersionMatchesMacroConstants) {
    const std::string expected =
        std::string("newdb-c-api/") +
        std::to_string(NEWDB_C_API_VERSION_MAJOR) + "." +
        std::to_string(NEWDB_C_API_VERSION_MINOR) + "." +
        std::to_string(NEWDB_C_API_VERSION_PATCH);
    EXPECT_EQ(std::string(newdb_version_string()), expected);
}

TEST(CApiSlim, EngineSessionCreateDestroy) {
    namespace fs = std::filesystem;
    const fs::path dir = fs::temp_directory_path() / "newdb_engine_session_capi";
    fs::create_directories(dir);
    newdb_engine_session_t* e =
        newdb_engine_session_create(dir.string().c_str(), "t.bin", "", 0);
    ASSERT_NE(e, nullptr);
    newdb_engine_session_destroy(e);
}

TEST(CApiSlim, SchemaCheckUsesEngineLoader) {
    namespace fs = std::filesystem;
    const fs::path dir = fs::temp_directory_path() / "newdb_capi_slim_schema";
    fs::create_directories(dir);
    const fs::path schema = dir / "attrs.txt";
    {
        std::ofstream out(schema);
        ASSERT_TRUE(out.good());
        out << "id:int\nname:string\n";
    }
    const newdb_schema_check_result r = newdb_check_schema_file(schema.string().c_str());
    EXPECT_EQ(r.ok, 1);
}

TEST(CApiSlim, ExecuteReportsSlimBuild) {
    newdb_session_handle h = newdb_session_create("", "users", "");
    ASSERT_NE(h, nullptr);
    char out[512] = {};
    EXPECT_EQ(newdb_session_execute(h, "COUNT", out, sizeof(out)), 0);
    EXPECT_EQ(newdb_session_last_error(h), NEWDB_ERR_EXECUTION_FAILED);
    const std::string s(out);
    EXPECT_NE(s.find("NEWDB_SHARED_SLIM"), std::string::npos);
    newdb_session_destroy(h);
}

TEST(CApiSlim, RuntimeStatsStubReturnsJson) {
    newdb_session_handle h = newdb_session_create("", "users", "");
    ASSERT_NE(h, nullptr);
    char buf[256] = {};
    ASSERT_EQ(newdb_session_runtime_stats(h, buf, sizeof(buf)), 1);
    EXPECT_NE(std::strstr(buf, "slim"), nullptr);
    newdb_session_destroy(h);
}

#else

TEST(CApiSlim, SkippedUnlessBuiltWithNewdbCApiSlim) {
    GTEST_SKIP() << "link this TU with -DNEWDB_C_API_SLIM=1 (newdb_capi_slim_tests target)";
}

#endif
