#include <gtest/gtest.h>

#include <cstring>
#include <filesystem>

#include "structdb_capi.h"

#include "test_artifact_env.hpp"

namespace fs = std::filesystem;

TEST(CapiSharedSmoke, VersionAndDllImportMacro) {
  const char* v = structdb_capi_version_string();
  ASSERT_NE(v, nullptr);
  EXPECT_STRNE(v, "");
  EXPECT_EQ(STRUCTDB_CAPI_VERSION_MAJOR, 1);
  EXPECT_EQ(STRUCTDB_CAPI_VERSION_MINOR, 9);
  EXPECT_EQ(STRUCTDB_CAPI_VERSION_PATCH, 0);
  EXPECT_EQ(structdb_capi_version(),
            (static_cast<uint32_t>(STRUCTDB_CAPI_VERSION_MAJOR) << 16) |
                (static_cast<uint32_t>(STRUCTDB_CAPI_VERSION_MINOR) << 8) |
                static_cast<uint32_t>(STRUCTDB_CAPI_VERSION_PATCH));
  EXPECT_STREQ(v, "1.9.0");
}

TEST(CapiSharedSmoke, EngineOpenNullDataDirUsesCwdDefault) {
  const auto root = structdb::testing::test_artifact_run_root() / "structdb_capi_shared_smoke" / "ws";
  fs::remove_all(root);
  fs::create_directories(root);
  const auto prev = fs::current_path();
  fs::current_path(root);
  char err[256] = {};
  structdb_engine* eng = structdb_engine_open(nullptr, err, sizeof(err));
  fs::current_path(prev);
  ASSERT_NE(eng, nullptr) << err;
  structdb_engine_shutdown(eng);
}
