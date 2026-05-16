#include "test_artifact_env.hpp"
#include "test_artifact_config.hpp"

#include <chrono>
#include <ctime>
#include <filesystem>
#include <iomanip>
#include <sstream>

namespace structdb::testing {
namespace {

std::filesystem::path g_run_root;

struct RunRootRegistration {
  RunRootRegistration() {
    using clock = std::chrono::system_clock;
    const auto now = clock::now();
    const std::time_t t = clock::to_time_t(now);
    const auto ms = static_cast<int>(
        std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count() % 1000);
    std::tm tm{};
#if defined(_WIN32)
    localtime_s(&tm, &t);
#else
    localtime_r(&t, &tm);
#endif
    std::ostringstream stamp;
    stamp << std::put_time(&tm, "%Y%m%d_%H%M%S") << '_' << std::setfill('0') << std::setw(3) << ms;
    const std::filesystem::path parent(STRUCTDB_TEST_RUNS_PARENT_STR);
    g_run_root = parent / stamp.str();
    std::filesystem::create_directories(g_run_root);
  }
} g_register_run_root;

}  // namespace

const std::filesystem::path& test_artifact_run_root() { return g_run_root; }

}  // namespace structdb::testing
