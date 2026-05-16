#include <gtest_capi.h>

#include <gtest/gtest.h>

#include <cctype>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>
#include <vector>

namespace {
std::mutex g_mu;
bool g_initialized = false;
std::vector<std::string> g_argv_storage;
std::vector<char*> g_argv_ptrs;
thread_local std::string g_ret_scratch;

int init_locked(int argc, const char* const* argv) {
    if (argc <= 0 || argv == nullptr) return -1;
    g_argv_storage.clear();
    g_argv_ptrs.clear();
    g_argv_storage.reserve(static_cast<std::size_t>(argc));
    g_argv_ptrs.reserve(static_cast<std::size_t>(argc));
    for (int i = 0; i < argc; ++i) g_argv_storage.emplace_back(argv[i] == nullptr ? "" : argv[i]);
    for (std::string& s : g_argv_storage) g_argv_ptrs.push_back(s.data());
    int mutable_argc = static_cast<int>(g_argv_ptrs.size());
    ::testing::InitGoogleTest(&mutable_argc, g_argv_ptrs.data());
    g_initialized = true;
    return 0;
}

int ensure_initialized_locked() {
    if (g_initialized) return 0;
    const char* argv0 = "gtest_capi";
    return init_locked(1, &argv0);
}

int parse_bool_like(const char* value, int* out) {
    if (value == nullptr || out == nullptr) return -1;
    std::string s(value);
    for (char& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    if (s == "1" || s == "true" || s == "on" || s == "yes") { *out = 1; return 0; }
    if (s == "0" || s == "false" || s == "off" || s == "no") { *out = 0; return 0; }
    return -1;
}

int test_result_status(const ::testing::TestInfo* info) {
    if (info == nullptr || info->result() == nullptr) return 0;
    const auto* res = info->result();
    if (!res->Passed()) return 2;
    if (res->Skipped()) return 3;
    return 1;
}
}  // namespace

extern "C" {
int gtest_capi_init_from_argv(int argc, const char* const* argv) {
    std::lock_guard<std::mutex> lk(g_mu);
    return init_locked(argc, argv);
}
int gtest_capi_set_filter(const char* filter) { std::lock_guard<std::mutex> lk(g_mu); if (ensure_initialized_locked() != 0) return -1; ::testing::GTEST_FLAG(filter) = (filter == nullptr) ? "*" : filter; return 0; }
const char* gtest_capi_get_filter(void) { std::lock_guard<std::mutex> lk(g_mu); if (ensure_initialized_locked() != 0) return nullptr; return ::testing::GTEST_FLAG(filter).c_str(); }
int gtest_capi_run_all(void) { std::lock_guard<std::mutex> lk(g_mu); if (ensure_initialized_locked() != 0) return -1; return RUN_ALL_TESTS(); }

int gtest_capi_list_tests(gtest_capi_line_callback callback, void* user_data) {
    if (callback == nullptr) return -1;
    std::lock_guard<std::mutex> lk(g_mu);
    if (ensure_initialized_locked() != 0) return -2;
    const ::testing::UnitTest* unit = ::testing::UnitTest::GetInstance();
    int count = 0;
    for (int i = 0; i < unit->total_test_suite_count(); ++i) {
        const ::testing::TestSuite* suite = unit->GetTestSuite(i);
        if (suite == nullptr) continue;
        for (int j = 0; j < suite->total_test_count(); ++j) {
            const ::testing::TestInfo* info = suite->GetTestInfo(j);
            if (info == nullptr) continue;
            std::string line = std::string(suite->name()) + "." + info->name();
            callback(line.c_str(), user_data);
            ++count;
        }
    }
    return count;
}

int gtest_capi_enumerate_tests(gtest_capi_test_callback callback, void* user_data) {
    if (callback == nullptr) return -1;
    std::lock_guard<std::mutex> lk(g_mu);
    if (ensure_initialized_locked() != 0) return -2;
    const ::testing::UnitTest* unit = ::testing::UnitTest::GetInstance();
    int count = 0;
    for (int i = 0; i < unit->total_test_suite_count(); ++i) {
        const ::testing::TestSuite* suite = unit->GetTestSuite(i);
        if (suite == nullptr) continue;
        for (int j = 0; j < suite->total_test_count(); ++j) {
            const ::testing::TestInfo* info = suite->GetTestInfo(j);
            if (info == nullptr) continue;
            callback(suite->name(), info->name(), info->should_run() ? 1 : 0, info->is_reportable() ? 0 : 1, test_result_status(info), user_data);
            ++count;
        }
    }
    return count;
}

int gtest_capi_set_output(const char* output) { std::lock_guard<std::mutex> lk(g_mu); if (ensure_initialized_locked() != 0) return -1; ::testing::GTEST_FLAG(output) = (output == nullptr) ? "" : output; return 0; }
const char* gtest_capi_get_output(void) { std::lock_guard<std::mutex> lk(g_mu); if (ensure_initialized_locked() != 0) return nullptr; return ::testing::GTEST_FLAG(output).c_str(); }
int gtest_capi_set_repeat(int repeat) { std::lock_guard<std::mutex> lk(g_mu); if (ensure_initialized_locked() != 0) return -1; ::testing::GTEST_FLAG(repeat) = repeat; return 0; }
int gtest_capi_get_repeat(void) { std::lock_guard<std::mutex> lk(g_mu); if (ensure_initialized_locked() != 0) return -1; return ::testing::GTEST_FLAG(repeat); }
int gtest_capi_set_shuffle(int enabled) { std::lock_guard<std::mutex> lk(g_mu); if (ensure_initialized_locked() != 0) return -1; ::testing::GTEST_FLAG(shuffle) = (enabled != 0); return 0; }
int gtest_capi_get_shuffle(void) { std::lock_guard<std::mutex> lk(g_mu); if (ensure_initialized_locked() != 0) return -1; return ::testing::GTEST_FLAG(shuffle) ? 1 : 0; }
int gtest_capi_set_random_seed(int seed) { std::lock_guard<std::mutex> lk(g_mu); if (ensure_initialized_locked() != 0) return -1; ::testing::GTEST_FLAG(random_seed) = seed; return 0; }
int gtest_capi_get_random_seed(void) { std::lock_guard<std::mutex> lk(g_mu); if (ensure_initialized_locked() != 0) return -1; return ::testing::GTEST_FLAG(random_seed); }
int gtest_capi_set_color(const char* color) { std::lock_guard<std::mutex> lk(g_mu); if (ensure_initialized_locked() != 0) return -1; ::testing::GTEST_FLAG(color) = (color == nullptr) ? "auto" : color; return 0; }
const char* gtest_capi_get_color(void) { std::lock_guard<std::mutex> lk(g_mu); if (ensure_initialized_locked() != 0) return nullptr; return ::testing::GTEST_FLAG(color).c_str(); }
int gtest_capi_set_break_on_failure(int enabled) { std::lock_guard<std::mutex> lk(g_mu); if (ensure_initialized_locked() != 0) return -1; ::testing::GTEST_FLAG(break_on_failure) = (enabled != 0); return 0; }
int gtest_capi_get_break_on_failure(void) { std::lock_guard<std::mutex> lk(g_mu); if (ensure_initialized_locked() != 0) return -1; return ::testing::GTEST_FLAG(break_on_failure) ? 1 : 0; }
int gtest_capi_set_throw_on_failure(int enabled) { std::lock_guard<std::mutex> lk(g_mu); if (ensure_initialized_locked() != 0) return -1; ::testing::GTEST_FLAG(throw_on_failure) = (enabled != 0); return 0; }
int gtest_capi_get_throw_on_failure(void) { std::lock_guard<std::mutex> lk(g_mu); if (ensure_initialized_locked() != 0) return -1; return ::testing::GTEST_FLAG(throw_on_failure) ? 1 : 0; }
int gtest_capi_set_catch_exceptions(int enabled) { std::lock_guard<std::mutex> lk(g_mu); if (ensure_initialized_locked() != 0) return -1; ::testing::GTEST_FLAG(catch_exceptions) = (enabled != 0); return 0; }
int gtest_capi_get_catch_exceptions(void) { std::lock_guard<std::mutex> lk(g_mu); if (ensure_initialized_locked() != 0) return -1; return ::testing::GTEST_FLAG(catch_exceptions) ? 1 : 0; }
int gtest_capi_set_also_run_disabled_tests(int enabled) { std::lock_guard<std::mutex> lk(g_mu); if (ensure_initialized_locked() != 0) return -1; ::testing::GTEST_FLAG(also_run_disabled_tests) = (enabled != 0); return 0; }
int gtest_capi_get_also_run_disabled_tests(void) { std::lock_guard<std::mutex> lk(g_mu); if (ensure_initialized_locked() != 0) return -1; return ::testing::GTEST_FLAG(also_run_disabled_tests) ? 1 : 0; }
int gtest_capi_set_brief(int enabled) { std::lock_guard<std::mutex> lk(g_mu); if (ensure_initialized_locked() != 0) return -1; ::testing::GTEST_FLAG(brief) = (enabled != 0); return 0; }
int gtest_capi_get_brief(void) { std::lock_guard<std::mutex> lk(g_mu); if (ensure_initialized_locked() != 0) return -1; return ::testing::GTEST_FLAG(brief) ? 1 : 0; }
int gtest_capi_set_print_time(int enabled) { std::lock_guard<std::mutex> lk(g_mu); if (ensure_initialized_locked() != 0) return -1; ::testing::GTEST_FLAG(print_time) = (enabled != 0); return 0; }
int gtest_capi_get_print_time(void) { std::lock_guard<std::mutex> lk(g_mu); if (ensure_initialized_locked() != 0) return -1; return ::testing::GTEST_FLAG(print_time) ? 1 : 0; }

int gtest_capi_set_option(const char* key, const char* value) {
    if (key == nullptr) return -1;
    if (std::strcmp(key, "filter") == 0) return gtest_capi_set_filter(value);
    if (std::strcmp(key, "output") == 0) return gtest_capi_set_output(value);
    if (std::strcmp(key, "color") == 0) return gtest_capi_set_color(value);
    int b = 0;
    if (std::strcmp(key, "repeat") == 0) return gtest_capi_set_repeat(value ? std::atoi(value) : 1);
    if (std::strcmp(key, "random_seed") == 0) return gtest_capi_set_random_seed(value ? std::atoi(value) : 0);
    if (parse_bool_like(value, &b) != 0) return -2;
    if (std::strcmp(key, "shuffle") == 0) return gtest_capi_set_shuffle(b);
    if (std::strcmp(key, "break_on_failure") == 0) return gtest_capi_set_break_on_failure(b);
    if (std::strcmp(key, "throw_on_failure") == 0) return gtest_capi_set_throw_on_failure(b);
    if (std::strcmp(key, "catch_exceptions") == 0) return gtest_capi_set_catch_exceptions(b);
    if (std::strcmp(key, "also_run_disabled_tests") == 0) return gtest_capi_set_also_run_disabled_tests(b);
    if (std::strcmp(key, "brief") == 0) return gtest_capi_set_brief(b);
    if (std::strcmp(key, "print_time") == 0) return gtest_capi_set_print_time(b);
    return -3;
}

const char* gtest_capi_get_option(const char* key) {
    if (key == nullptr) return nullptr;
    if (std::strcmp(key, "filter") == 0) return gtest_capi_get_filter();
    if (std::strcmp(key, "output") == 0) return gtest_capi_get_output();
    if (std::strcmp(key, "color") == 0) return gtest_capi_get_color();
    if (std::strcmp(key, "repeat") == 0) { g_ret_scratch = std::to_string(gtest_capi_get_repeat()); return g_ret_scratch.c_str(); }
    if (std::strcmp(key, "random_seed") == 0) { g_ret_scratch = std::to_string(gtest_capi_get_random_seed()); return g_ret_scratch.c_str(); }
    if (std::strcmp(key, "shuffle") == 0) { g_ret_scratch = gtest_capi_get_shuffle() ? "1" : "0"; return g_ret_scratch.c_str(); }
    if (std::strcmp(key, "break_on_failure") == 0) { g_ret_scratch = gtest_capi_get_break_on_failure() ? "1" : "0"; return g_ret_scratch.c_str(); }
    if (std::strcmp(key, "throw_on_failure") == 0) { g_ret_scratch = gtest_capi_get_throw_on_failure() ? "1" : "0"; return g_ret_scratch.c_str(); }
    if (std::strcmp(key, "catch_exceptions") == 0) { g_ret_scratch = gtest_capi_get_catch_exceptions() ? "1" : "0"; return g_ret_scratch.c_str(); }
    if (std::strcmp(key, "also_run_disabled_tests") == 0) { g_ret_scratch = gtest_capi_get_also_run_disabled_tests() ? "1" : "0"; return g_ret_scratch.c_str(); }
    if (std::strcmp(key, "brief") == 0) { g_ret_scratch = gtest_capi_get_brief() ? "1" : "0"; return g_ret_scratch.c_str(); }
    if (std::strcmp(key, "print_time") == 0) { g_ret_scratch = gtest_capi_get_print_time() ? "1" : "0"; return g_ret_scratch.c_str(); }
    return nullptr;
}

int gtest_capi_total_test_suite_count(void) { std::lock_guard<std::mutex> lk(g_mu); if (ensure_initialized_locked() != 0) return -1; return ::testing::UnitTest::GetInstance()->total_test_suite_count(); }
int gtest_capi_total_test_count(void) { std::lock_guard<std::mutex> lk(g_mu); if (ensure_initialized_locked() != 0) return -1; return ::testing::UnitTest::GetInstance()->total_test_count(); }
int gtest_capi_test_to_run_count(void) { std::lock_guard<std::mutex> lk(g_mu); if (ensure_initialized_locked() != 0) return -1; return ::testing::UnitTest::GetInstance()->test_to_run_count(); }
int gtest_capi_successful_test_count(void) { std::lock_guard<std::mutex> lk(g_mu); if (ensure_initialized_locked() != 0) return -1; return ::testing::UnitTest::GetInstance()->successful_test_count(); }
int gtest_capi_failed_test_count(void) { std::lock_guard<std::mutex> lk(g_mu); if (ensure_initialized_locked() != 0) return -1; return ::testing::UnitTest::GetInstance()->failed_test_count(); }
int gtest_capi_skipped_test_count(void) { std::lock_guard<std::mutex> lk(g_mu); if (ensure_initialized_locked() != 0) return -1; return ::testing::UnitTest::GetInstance()->skipped_test_count(); }
int gtest_capi_disabled_test_count(void) { std::lock_guard<std::mutex> lk(g_mu); if (ensure_initialized_locked() != 0) return -1; return ::testing::UnitTest::GetInstance()->disabled_test_count(); }
int gtest_capi_reportable_disabled_test_count(void) { std::lock_guard<std::mutex> lk(g_mu); if (ensure_initialized_locked() != 0) return -1; return ::testing::UnitTest::GetInstance()->reportable_disabled_test_count(); }
long long gtest_capi_elapsed_time_ms(void) { std::lock_guard<std::mutex> lk(g_mu); if (ensure_initialized_locked() != 0) return -1; return static_cast<long long>(::testing::UnitTest::GetInstance()->elapsed_time()); }
}  // extern "C"
