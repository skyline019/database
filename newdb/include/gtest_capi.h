#pragma once

#if defined(_WIN32)
#  if defined(GTEST_C_API_SHARED_BUILD)
#    define GTEST_C_API __declspec(dllexport)
#  elif defined(GTEST_C_API_SHARED_USE)
#    define GTEST_C_API __declspec(dllimport)
#  else
#    define GTEST_C_API
#  endif
#else
#  define GTEST_C_API
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef void (*gtest_capi_line_callback)(const char* line, void* user_data);

/*
 * gtest_capi_test_callback arguments:
 * - suite_name/test_name: test identity.
 * - should_run: 1 if selected by current filter.
 * - is_disabled: 1 if test is disabled (name starts with DISABLED_).
 * - result_status: 0=notrun, 1=passed, 2=failed, 3=skipped.
 */
typedef void (*gtest_capi_test_callback)(
    const char* suite_name,
    const char* test_name,
    int should_run,
    int is_disabled,
    int result_status,
    void* user_data);

/* Initialize GoogleTest from argv-like arguments. */
GTEST_C_API int gtest_capi_init_from_argv(int argc, const char* const* argv);

/* Run tests and return GoogleTest exit code (0 means success). */
GTEST_C_API int gtest_capi_run_all(void);

/* List tests as "Suite.Test". Returns emitted count, negative on error. */
GTEST_C_API int gtest_capi_list_tests(gtest_capi_line_callback callback, void* user_data);

/* Enumerate detailed test metadata and (if run) result states. */
GTEST_C_API int gtest_capi_enumerate_tests(gtest_capi_test_callback callback, void* user_data);

/* Common options used by cross-language launchers. */
GTEST_C_API int gtest_capi_set_filter(const char* filter);
GTEST_C_API const char* gtest_capi_get_filter(void);
GTEST_C_API int gtest_capi_set_output(const char* output);
GTEST_C_API const char* gtest_capi_get_output(void);
GTEST_C_API int gtest_capi_set_repeat(int repeat);
GTEST_C_API int gtest_capi_get_repeat(void);
GTEST_C_API int gtest_capi_set_shuffle(int enabled);
GTEST_C_API int gtest_capi_get_shuffle(void);
GTEST_C_API int gtest_capi_set_random_seed(int seed);
GTEST_C_API int gtest_capi_get_random_seed(void);
GTEST_C_API int gtest_capi_set_color(const char* color);
GTEST_C_API const char* gtest_capi_get_color(void);
GTEST_C_API int gtest_capi_set_break_on_failure(int enabled);
GTEST_C_API int gtest_capi_get_break_on_failure(void);
GTEST_C_API int gtest_capi_set_throw_on_failure(int enabled);
GTEST_C_API int gtest_capi_get_throw_on_failure(void);
GTEST_C_API int gtest_capi_set_catch_exceptions(int enabled);
GTEST_C_API int gtest_capi_get_catch_exceptions(void);
GTEST_C_API int gtest_capi_set_also_run_disabled_tests(int enabled);
GTEST_C_API int gtest_capi_get_also_run_disabled_tests(void);
GTEST_C_API int gtest_capi_set_brief(int enabled);
GTEST_C_API int gtest_capi_get_brief(void);
GTEST_C_API int gtest_capi_set_print_time(int enabled);
GTEST_C_API int gtest_capi_get_print_time(void);

/*
 * Generic option setter/getter for language bindings.
 * Known keys: filter/output/repeat/shuffle/random_seed/color/break_on_failure/
 * throw_on_failure/catch_exceptions/also_run_disabled_tests/brief/print_time
 */
GTEST_C_API int gtest_capi_set_option(const char* key, const char* value);
GTEST_C_API const char* gtest_capi_get_option(const char* key);

/* Runtime counters from UnitTest singleton. */
GTEST_C_API int gtest_capi_total_test_suite_count(void);
GTEST_C_API int gtest_capi_total_test_count(void);
GTEST_C_API int gtest_capi_test_to_run_count(void);
GTEST_C_API int gtest_capi_successful_test_count(void);
GTEST_C_API int gtest_capi_failed_test_count(void);
GTEST_C_API int gtest_capi_skipped_test_count(void);
GTEST_C_API int gtest_capi_disabled_test_count(void);
GTEST_C_API int gtest_capi_reportable_disabled_test_count(void);
GTEST_C_API long long gtest_capi_elapsed_time_ms(void);

#ifdef __cplusplus
}  // extern "C"
#endif
