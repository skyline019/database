#pragma once

#include <stddef.h>

#if defined(_WIN32)
#  if defined(NEWDB_SHARED_BUILD)
#    define NEWDB_API __declspec(dllexport)
#  elif defined(NEWDB_SHARED_USE)
#    define NEWDB_API __declspec(dllimport)
#  else
#    define NEWDB_API
#  endif
#else
#  define NEWDB_API
#endif

#ifdef __cplusplus
extern "C" {
#endif

#define NEWDB_C_API_VERSION_MAJOR 0
#define NEWDB_C_API_VERSION_MINOR 5
#define NEWDB_C_API_VERSION_PATCH 1
#define NEWDB_C_API_ABI_VERSION 1

typedef enum newdb_error_code {
    NEWDB_OK = 0,
    NEWDB_ERR_INVALID_ARGUMENT = 1,
    NEWDB_ERR_INVALID_HANDLE = 2,
    NEWDB_ERR_EXECUTION_FAILED = 3,
    NEWDB_ERR_INTERNAL = 4,
    NEWDB_ERR_LOG_IO = 5,
    NEWDB_ERR_SESSION_TERMINATED = 6,
    /** CLI backend library missing or failed to load (`NEWDB_C_API_PLUGIN_BACKEND` builds). */
    NEWDB_ERR_BACKEND_UNAVAILABLE = 7
} newdb_error_code;

/*
 * Thread-safety contract (v0.2):
 * - Global version / ABI / error-string APIs are thread-safe.
 * - Per-session APIs are safe when each thread uses its own session handle.
 * - Sharing one session handle across concurrent threads is not guaranteed safe.
 */
typedef struct newdb_schema_check_result {
    int ok;
    char message[512];
} newdb_schema_check_result;

typedef void* newdb_session_handle;

NEWDB_API const char* newdb_version_string(void);
NEWDB_API int newdb_api_version_major(void);
NEWDB_API int newdb_api_version_minor(void);
NEWDB_API int newdb_api_version_patch(void);
NEWDB_API int newdb_abi_version(void);
NEWDB_API int newdb_negotiate_abi(int requested_abi);
NEWDB_API const char* newdb_error_code_string(int code);
NEWDB_API int newdb_sum(int lhs, int rhs);
NEWDB_API newdb_schema_check_result newdb_check_schema_file(const char* attr_file_path);
/**
 * Creates a session, or returns NULL on invalid arguments or initialization failure.
 * In `NEWDB_C_API_PLUGIN_BACKEND` builds, NULL also means the CLI backend module
 * (`NEWDB_CLI_BACKEND_PATH`) failed to load; there is no handle, so
 * `newdb_session_last_error` does not apply. `NEWDB_ERR_BACKEND_UNAVAILABLE` names
 * that failure class for documentation and for APIs that may surface it later.
 */
NEWDB_API newdb_session_handle newdb_session_create(const char* data_dir,
                                                    const char* table_name,
                                                    const char* log_file_path);
NEWDB_API void newdb_session_destroy(newdb_session_handle handle);
NEWDB_API int newdb_session_set_table(newdb_session_handle handle, const char* table_name);
NEWDB_API int newdb_session_last_error(newdb_session_handle handle);
NEWDB_API int newdb_session_execute(newdb_session_handle handle,
                                    const char* command_line,
                                    char* output_buf,
                                    size_t output_buf_size);
/**
 * CliEmbedTier combined JSON: engine POD slice + CLI embed extensions (same object as `SHOW TUNING JSON`).
 * See RUNTIME_STATS_JSON_LAYERING.md (`newdb_engine_runtime_stats_json` vs `newdb_cli_runtime_stats_json`).
 */
NEWDB_API int newdb_session_runtime_stats(newdb_session_handle handle,
                                          char* output_buf,
                                          size_t output_buf_size);
NEWDB_API int newdb_session_append_runtime_snapshot(newdb_session_handle handle,
                                                    const char* output_jsonl_path,
                                                    const char* label);
/**
 * CliEmbedTier: planner/parser/WHERE stack lives under `cli/modules/where` (not engine-only).
 * `NEWDB_SHARED_SLIM` / missing CLI backend return a documented stub / error JSON (see C_API_CAPABILITY_TIERS.md).
 * WHERE token sequence (same as CLI WHERE args): `attr op value [AND|OR attr op value ...]`.
 */
NEWDB_API int newdb_session_where_plan_json(newdb_session_handle handle,
                                            int argc,
                                            const char* const* argv_where_tokens,
                                            char* output_buf,
                                            size_t output_buf_size);

#ifdef __cplusplus
}  // extern "C"
#endif
