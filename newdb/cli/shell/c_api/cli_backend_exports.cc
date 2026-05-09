#include <waterfall/config.h>

#include <newdb/c_api_cli_bridge.h>

#include <memory>
#include <string>

#if defined(_WIN32)
#  if defined(NEWDB_CLI_BACKEND_BUILD)
#    define NEWDB_CLI_BE_API __declspec(dllexport)
#  else
#    define NEWDB_CLI_BE_API
#  endif
#else
#  define NEWDB_CLI_BE_API __attribute__((visibility("default")))
#endif

extern "C" {

NEWDB_CLI_BE_API void* newdb_cli_backend_session_create(const char* data_dir,
                                                        const char* table_name,
                                                        const char* log_file_path) {
    auto* s = new (std::nothrow) NewdbCApiCliSession();
    if (s == nullptr) {
        return nullptr;
    }
    std::string lp;
    if (!s->init(data_dir, table_name, log_file_path, &lp)) {
        delete s;
        return nullptr;
    }
    return s;
}

NEWDB_CLI_BE_API void newdb_cli_backend_session_destroy(void* p) {
    delete static_cast<NewdbCApiCliSession*>(p);
}

NEWDB_CLI_BE_API int newdb_cli_backend_try_fastpath(void* p, const char* line) {
    return static_cast<NewdbCApiCliSession*>(p)->try_engine_execute_fastpath(line) ? 1 : 0;
}

NEWDB_CLI_BE_API int newdb_cli_backend_process_line(void* p, const char* line) {
    return static_cast<NewdbCApiCliSession*>(p)->process_command_line_normalized(line) ? 1 : 0;
}

NEWDB_CLI_BE_API int newdb_cli_backend_apply_table(void* p, const char* table_name) {
    return static_cast<NewdbCApiCliSession*>(p)->apply_table(table_name) ? 1 : 0;
}

NEWDB_CLI_BE_API const char* newdb_cli_backend_log_path(void* p) {
    static thread_local std::string s;
    s = static_cast<NewdbCApiCliSession*>(p)->log_path();
    return s.c_str();
}

NEWDB_CLI_BE_API const char* newdb_cli_backend_current_table(void* p) {
    static thread_local std::string s;
    s = static_cast<NewdbCApiCliSession*>(p)->current_table_name();
    return s.c_str();
}

NEWDB_CLI_BE_API const char* newdb_cli_backend_runtime_stats_json(void* p) {
    static thread_local std::string s;
    s = static_cast<NewdbCApiCliSession*>(p)->runtime_stats_json();
    return s.c_str();
}

NEWDB_CLI_BE_API const char* newdb_cli_backend_runtime_snapshot_line(void* p, const char* label) {
    static thread_local std::string s;
    s = static_cast<NewdbCApiCliSession*>(p)->runtime_snapshot_jsonl_line(label ? std::string(label)
                                                                               : std::string());
    return s.c_str();
}

NEWDB_CLI_BE_API const char* newdb_cli_backend_where_plan_json(void* p,
                                                               int argc,
                                                               const char* const* argv) {
    static thread_local std::string s;
    s.clear();
    if (!static_cast<NewdbCApiCliSession*>(p)->where_plan_json(argc, argv, &s)) {
        return nullptr;
    }
    return s.c_str();
}

}  // extern "C"
