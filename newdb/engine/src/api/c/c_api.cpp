#include <newdb/c_api.h>
#include <newdb/c_api_helpers.h>
#include <newdb/read_path_policy.h>
#include <newdb/schema.h>
#include <newdb/schema_io.h>

#if !defined(NEWDB_C_API_PLUGIN_BACKEND)
#include <newdb/c_api_cli_bridge.h>
#else
#include <cstdlib>
#include <mutex>
#if defined(_WIN32)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <dlfcn.h>
#endif
#endif

#include <cstdio>
#include <cstring>
#include <atomic>
#include <filesystem>
#include <fstream>
#include <memory>
#include <exception>
#include <string>
#include <sstream>
#include <chrono>

namespace {

#define NEWDB_STR_IMPL(x) #x
#define NEWDB_STR(x) NEWDB_STR_IMPL(x)

static constexpr const char* kNewdbCApiVersionString =
    "newdb-c-api/"
    NEWDB_STR(NEWDB_C_API_VERSION_MAJOR) "."
    NEWDB_STR(NEWDB_C_API_VERSION_MINOR) "."
    NEWDB_STR(NEWDB_C_API_VERSION_PATCH);

#if defined(NEWDB_C_API_PLUGIN_BACKEND)

struct CliDll {
    void* mod{nullptr};
    void* (*fn_create)(const char*, const char*, const char*){nullptr};
    void (*fn_destroy)(void*){nullptr};
    int (*fn_try_fastpath)(void*, const char*){nullptr};
    int (*fn_process_line)(void*, const char*){nullptr};
    int (*fn_apply_table)(void*, const char*){nullptr};
    const char* (*fn_log_path)(void*){nullptr};
    const char* (*fn_current_table)(void*){nullptr};
    const char* (*fn_runtime_stats)(void*){nullptr};
    const char* (*fn_snapshot_line)(void*, const char*){nullptr};
    const char* (*fn_where_plan)(void*, int, const char* const*){nullptr};
};

static CliDll g_cli_dll;
static std::mutex g_cli_dll_mu;

static void cli_dll_reset_unlocked() {
    if (g_cli_dll.mod != nullptr) {
#if defined(_WIN32)
        FreeLibrary(static_cast<HMODULE>(g_cli_dll.mod));
#else
        dlclose(g_cli_dll.mod);
#endif
        g_cli_dll = CliDll{};
    }
}

static bool cli_dll_load_unlocked() {
    if (g_cli_dll.fn_create != nullptr) {
        return true;
    }
    const char* path = std::getenv("NEWDB_CLI_BACKEND_PATH");
    if (path == nullptr || path[0] == '\0') {
        return false;
    }
#if defined(_WIN32)
    void* mod = LoadLibraryA(path);
#else
    void* mod = dlopen(path, RTLD_NOW);
#endif
    if (mod == nullptr) {
        return false;
    }
    g_cli_dll.mod = mod;
    g_cli_dll.fn_create = reinterpret_cast<decltype(g_cli_dll.fn_create)>(
#if defined(_WIN32)
        reinterpret_cast<void*>(GetProcAddress(static_cast<HMODULE>(mod), "newdb_cli_backend_session_create"))
#else
        dlsym(mod, "newdb_cli_backend_session_create")
#endif
    );
    g_cli_dll.fn_destroy = reinterpret_cast<decltype(g_cli_dll.fn_destroy)>(
#if defined(_WIN32)
        reinterpret_cast<void*>(GetProcAddress(static_cast<HMODULE>(mod), "newdb_cli_backend_session_destroy"))
#else
        dlsym(mod, "newdb_cli_backend_session_destroy")
#endif
    );
    g_cli_dll.fn_try_fastpath = reinterpret_cast<decltype(g_cli_dll.fn_try_fastpath)>(
#if defined(_WIN32)
        reinterpret_cast<void*>(GetProcAddress(static_cast<HMODULE>(mod), "newdb_cli_backend_try_fastpath"))
#else
        dlsym(mod, "newdb_cli_backend_try_fastpath")
#endif
    );
    g_cli_dll.fn_process_line = reinterpret_cast<decltype(g_cli_dll.fn_process_line)>(
#if defined(_WIN32)
        reinterpret_cast<void*>(GetProcAddress(static_cast<HMODULE>(mod), "newdb_cli_backend_process_line"))
#else
        dlsym(mod, "newdb_cli_backend_process_line")
#endif
    );
    g_cli_dll.fn_apply_table = reinterpret_cast<decltype(g_cli_dll.fn_apply_table)>(
#if defined(_WIN32)
        reinterpret_cast<void*>(GetProcAddress(static_cast<HMODULE>(mod), "newdb_cli_backend_apply_table"))
#else
        dlsym(mod, "newdb_cli_backend_apply_table")
#endif
    );
    g_cli_dll.fn_log_path = reinterpret_cast<decltype(g_cli_dll.fn_log_path)>(
#if defined(_WIN32)
        reinterpret_cast<void*>(GetProcAddress(static_cast<HMODULE>(mod), "newdb_cli_backend_log_path"))
#else
        dlsym(mod, "newdb_cli_backend_log_path")
#endif
    );
    g_cli_dll.fn_current_table = reinterpret_cast<decltype(g_cli_dll.fn_current_table)>(
#if defined(_WIN32)
        reinterpret_cast<void*>(GetProcAddress(static_cast<HMODULE>(mod), "newdb_cli_backend_current_table"))
#else
        dlsym(mod, "newdb_cli_backend_current_table")
#endif
    );
    g_cli_dll.fn_runtime_stats = reinterpret_cast<decltype(g_cli_dll.fn_runtime_stats)>(
#if defined(_WIN32)
        reinterpret_cast<void*>(GetProcAddress(static_cast<HMODULE>(mod), "newdb_cli_backend_runtime_stats_json"))
#else
        dlsym(mod, "newdb_cli_backend_runtime_stats_json")
#endif
    );
    g_cli_dll.fn_snapshot_line = reinterpret_cast<decltype(g_cli_dll.fn_snapshot_line)>(
#if defined(_WIN32)
        reinterpret_cast<void*>(GetProcAddress(static_cast<HMODULE>(mod), "newdb_cli_backend_runtime_snapshot_line"))
#else
        dlsym(mod, "newdb_cli_backend_runtime_snapshot_line")
#endif
    );
    g_cli_dll.fn_where_plan = reinterpret_cast<decltype(g_cli_dll.fn_where_plan)>(
#if defined(_WIN32)
        reinterpret_cast<void*>(GetProcAddress(static_cast<HMODULE>(mod), "newdb_cli_backend_where_plan_json"))
#else
        dlsym(mod, "newdb_cli_backend_where_plan_json")
#endif
    );

    const bool ok = g_cli_dll.fn_create != nullptr && g_cli_dll.fn_destroy != nullptr &&
                    g_cli_dll.fn_try_fastpath != nullptr && g_cli_dll.fn_process_line != nullptr &&
                    g_cli_dll.fn_apply_table != nullptr && g_cli_dll.fn_log_path != nullptr &&
                    g_cli_dll.fn_current_table != nullptr && g_cli_dll.fn_runtime_stats != nullptr &&
                    g_cli_dll.fn_snapshot_line != nullptr && g_cli_dll.fn_where_plan != nullptr;
    if (!ok) {
        cli_dll_reset_unlocked();
        return false;
    }
    return true;
}

static bool cli_dll_load() {
    std::lock_guard<std::mutex> lk(g_cli_dll_mu);
    return cli_dll_load_unlocked();
}

static std::string cli_bridge_log_path(void* bridge) {
    const char* s = g_cli_dll.fn_log_path(bridge);
    return s != nullptr ? std::string(s) : std::string();
}

#endif  // NEWDB_C_API_PLUGIN_BACKEND

struct CApiSession {
#if defined(NEWDB_C_API_PLUGIN_BACKEND)
    void* bridge{nullptr};
#else
    std::unique_ptr<NewdbCApiCliSession> cli;
#endif
    std::string table_name;
    std::string log_path;
    int last_error{NEWDB_OK};
};

newdb_schema_check_result make_result(int ok, const std::string& msg) {
    newdb_schema_check_result out{};
    out.ok = ok;
    if (msg.empty()) {
        out.message[0] = '\0';
        return out;
    }

    const size_t cap = sizeof(out.message);
    std::strncpy(out.message, msg.c_str(), cap - 1);
    out.message[cap - 1] = '\0';
    return out;
}

void set_last_error(CApiSession* s, int code) {
    if (s != nullptr) {
        s->last_error = code;
    }
}

}  // namespace

extern "C" {

const char* newdb_version_string(void) {
    return kNewdbCApiVersionString;
}

int newdb_api_version_major(void) { return NEWDB_C_API_VERSION_MAJOR; }
int newdb_api_version_minor(void) { return NEWDB_C_API_VERSION_MINOR; }
int newdb_api_version_patch(void) { return NEWDB_C_API_VERSION_PATCH; }
int newdb_abi_version(void) { return NEWDB_C_API_ABI_VERSION; }
int newdb_negotiate_abi(int requested_abi) { return requested_abi == NEWDB_C_API_ABI_VERSION ? 1 : 0; }

const char* newdb_error_code_string(int code) {
    switch (code) {
        case NEWDB_OK: return "ok";
        case NEWDB_ERR_INVALID_ARGUMENT: return "invalid_argument";
        case NEWDB_ERR_INVALID_HANDLE: return "invalid_handle";
        case NEWDB_ERR_EXECUTION_FAILED: return "execution_failed";
        case NEWDB_ERR_INTERNAL: return "internal";
        case NEWDB_ERR_LOG_IO: return "log_io";
        case NEWDB_ERR_SESSION_TERMINATED: return "session_terminated";
        case NEWDB_ERR_BACKEND_UNAVAILABLE: return "backend_unavailable";
        default: return "unknown";
    }
}

int newdb_sum(int lhs, int rhs) {
    return lhs + rhs;
}

newdb_schema_check_result newdb_check_schema_file(const char* attr_file_path) {
    if (attr_file_path == nullptr || attr_file_path[0] == '\0') {
        return make_result(0, "attr file path is empty");
    }

    newdb::TableSchema schema{};
    const newdb::Status st = newdb::load_schema_text(attr_file_path, schema);
    if (!st.ok) {
        return make_result(0, st.message);
    }
    return make_result(1, "");
}

newdb_session_handle newdb_session_create(const char* data_dir,
                                          const char* table_name,
                                          const char* log_file_path) {
    auto ptr = std::make_unique<CApiSession>();

    try {
#if defined(NEWDB_C_API_PLUGIN_BACKEND)
        if (!cli_dll_load()) {
            return nullptr;
        }
        void* br = g_cli_dll.fn_create(data_dir, table_name, log_file_path);
        if (br == nullptr) {
            return nullptr;
        }
        ptr->bridge = br;
        ptr->log_path = cli_bridge_log_path(br);
        const char* tn = g_cli_dll.fn_current_table(br);
        ptr->table_name = tn != nullptr ? std::string(tn) : std::string();
#else
        ptr->cli = std::make_unique<NewdbCApiCliSession>();
        if (!ptr->cli->init(data_dir, table_name, log_file_path, &ptr->log_path)) {
            return nullptr;
        }
        ptr->table_name = ptr->cli->current_table_name();
#endif
        ptr->last_error = NEWDB_OK;
        return ptr.release();
    } catch (...) {
        return nullptr;
    }
}

void newdb_session_destroy(newdb_session_handle handle) {
    auto* ptr = static_cast<CApiSession*>(handle);
    if (ptr == nullptr) {
        return;
    }
#if defined(NEWDB_C_API_PLUGIN_BACKEND)
    if (ptr->bridge != nullptr && g_cli_dll.fn_destroy != nullptr) {
        g_cli_dll.fn_destroy(ptr->bridge);
    }
#endif
    delete ptr;
}

int newdb_session_set_table(newdb_session_handle handle, const char* table_name) {
    auto* ptr = static_cast<CApiSession*>(handle);
    if (ptr == nullptr) {
        return 0;
    }
    if (table_name == nullptr || table_name[0] == '\0') {
        set_last_error(ptr, NEWDB_ERR_INVALID_ARGUMENT);
        return 0;
    }
#if defined(NEWDB_C_API_PLUGIN_BACKEND)
    if (g_cli_dll.fn_apply_table(ptr->bridge, table_name) == 0) {
        set_last_error(ptr, NEWDB_ERR_EXECUTION_FAILED);
        return 0;
    }
    ptr->log_path = cli_bridge_log_path(ptr->bridge);
    const char* tn = g_cli_dll.fn_current_table(ptr->bridge);
    ptr->table_name = tn != nullptr ? std::string(tn) : std::string();
#else
    if (!ptr->cli->apply_table(table_name)) {
        set_last_error(ptr, NEWDB_ERR_EXECUTION_FAILED);
        return 0;
    }
    ptr->table_name = ptr->cli->current_table_name();
    ptr->log_path = ptr->cli->log_path();
#endif
    set_last_error(ptr, NEWDB_OK);
    return 1;
}

int newdb_session_last_error(newdb_session_handle handle) {
    auto* ptr = static_cast<CApiSession*>(handle);
    if (ptr == nullptr) {
        return NEWDB_ERR_INVALID_HANDLE;
    }
    return ptr->last_error;
}

int newdb_session_execute(newdb_session_handle handle,
                          const char* command_line,
                          char* output_buf,
                          size_t output_buf_size) {
    auto* ptr = static_cast<CApiSession*>(handle);
    if (ptr == nullptr || command_line == nullptr || output_buf == nullptr || output_buf_size == 0) {
        set_last_error(ptr, ptr == nullptr ? NEWDB_ERR_INVALID_HANDLE : NEWDB_ERR_INVALID_ARGUMENT);
        return 0;
    }
    std::error_code ec;
#if defined(NEWDB_C_API_PLUGIN_BACKEND)
    ptr->log_path = cli_bridge_log_path(ptr->bridge);
#else
    ptr->log_path = ptr->cli->log_path();
#endif
    const auto before = std::filesystem::file_size(ptr->log_path, ec);
    const std::uintmax_t start_pos = ec ? 0 : before;
    std::string out;
    int rc = 1;
    try {
        const std::string normalized = newdb::c_api_detail::normalize_paren_txn_command(command_line);
        bool keep_going = true;
#if defined(NEWDB_C_API_PLUGIN_BACKEND)
        if (g_cli_dll.fn_try_fastpath(ptr->bridge, normalized.c_str()) == 0) {
            keep_going = g_cli_dll.fn_process_line(ptr->bridge, normalized.c_str()) != 0;
        }
#else
        if (!ptr->cli->try_engine_execute_fastpath(normalized.c_str())) {
            keep_going = ptr->cli->process_command_line_normalized(normalized.c_str());
        }
#endif
        if (!keep_going) {
            out = "[INFO] session terminated by command\n";
            set_last_error(ptr, NEWDB_ERR_SESSION_TERMINATED);
            rc = 0;
        } else {
            const newdb::c_api_detail::TailReadResult tail =
                newdb::c_api_detail::read_file_tail(ptr->log_path, start_pos);
            if (!tail.ok) {
                out = "[ERROR] command output log read failed\n";
                set_last_error(ptr, NEWDB_ERR_LOG_IO);
                rc = 0;
            } else {
                out = tail.data;
                set_last_error(ptr, NEWDB_OK);
                if (newdb::c_api_detail::output_indicates_business_error(out)) {
                    set_last_error(ptr, NEWDB_ERR_EXECUTION_FAILED);
                    rc = 0;
                }
            }
        }
    } catch (const std::exception& e) {
        out = std::string("[ERROR] command failed: ") + e.what() + "\n";
        set_last_error(ptr, NEWDB_ERR_EXECUTION_FAILED);
        rc = 0;
    } catch (...) {
        out = "[ERROR] command failed: unknown exception\n";
        set_last_error(ptr, NEWDB_ERR_INTERNAL);
        rc = 0;
    }
    if (out.empty()) {
        out = "[INFO] command executed with no log output\n";
    }
    newdb::c_api_detail::prepend_capi_error_line(out, ptr->last_error);
    const size_t copy_len = (out.size() < output_buf_size - 1) ? out.size() : (output_buf_size - 1);
    std::memcpy(output_buf, out.data(), copy_len);
    output_buf[copy_len] = '\0';
    return rc;
}

int newdb_session_runtime_stats(newdb_session_handle handle,
                                char* output_buf,
                                size_t output_buf_size) {
    auto* ptr = static_cast<CApiSession*>(handle);
    if (ptr == nullptr || output_buf == nullptr || output_buf_size == 0) {
        set_last_error(ptr, ptr == nullptr ? NEWDB_ERR_INVALID_HANDLE : NEWDB_ERR_INVALID_ARGUMENT);
        return 0;
    }
#if defined(NEWDB_C_API_PLUGIN_BACKEND)
    const char* js = g_cli_dll.fn_runtime_stats(ptr->bridge);
    const std::string out = std::string(js != nullptr ? js : "") + "\n";
#else
    const std::string out = ptr->cli->runtime_stats_json() + "\n";
#endif
    const size_t copy_len = (out.size() < output_buf_size - 1) ? out.size() : (output_buf_size - 1);
    std::memcpy(output_buf, out.data(), copy_len);
    output_buf[copy_len] = '\0';
    set_last_error(ptr, NEWDB_OK);
    return 1;
}

int newdb_session_where_plan_json(newdb_session_handle handle,
                                  int argc,
                                  const char* const* argv_where_tokens,
                                  char* output_buf,
                                  size_t output_buf_size) {
    auto* ptr = static_cast<CApiSession*>(handle);
    if (ptr == nullptr || output_buf == nullptr || output_buf_size == 0) {
        set_last_error(ptr, ptr == nullptr ? NEWDB_ERR_INVALID_HANDLE : NEWDB_ERR_INVALID_ARGUMENT);
        return 0;
    }
#if defined(NEWDB_C_API_PLUGIN_BACKEND)
    ptr->log_path = cli_bridge_log_path(ptr->bridge);
    const char* w = g_cli_dll.fn_where_plan(ptr->bridge, argc, argv_where_tokens);
    if (w == nullptr) {
        set_last_error(ptr, NEWDB_ERR_EXECUTION_FAILED);
        return 0;
    }
    std::string o = w;
#else
    ptr->log_path = ptr->cli->log_path();
    std::string o;
    if (!ptr->cli->where_plan_json(argc, argv_where_tokens, &o)) {
        set_last_error(ptr, NEWDB_ERR_EXECUTION_FAILED);
        return 0;
    }
#endif
    const size_t copy_len = (o.size() < output_buf_size - 1) ? o.size() : (output_buf_size - 1);
    std::memcpy(output_buf, o.data(), copy_len);
    output_buf[copy_len] = '\0';
    set_last_error(ptr, NEWDB_OK);
    return 1;
}

int newdb_session_append_runtime_snapshot(newdb_session_handle handle,
                                          const char* output_jsonl_path,
                                          const char* label) {
    auto* ptr = static_cast<CApiSession*>(handle);
    if (ptr == nullptr) {
        set_last_error(ptr, NEWDB_ERR_INVALID_HANDLE);
        return 0;
    }
    if (output_jsonl_path == nullptr || output_jsonl_path[0] == '\0') {
        set_last_error(ptr, NEWDB_ERR_INVALID_ARGUMENT);
        return 0;
    }
    const std::string label_s = (label == nullptr) ? std::string() : std::string(label);
#if defined(NEWDB_C_API_PLUGIN_BACKEND)
    const char* sl = g_cli_dll.fn_snapshot_line(ptr->bridge, label_s.c_str());
    const std::string line = std::string(sl != nullptr ? sl : "") + "\n";
#else
    const std::string line = ptr->cli->runtime_snapshot_jsonl_line(label_s) + "\n";
#endif
    std::ofstream out(output_jsonl_path, std::ios::out | std::ios::app);
    if (!out.good()) {
        set_last_error(ptr, NEWDB_ERR_LOG_IO);
        return 0;
    }
    out << line;
    if (!out.good()) {
        set_last_error(ptr, NEWDB_ERR_LOG_IO);
        return 0;
    }
    set_last_error(ptr, NEWDB_OK);
    return 1;
}

}  // extern "C"
