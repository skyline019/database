// Slim C ABI: links only against newdb_core (no CLI / newdb_shell).
// Session-oriented APIs return errors or stubs; use full newdb_shared or embed newdb_shell for CLI-backed behavior.

#include <newdb/c_api.h>
#include <newdb/c_api_helpers.h>
#include <newdb/engine_session_handle.h>
#include <newdb/schema_io.h>

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <memory>
#include <string>

namespace {

#define NEWDB_STR_IMPL(x) #x
#define NEWDB_STR(x) NEWDB_STR_IMPL(x)

static constexpr const char* kNewdbCApiVersionString =
    "newdb-c-api/"
    NEWDB_STR(NEWDB_C_API_VERSION_MAJOR) "."
    NEWDB_STR(NEWDB_C_API_VERSION_MINOR) "."
    NEWDB_STR(NEWDB_C_API_VERSION_PATCH);

static constexpr const char kSlimExecuteMsg[] =
    "[ERROR] newdb was built with NEWDB_SHARED_SLIM: session execute requires the full shared "
    "library (NEWDB_SHARED_SLIM=OFF) or link newdb_shell / run newdb_demo.\n";

static constexpr const char kSlimRuntimeMsg[] =
    "{\"build\":\"slim\",\"note\":\"full runtime stats require non-slim newdb_shared\"}\n";

static constexpr const char kSlimWherePlanMsg[] = "{\"ok\":0,\"error\":\"slim_build_no_cli_planner\"}\n";

struct CApiSlimSession {
    newdb_engine_session_t* engine{nullptr};
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

void set_last_error(CApiSlimSession* s, int code) {
    if (s != nullptr) {
        s->last_error = code;
    }
}

}  // namespace

extern "C" {

const char* newdb_version_string(void) {
    return kNewdbCApiVersionString;
}

int newdb_api_version_major(void) {
    return NEWDB_C_API_VERSION_MAJOR;
}
int newdb_api_version_minor(void) {
    return NEWDB_C_API_VERSION_MINOR;
}
int newdb_api_version_patch(void) {
    return NEWDB_C_API_VERSION_PATCH;
}
int newdb_abi_version(void) {
    return NEWDB_C_API_ABI_VERSION;
}
int newdb_negotiate_abi(int requested_abi) {
    return requested_abi == NEWDB_C_API_ABI_VERSION ? 1 : 0;
}

const char* newdb_error_code_string(int code) {
    switch (code) {
        case NEWDB_OK:
            return "ok";
        case NEWDB_ERR_INVALID_ARGUMENT:
            return "invalid_argument";
        case NEWDB_ERR_INVALID_HANDLE:
            return "invalid_handle";
        case NEWDB_ERR_EXECUTION_FAILED:
            return "execution_failed";
        case NEWDB_ERR_INTERNAL:
            return "internal";
        case NEWDB_ERR_LOG_IO:
            return "log_io";
        case NEWDB_ERR_SESSION_TERMINATED:
            return "session_terminated";
        case NEWDB_ERR_BACKEND_UNAVAILABLE:
            return "backend_unavailable";
        default:
            return "unknown";
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
    try {
        auto ptr = std::make_unique<CApiSlimSession>();
        ptr->engine = newdb_engine_session_create(data_dir, table_name, log_file_path, 0);
        if (ptr->engine == nullptr) {
            return nullptr;
        }
        ptr->last_error = NEWDB_OK;
        return ptr.release();
    } catch (...) {
        return nullptr;
    }
}

void newdb_session_destroy(newdb_session_handle handle) {
    auto* ptr = static_cast<CApiSlimSession*>(handle);
    if (ptr == nullptr) {
        return;
    }
    newdb_engine_session_destroy(ptr->engine);
    delete ptr;
}

int newdb_session_set_table(newdb_session_handle handle, const char* table_name) {
    auto* ptr = static_cast<CApiSlimSession*>(handle);
    if (ptr == nullptr) {
        return 0;
    }
    if (table_name == nullptr || table_name[0] == '\0') {
        set_last_error(ptr, NEWDB_ERR_INVALID_ARGUMENT);
        return 0;
    }
    set_last_error(ptr, NEWDB_OK);
    return 1;
}

int newdb_session_last_error(newdb_session_handle handle) {
    auto* ptr = static_cast<CApiSlimSession*>(handle);
    if (ptr == nullptr) {
        return NEWDB_ERR_INVALID_HANDLE;
    }
    return ptr->last_error;
}

int newdb_session_execute(newdb_session_handle handle,
                          const char* command_line,
                          char* output_buf,
                          size_t output_buf_size) {
    auto* ptr = static_cast<CApiSlimSession*>(handle);
    if (ptr == nullptr || command_line == nullptr || output_buf == nullptr || output_buf_size == 0) {
        set_last_error(ptr, ptr == nullptr ? NEWDB_ERR_INVALID_HANDLE : NEWDB_ERR_INVALID_ARGUMENT);
        return 0;
    }
    std::string out = kSlimExecuteMsg;
    set_last_error(ptr, NEWDB_ERR_EXECUTION_FAILED);
    newdb::c_api_detail::prepend_capi_error_line(out, ptr->last_error);
    const size_t copy_len = (out.size() < output_buf_size - 1) ? out.size() : (output_buf_size - 1);
    std::memcpy(output_buf, out.data(), copy_len);
    output_buf[copy_len] = '\0';
    return 0;
}

int newdb_session_runtime_stats(newdb_session_handle handle,
                                char* output_buf,
                                size_t output_buf_size) {
    auto* ptr = static_cast<CApiSlimSession*>(handle);
    if (ptr == nullptr || output_buf == nullptr || output_buf_size == 0) {
        set_last_error(ptr, ptr == nullptr ? NEWDB_ERR_INVALID_HANDLE : NEWDB_ERR_INVALID_ARGUMENT);
        return 0;
    }
    const std::string out = kSlimRuntimeMsg;
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
    (void)argv_where_tokens;
    auto* ptr = static_cast<CApiSlimSession*>(handle);
    if (ptr == nullptr || output_buf == nullptr || output_buf_size == 0) {
        set_last_error(ptr, ptr == nullptr ? NEWDB_ERR_INVALID_HANDLE : NEWDB_ERR_INVALID_ARGUMENT);
        return 0;
    }
    if (argc < 3) {
        set_last_error(ptr, NEWDB_ERR_INVALID_ARGUMENT);
        return 0;
    }
    const size_t n = std::strlen(kSlimWherePlanMsg);
    const size_t copy_len = std::min(output_buf_size - 1, n);
    std::memcpy(output_buf, kSlimWherePlanMsg, copy_len);
    output_buf[copy_len] = '\0';
    set_last_error(ptr, NEWDB_ERR_EXECUTION_FAILED);
    return 0;
}

int newdb_session_append_runtime_snapshot(newdb_session_handle handle,
                                          const char* output_jsonl_path,
                                          const char* /*label*/) {
    auto* ptr = static_cast<CApiSlimSession*>(handle);
    if (ptr == nullptr) {
        set_last_error(ptr, NEWDB_ERR_INVALID_HANDLE);
        return 0;
    }
    if (output_jsonl_path == nullptr || output_jsonl_path[0] == '\0') {
        set_last_error(ptr, NEWDB_ERR_INVALID_ARGUMENT);
        return 0;
    }
    std::ofstream out(output_jsonl_path, std::ios::out | std::ios::app);
    if (!out.good()) {
        set_last_error(ptr, NEWDB_ERR_LOG_IO);
        return 0;
    }
    out << kSlimRuntimeMsg;
    if (!out.good()) {
        set_last_error(ptr, NEWDB_ERR_LOG_IO);
        return 0;
    }
    set_last_error(ptr, NEWDB_OK);
    return 1;
}

}  // extern "C"
