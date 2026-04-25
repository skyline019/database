#include <newdb/c_api.h>
#include <newdb/schema.h>
#include <newdb/schema_io.h>

#include "demo_commands.h"
#include "demo_runner.h"
#include "shell_state.h"

#include <cstring>
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

struct CApiSession {
    ShellState shell;
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

struct TailReadResult {
    std::string data;
    bool ok{true};
};

TailReadResult read_file_tail(const std::string& path, std::uintmax_t start_pos) {
    TailReadResult out{};
    std::error_code ec;
    const auto file_size = std::filesystem::file_size(path, ec);
    if (ec || file_size <= start_pos) {
        out.ok = !ec;
        return out;
    }
    std::ifstream in(path, std::ios::binary);
    if (!in.good()) {
        out.ok = false;
        return out;
    }
    in.seekg(static_cast<std::streamoff>(start_pos), std::ios::beg);
    out.data.assign(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
    return out;
}

bool apply_table(CApiSession& s, const char* table_name) {
    if (table_name == nullptr || table_name[0] == '\0') {
        return false;
    }
    s.table_name = table_name;
    const std::string data_file = s.table_name + ".bin";
    s.shell.session.table_name = s.table_name;
    s.shell.session.data_path = resolve_table_file(s.shell, data_file);
    reload_schema_from_data_path(s.shell, s.shell.session.data_path);
    return true;
}

void set_last_error(CApiSession* s, int code) {
    if (s != nullptr) {
        s->last_error = code;
    }
}

bool output_indicates_business_error(const std::string& out) {
    if (out.find("[ERROR]") != std::string::npos) {
        return true;
    }
    if (out.find("expects ") != std::string::npos && out.find(", got '") != std::string::npos) {
        return true;
    }
    if (out.find(" failed") != std::string::npos || out.find(" invalid") != std::string::npos) {
        return true;
    }
    if (out.find("duplicate ") != std::string::npos || out.find(" missing ") != std::string::npos) {
        return true;
    }
    if (out.find("usage:") != std::string::npos) {
        return true;
    }
    return false;
}

void prepend_capi_error_line(std::string& out, int code) {
    if (code == NEWDB_OK) {
        return;
    }
    const std::string prefix =
        std::string("[CAPI_ERROR] code=") + newdb_error_code_string(code) + " numeric=" + std::to_string(code) + "\n";
    out.insert(0, prefix);
}

std::string build_runtime_stats_json(const CApiSession& s) {
    const auto stats = s.shell.txn.runtimeStats();
    std::ostringstream oss;
    oss << "{"
        << "\"vacuum_trigger_count\":" << stats.vacuum_trigger_count << ","
        << "\"vacuum_execute_count\":" << stats.vacuum_execute_count << ","
        << "\"vacuum_cooldown_skip_count\":" << stats.vacuum_cooldown_skip_count << ","
        << "\"write_conflicts\":" << stats.write_conflict_count << ","
        << "\"vacuum_running\":" << (s.shell.txn.vacuumRunning() ? "true" : "false") << ","
        << "\"vacuum_ops_threshold\":" << s.shell.txn.vacuumOpsThreshold() << ","
        << "\"vacuum_min_interval_sec\":" << s.shell.txn.vacuumMinIntervalSec()
        << "}";
    return oss.str();
}

std::string json_escape_local(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 8);
    for (unsigned char c : s) {
        switch (c) {
            case '\"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\b': out += "\\b"; break;
            case '\f': out += "\\f"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (c < 0x20) {
                    char buf[7];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned int>(c));
                    out += buf;
                } else {
                    out.push_back(static_cast<char>(c));
                }
        }
    }
    return out;
}

std::string build_runtime_snapshot_jsonl_line(const CApiSession& s, const std::string& label) {
    const auto now = std::chrono::system_clock::now();
    const auto ts_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    std::ostringstream oss;
    oss << "{"
        << "\"schema_version\":\"newdb.runtime_stats.v1\","
        << "\"ts_ms\":" << ts_ms << ","
        << "\"label\":\"" << json_escape_local(label) << "\","
        << "\"stats\":" << build_runtime_stats_json(s)
        << "}";
    return oss.str();
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
        DemoCliWorkspace ws{};
        ws.data_dir = (data_dir == nullptr) ? "" : data_dir;
        ws.table_name = (table_name == nullptr || table_name[0] == '\0') ? "users" : table_name;
        ws.log_file = (log_file_path == nullptr) ? "" : log_file_path;
        const std::string default_log_name = demo_default_log_spec(ws);
        demo_init_session_logging(ptr->shell, ws, default_log_name, false, false);
        ptr->log_path = ptr->shell.log_file_path;
        (void)apply_table(*ptr, ws.table_name.c_str());
        ptr->last_error = NEWDB_OK;
        return ptr.release();
    } catch (...) {
        return nullptr;
    }
}

void newdb_session_destroy(newdb_session_handle handle) {
    auto* ptr = static_cast<CApiSession*>(handle);
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
    if (!apply_table(*ptr, table_name)) {
        set_last_error(ptr, NEWDB_ERR_EXECUTION_FAILED);
        return 0;
    }
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
    const auto before = std::filesystem::file_size(ptr->log_path, ec);
    const std::uintmax_t start_pos = ec ? 0 : before;
    std::string out;
    int rc = 1;
    try {
        const bool keep_going = process_command_line(ptr->shell, command_line);
        if (!keep_going) {
            out = "[INFO] session terminated by command\n";
            set_last_error(ptr, NEWDB_ERR_SESSION_TERMINATED);
            rc = 0;
        } else {
            const TailReadResult tail = read_file_tail(ptr->log_path, start_pos);
            if (!tail.ok) {
                out = "[ERROR] command output log read failed\n";
                set_last_error(ptr, NEWDB_ERR_LOG_IO);
                rc = 0;
            } else {
                out = tail.data;
                set_last_error(ptr, NEWDB_OK);
                if (output_indicates_business_error(out)) {
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
    prepend_capi_error_line(out, ptr->last_error);
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
    const std::string out = build_runtime_stats_json(*ptr) + "\n";
    const size_t copy_len = (out.size() < output_buf_size - 1) ? out.size() : (output_buf_size - 1);
    std::memcpy(output_buf, out.data(), copy_len);
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
    const std::string line = build_runtime_snapshot_jsonl_line(*ptr, label_s) + "\n";
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
