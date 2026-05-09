#include <waterfall/config.h>

#include <newdb/c_api_cli_bridge.h>
#include <newdb/engine_session_handle.h>
#include <newdb/json_escape.h>
#include <newdb/session_apply_table.h>

#include "cli/shell/bootstrap/demo_cli.h"
#include "cli/shell/bootstrap/demo_runner.h"
#include "cli/shell/c_api/cli_dispatch_command_line.h"
#include "cli/modules/common/logging/logging.h"
#include "cli/modules/common/util/utils.h"
#include "cli/shell/state/shell_state.h"
#include "cli/shell/state/shell_state_facade.h"
#include "cli/shell/state/shell_state_heap_read_guard.h"
#include "cli/shell/state/shell_state_ops.h"
#include "cli/modules/txn/coordinator/txn_manager.h"
#include "cli/modules/where/executor/where.h"
#include "cli/shell/c_api/runtime_stats_json_builder.h"
#include "cli/shell/c_api/show_storage_log.h"

#include <cstdio>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

struct NewdbCApiCliSession::Impl {
    newdb_engine_session_t* engine_{nullptr};
    ShellState shell;
    std::string table_name;

    explicit Impl(newdb_engine_session_t* e) : engine_(e), shell(ShellStateEngineBorrowedTag{}, e) {}
};

NewdbCApiCliSession::NewdbCApiCliSession() = default;

NewdbCApiCliSession::~NewdbCApiCliSession() {
    if (!impl_) {
        return;
    }
    newdb_engine_session_t* e = impl_->engine_;
    impl_->engine_ = nullptr;
    impl_.reset();
    newdb_engine_session_destroy(e);
}

bool NewdbCApiCliSession::init(const char* data_dir,
                               const char* table_name,
                               const char* log_file_path,
                               std::string* out_log_path) {
    auto* engine = newdb_engine_session_create(data_dir, nullptr, log_file_path, 0);
    if (engine == nullptr) {
        return false;
    }
    try {
        impl_ = std::make_unique<Impl>(engine);
    } catch (...) {
        newdb_engine_session_destroy(engine);
        return false;
    }

    DemoCliWorkspace ws{};
    ws.data_dir = (data_dir == nullptr) ? "" : std::string(data_dir);
    ws.table_name = (table_name == nullptr || table_name[0] == '\0') ? "users" : std::string(table_name);
    ws.log_file = (log_file_path == nullptr) ? "" : std::string(log_file_path);
    const std::string default_log_name = demo_default_log_spec(ws);
    demo_init_session_logging(impl_->shell, ws, default_log_name, false, false);
    log_path_ = impl_->shell.log_file_path();
    if (out_log_path != nullptr) {
        *out_log_path = log_path_;
    }
    if (!apply_table(ws.table_name.c_str())) {
        newdb_engine_session_t* e = impl_->engine_;
        impl_->engine_ = nullptr;
        impl_.reset();
        newdb_engine_session_destroy(e);
        return false;
    }
    return true;
}

bool NewdbCApiCliSession::apply_table(const char* table_name) {
    if (table_name == nullptr || table_name[0] == '\0') {
        return false;
    }
    impl_->table_name = table_name;
    impl_->shell.reset_session_heap_guard();
    const newdb::Status st = newdb::session_apply_table_stem_and_reload_schema(
        impl_->shell.session(), impl_->shell.data_dir(), impl_->table_name);
    return st.ok;
}

bool NewdbCApiCliSession::process_command_line_normalized(const char* normalized_line) {
    return cli_dispatch_execute_normalized_line(impl_->shell, normalized_line);
}

bool NewdbCApiCliSession::try_engine_execute_fastpath(const char* normalized_line) {
    if (impl_ == nullptr || normalized_line == nullptr) {
        return false;
    }
    std::string line(normalized_line);
    while (!line.empty() && (line.back() == '\n' || line.back() == '\r')) {
        line.pop_back();
    }
    const char* p = line.c_str();
    while (*p == ' ' || *p == '\t') {
        ++p;
    }
    if (strcasecmp_ascii(p, "COUNT") == 0 && p[5] == '\0') {
        ShellState& st = impl_->shell;
        ShellStateFacade f(st);
        const char* log_file = f.log_file_path().c_str();
        f.bind_logging();
        append_session_log_line(log_file, line.c_str(), f.encrypt_log());

        struct ShellHeapGuardClear {
            ShellStateFacade* fp{nullptr};
            ~ShellHeapGuardClear() {
                if (fp != nullptr) {
                    fp->reset_session_heap_guard();
                }
            }
        } shell_heap_clear{&f};

        newdb::HeapTable* tbl_ptr = get_cached_table(st);
        if (tbl_ptr == nullptr) {
            return true;
        }
        newdb::HeapTable& tbl = *tbl_ptr;
        const HeapReadViewGuard heap_read_view(st, tbl);
        const std::string& current_table = f.table_name();
        const std::size_t visible = tbl.logical_row_count();
        log_and_print(log_file,
                      "[COUNT] table='%s' rows=%zu decode_calls=%llu decode_hits=%llu decode_misses=%llu\n",
                      current_table.c_str(),
                      visible,
                      static_cast<unsigned long long>(tbl.decode_heap_slot_calls),
                      static_cast<unsigned long long>(tbl.decode_heap_slot_hits),
                      static_cast<unsigned long long>(tbl.decode_heap_slot_misses));
        return true;
    }

    const bool tuning_json =
        strcasecmp_ascii(p, "SHOW TUNING JSON") == 0 && p[sizeof("SHOW TUNING JSON") - 1] == '\0';
    const bool status_json =
        strcasecmp_ascii(p, "SHOW STATUS JSON") == 0 && p[sizeof("SHOW STATUS JSON") - 1] == '\0';
    if (tuning_json || status_json) {
        ShellState& st = impl_->shell;
        ShellStateFacade f(st);
        const char* log_file = f.log_file_path().c_str();
        f.bind_logging();
        append_session_log_line(log_file, line.c_str(), f.encrypt_log());
        ShellStateFacade stats_f(st);
        const std::string json = newdb::capi_cli::format_runtime_stats_json(stats_f);
        log_and_print(log_file, "%s\n", json.c_str());
        return true;
    }

    if (strcasecmp_ascii(p, "SHOW STORAGE") == 0 &&
        p[sizeof("SHOW STORAGE") - 1] == '\0') {
        ShellState& st = impl_->shell;
        ShellStateFacade f(st);
        const char* log_file = f.log_file_path().c_str();
        f.bind_logging();
        append_session_log_line(log_file, line.c_str(), f.encrypt_log());
        newdb::capi_cli::emit_show_storage_log_lines(f, log_file);
        return true;
    }

    return false;
}

std::string NewdbCApiCliSession::log_path() const {
    return log_path_;
}

std::string NewdbCApiCliSession::current_table_name() const {
    return impl_ ? impl_->table_name : std::string{};
}

std::string NewdbCApiCliSession::runtime_stats_json() {
    ShellStateFacade f(impl_->shell);
    return newdb::capi_cli::format_runtime_stats_json(f);
}

std::string NewdbCApiCliSession::runtime_snapshot_jsonl_line(const std::string& label) {
    ShellStateFacade f(impl_->shell);
    return newdb::capi_cli::format_runtime_snapshot_jsonl_line(f, label);
}

bool NewdbCApiCliSession::where_plan_json(int argc,
                                          const char* const* argv_where_tokens,
                                          std::string* out_json) {
    if (argc < 3 || argv_where_tokens == nullptr || out_json == nullptr) {
        return false;
    }
    std::vector<std::string> args;
    args.reserve(static_cast<std::size_t>(argc));
    for (int i = 0; i < argc; ++i) {
        args.emplace_back(argv_where_tokens[i] ? argv_where_tokens[i] : "");
    }
    std::vector<WhereCond> conds;
    std::string err;
    if (!parse_where_args_to_conds(args, conds, err)) {
        std::ostringstream oss;
        oss << "{\"ok\":0,\"error\":\"" << newdb::json_escape(err) << "\"}\n";
        *out_json = oss.str();
        return true;
    }
    ShellStateFacade capi_f(impl_->shell);
    return capi_f.emit_where_plan_json(log_path_.c_str(), conds.data(), conds.size(), out_json);
}
