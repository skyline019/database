#pragma once

#include <memory>
#include <string>

/// Owns `ShellState` and CLI dispatch for the full C API implementation.
/// Defined in `cli/shell/c_api/c_api_cli_bridge.cc` so `c_api.cpp` stays free of `cli/shell` headers.
class NewdbCApiCliSession {
public:
    NewdbCApiCliSession();
    ~NewdbCApiCliSession();

    NewdbCApiCliSession(const NewdbCApiCliSession&) = delete;
    NewdbCApiCliSession& operator=(const NewdbCApiCliSession&) = delete;

    bool init(const char* data_dir,
              const char* table_name,
              const char* log_file_path,
              std::string* out_log_path = nullptr);
    bool apply_table(const char* table_name);
    bool process_command_line_normalized(const char* normalized_line);

    /// Bare `COUNT` (after txn normalization): session log + heap snapshot without `process_command_line`.
    /// Returns true if the line was handled here (including no-table short-circuit); false to use dispatch.
    bool try_engine_execute_fastpath(const char* normalized_line);

    [[nodiscard]] std::string log_path() const;
    [[nodiscard]] std::string current_table_name() const;
    [[nodiscard]] std::string runtime_stats_json();
    [[nodiscard]] std::string runtime_snapshot_jsonl_line(const std::string& label);
    bool where_plan_json(int argc, const char* const* argv_where_tokens, std::string* out_json);

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
    std::string log_path_;
};
