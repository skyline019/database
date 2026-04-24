#pragma once

#include "demo_cli.h"

#include <string>

struct ShellState;

// Common workspace init: log path, data_dir, flags, logging_bind_shell, txn root, session separator.
void demo_init_session_logging(ShellState& app,
                               const struct DemoCliWorkspace& ws,
                               const std::string& default_log_name,
                               bool encrypt_log,
                               bool verbose);

// Batch / --exec / --run-mdb / --import-dir (partial). Returns -1 to continue interactive (REPL or GUI).
// Returns 0 or 1 for CLI terminal exit code.
int demo_try_run_terminal_phase(ShellState& app,
                                const DemoCliInvocation& inv,
                                const std::string& data_table,
                                const std::string& data_file_str);

// When --table was given on argv: bind default heap + reload schema.
void demo_preselect_default_table(ShellState& app,
                                  const DemoCliInvocation& inv,
                                  const std::string& data_table,
                                  const std::string& data_file_str);
