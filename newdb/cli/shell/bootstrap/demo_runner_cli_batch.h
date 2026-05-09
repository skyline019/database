#pragma once

#include <string>

#include "cli/shell/bootstrap/demo_cli.h"
#include "cli/shell/state/shell_state_fwd.h"

struct CliBatchQueryBalance;
struct CliBatchFindId;
struct CliBatchPage;

/// `resolved_data_path` is the absolute `.bin` path (`resolve_table_file` in the caller).
/// Implemented in [`shell_state_ops.cc`](../state/shell_state_ops.cc) (standalone `Session` loads; no extra TU).
int demo_run_cli_batch_query_balance(ShellState& app,
                                     const std::string& resolved_data_path,
                                     const std::string& data_table,
                                     const CliBatchQueryBalance& qb);

int demo_run_cli_batch_find_id(ShellState& app,
                               const std::string& resolved_data_path,
                               const std::string& data_table,
                               const CliBatchFindId& bf);

int demo_run_cli_batch_page(ShellState& app,
                            const std::string& resolved_data_path,
                            const std::string& data_table,
                            const CliBatchPage& pg);
