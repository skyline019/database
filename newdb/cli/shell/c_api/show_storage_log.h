#pragma once

class ShellStateFacade;

namespace newdb::capi_cli {

/// Workspace WAL / *.bin summary line (`[STORAGE] ...`), shared with [`txn_handler.cc`](../dispatch/handlers/txn/txn_handler.cc).
/// Does not append the session command line; callers do that when needed (dispatch prepends via `process_command_line`).
void emit_show_storage_log_lines(ShellStateFacade& f, const char* log_file);

} // namespace newdb::capi_cli
