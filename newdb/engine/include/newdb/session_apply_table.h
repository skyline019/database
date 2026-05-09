#pragma once

#include <newdb/error.h>
#include <newdb/session.h>

#include <string>

namespace newdb {

/// Resolve `table_stem + ".bin"` against `data_dir` using the same rules as
/// `shell_state_paths.h` (`resolve_table_file_paths`; workspace-relative or absolute).
[[nodiscard]] std::string resolve_workspace_table_file(const std::string& data_dir,
                                                       const std::string& rel_or_abs);

/// Sets `session.table_name`, `session.data_path`, loads sidecar schema for that heap file, and
/// invalidates the session cache. Does **not** reset shell heap guards; callers that embed
/// `Session` inside `ShellState` should call `reset_session_heap_guard()` before this when reloading.
[[nodiscard]] Status session_apply_table_stem_and_reload_schema(Session& session,
                                                                const std::string& data_dir,
                                                                const std::string& table_stem);

} // namespace newdb
