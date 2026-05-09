#pragma once

class ShellState;

/// Narrow surface for executing one normalized CLI line via the dispatch router.
/// Implemented in `cli_dispatch_command_line.cc` so embedders (e.g. `c_api_cli_bridge.cc`)
/// do not include `dispatch.h`.
[[nodiscard]] bool cli_dispatch_execute_normalized_line(ShellState& st, const char* normalized_line);
