#include "cli/shell/c_api/cli_dispatch_command_line.h"

#include "cli/shell/dispatch/router/dispatch.h"
#include "cli/shell/state/shell_state_fwd.h"

bool cli_dispatch_execute_normalized_line(ShellState& st, const char* normalized_line) {
    return process_command_line(st, normalized_line);
}
