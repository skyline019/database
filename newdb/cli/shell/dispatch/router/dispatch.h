#pragma once

#include "cli/shell/state/shell_state_fwd.h"

// Handle one interactive or --exec line; returns false to end the shell session.
bool process_command_line(ShellState& st, const char* input_line);
