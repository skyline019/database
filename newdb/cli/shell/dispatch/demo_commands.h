#pragma once

struct ShellState;

// Handle one interactive or --exec line; returns false to end the shell session.
bool process_command_line(ShellState& st, const char* input_line);
