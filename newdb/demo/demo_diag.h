#pragma once

struct ShellState;

// Extra diagnostics to stderr when ShellState::verbose is set.
void demo_verbose(const ShellState& st, const char* fmt, ...);
