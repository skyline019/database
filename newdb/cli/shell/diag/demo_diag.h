#pragma once

struct ShellStateFacade;

// Extra diagnostics to stderr when ShellState::verbose is set (via facade).
void demo_verbose(const ShellStateFacade& f, const char* fmt, ...);
