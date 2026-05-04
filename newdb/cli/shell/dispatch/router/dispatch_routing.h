#pragma once

// True when the trimmed command line can only be handled by phase-2 handlers (needs HeapTable),
// so phase-1 handler chain can be skipped for a small dispatch win.
bool shell_line_targets_phase2_only(const char* line);
