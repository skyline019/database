#pragma once

struct ShellState;

// When session has no table/file yet, scan workspace for *.bin (same rules as interactive shell).
void demo_autopick_initial_table_if_empty(ShellState& st);

void interactive_shell(ShellState& st, const char* data_file, const char* table_name);

void run_mdb_script(ShellState& st, const char* script_file);
