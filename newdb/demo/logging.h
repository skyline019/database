#pragma once

#include <cstddef>
#include <string>

struct ShellState;

// Binds mirror FD + encryption flags from the active shell (call once per mode / after filling ShellState).
void logging_bind_shell(const ShellState* st);

// Optional: duplicate stdout/stderr-style output into a GUI (Qt) or other sink. Not thread-safe.
using NewdbConsoleEchoFn = void (*)(void* user, const char* text);
void logging_set_console_echo(NewdbConsoleEchoFn fn, void* user);
void logging_clear_console_echo();

// Nested capture (e.g. DatabaseEngine::executeCommand while MainWindow also echoes).
void logging_push_console_echo(NewdbConsoleEchoFn fn, void* user);
void logging_pop_console_echo();

// printf to stdout and optional echo (used by table_view / batch paths; session log unchanged).
void logging_console_printf(const char* fmt, ...);

// fprintf to stderr and optional echo (CLI errors + verbose).
void logging_stderr_printf(const char* fmt, ...);

// Append one user command line to the session log (plain line or legacy XOR frames).
void append_session_log_line(const char* log_file, const char* line, bool encrypt);

// Legacy: XOR-framed append (used when encrypt == true).
void append_encrypted_log(const char* log_file, const char* line);

// Plain UTF-8 line with trailing newline (default logging).
void append_plain_log_line(const char* log_file, const char* line);

// Echo to stdout, optional mirror FD, and session log file.
void log_and_print(const char* log_file, const char* fmt, ...);

// Dump session log: plain text lines, or legacy XOR format if detected.
void dump_log_file(const char* log_file);

// Deprecated name: calls dump_log_file.
void dump_encrypted_log(const char* log_file);

// Load existing log content as text (plain or legacy XOR framing).
std::string load_log_file_text(const char* log_file);

void log_session_separator(const char* log_file);
