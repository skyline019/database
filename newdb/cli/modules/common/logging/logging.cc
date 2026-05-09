#include "cli/modules/common/logging/logging.h"

#include <cstdio>
#include <cstring>
#include <optional>
#include <string>
#include <vector>
#include <cstdarg>
#include <cstdint>
#include <ctime>
#if defined(__linux__)
#include <sys/socket.h>
#endif

static constexpr std::uint8_t kLogXorKey = 0x5Au;

static std::optional<LoggingShellSink> g_log_sink;

static NewdbConsoleEchoFn g_console_echo = nullptr;
static void* g_console_echo_user = nullptr;

void logging_bind_sink(const LoggingShellSink* sink) {
    if (sink == nullptr) {
        g_log_sink.reset();
    } else {
        g_log_sink = *sink;
    }
}

void logging_set_console_echo(NewdbConsoleEchoFn fn, void* user) {
    g_console_echo = fn;
    g_console_echo_user = user;
}

void logging_clear_console_echo() {
    g_console_echo = nullptr;
    g_console_echo_user = nullptr;
}

namespace {

struct EchoFrame {
    NewdbConsoleEchoFn fn;
    void* user;
};

std::vector<EchoFrame> g_console_echo_stack;

bool starts_with(const char* s, const char* prefix) {
    if (s == nullptr || prefix == nullptr) return false;
    const std::size_t n = std::strlen(prefix);
    return std::strncmp(s, prefix, n) == 0;
}

bool bench_quiet_log_enabled() {
    const char* v = std::getenv("NEWDB_BENCH_QUIET_LOG");
    if (v == nullptr) return false;
    return std::strcmp(v, "0") != 0;
}

bool should_persist_log_line(const char* line) {
    if (!bench_quiet_log_enabled()) return true;
    // In bench-quiet mode, keep only lines needed by benchmark parser.
    if (starts_with(line, "PROGRESS ") || starts_with(line, "PROGRESS_INNER ")) return true;
    if (starts_with(line, "{\"")) return true; // SHOW TUNING JSON snapshot lines.
    return false;
}

} // namespace

void logging_push_console_echo(NewdbConsoleEchoFn fn, void* user) {
    g_console_echo_stack.push_back(EchoFrame{g_console_echo, g_console_echo_user});
    g_console_echo = fn;
    g_console_echo_user = user;
}

void logging_pop_console_echo() {
    if (g_console_echo_stack.empty()) {
        g_console_echo = nullptr;
        g_console_echo_user = nullptr;
        return;
    }
    const EchoFrame top = g_console_echo_stack.back();
    g_console_echo_stack.pop_back();
    g_console_echo = top.fn;
    g_console_echo_user = top.user;
}

static void write_stdout_and_echo(const char* s) {
    if (s == nullptr) {
        return;
    }
    std::fputs(s, stdout);
    if (g_console_echo != nullptr) {
        g_console_echo(g_console_echo_user, s);
    }
}

void logging_console_printf(const char* fmt, ...) {
    char buf[65536];
    va_list ap;
    va_start(ap, fmt);
    const int n = std::vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    if (n < 0) {
        return;
    }
    buf[sizeof(buf) - 1] = '\0';
    write_stdout_and_echo(buf);
}

static void write_stderr_and_echo(const char* s) {
    if (s == nullptr) {
        return;
    }
    std::fputs(s, stderr);
    if (g_console_echo != nullptr) {
        g_console_echo(g_console_echo_user, s);
    }
}

void logging_stderr_printf(const char* fmt, ...) {
    char buf[65536];
    va_list ap;
    va_start(ap, fmt);
    const int n = std::vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    if (n < 0) {
        return;
    }
    buf[sizeof(buf) - 1] = '\0';
    write_stderr_and_echo(buf);
}

void append_plain_log_line(const char* log_file, const char* line) {
    if (log_file == nullptr) {
        return;
    }
    FILE* fp = std::fopen(log_file, "ab");
    if (!fp) {
        std::perror("open log for append");
        return;
    }
    std::fwrite(line, 1, std::strlen(line), fp);
    std::fputc('\n', fp);
    std::fclose(fp);
}

void append_encrypted_log(const char* log_file, const char* line) {
    if (log_file == nullptr) {
        return;
    }
    std::size_t len = std::strlen(line);
    FILE* fp = std::fopen(log_file, "ab");
    if (!fp) {
        std::perror("open log for append");
        return;
    }
    std::uint32_t ulen = static_cast<std::uint32_t>(len);
    std::fwrite(&ulen, sizeof(ulen), 1, fp);
    std::vector<unsigned char> buf(len);
    for (std::size_t i = 0; i < len; ++i) {
        buf[i] = static_cast<unsigned char>(line[i]) ^ kLogXorKey;
    }
    std::fwrite(buf.data(), 1, buf.size(), fp);
    std::fclose(fp);
}

void append_session_log_line(const char* log_file, const char* line, bool encrypt) {
    if (encrypt) {
        append_encrypted_log(log_file, line);
    } else {
        append_plain_log_line(log_file, line);
    }
}

void log_and_print(const char* log_file, const char* fmt, ...) {
    // SHOW TUNING JSON keeps growing with observability fields; use a larger
    // stack buffer to avoid truncating structured log lines.
    char buf[16384];
    va_list ap;
    va_start(ap, fmt);
    int n = std::vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    if (n < 0) {
        return;
    }
    buf[sizeof(buf) - 1] = '\0';
    write_stdout_and_echo(buf);
    const bool enc = g_log_sink.has_value() && g_log_sink->encrypt_log;
    if (should_persist_log_line(buf)) {
        append_session_log_line(log_file, buf, enc);
    }

#if defined(__linux__)
    if (g_log_sink.has_value() && g_log_sink->mirror_output_fd >= 0) {
        (void)send(g_log_sink->mirror_output_fd, buf, std::strlen(buf), 0);
    }
#endif
}

static bool dump_legacy_xor_framed_log(FILE* fp) {
    write_stdout_and_echo("=== Log (legacy XOR framing) ===\n");
    bool any = false;
    while (true) {
        std::uint32_t ulen = 0;
        if (std::fread(&ulen, sizeof(ulen), 1, fp) != 1) {
            break;
        }
        if (ulen == 0 || ulen > 64 * 1024) {
            return false;
        }
        std::vector<unsigned char> buf(ulen);
        if (std::fread(buf.data(), 1, buf.size(), fp) != buf.size()) {
            return false;
        }
        any = true;
        std::string line(ulen, '\0');
        for (std::size_t i = 0; i < ulen; ++i) {
            line[i] = static_cast<char>(buf[i] ^ kLogXorKey);
        }
        logging_console_printf("LOG: %s\n", line.c_str());
    }
    return any;
}

void dump_log_file(const char* log_file) {
    FILE* fp = std::fopen(log_file, "rb");
    if (!fp) {
        std::perror("open log for read");
        return;
    }
    std::uint32_t probe = 0;
    const size_t nread = std::fread(&probe, 1, sizeof(probe), fp);
    std::rewind(fp);
    if (nread == sizeof(probe) && probe > 0 && probe <= 64 * 1024) {
        if (dump_legacy_xor_framed_log(fp)) {
            std::fclose(fp);
            return;
        }
        std::rewind(fp);
    }
    logging_console_printf("=== Plain log from %s ===\n", log_file);
    char line[4096];
    while (std::fgets(line, sizeof(line), fp)) {
        write_stdout_and_echo(line);
    }
    std::fclose(fp);
}

void dump_encrypted_log(const char* log_file) {
    dump_log_file(log_file);
}

std::string load_log_file_text(const char* log_file) {
    if (log_file == nullptr) {
        return {};
    }
    FILE* fp = std::fopen(log_file, "rb");
    if (!fp) {
        return {};
    }
    std::uint32_t probe = 0;
    const size_t nread = std::fread(&probe, 1, sizeof(probe), fp);
    std::rewind(fp);

    std::string out;
    if (nread == sizeof(probe) && probe > 0 && probe <= 64 * 1024) {
        while (true) {
            std::uint32_t ulen = 0;
            if (std::fread(&ulen, sizeof(ulen), 1, fp) != 1) {
                break;
            }
            if (ulen == 0 || ulen > 64 * 1024) {
                out.clear();
                std::rewind(fp);
                break;
            }
            std::vector<unsigned char> buf(ulen);
            if (std::fread(buf.data(), 1, buf.size(), fp) != buf.size()) {
                out.clear();
                std::rewind(fp);
                break;
            }
            std::string line(ulen, '\0');
            for (std::size_t i = 0; i < ulen; ++i) {
                line[i] = static_cast<char>(buf[i] ^ kLogXorKey);
            }
            out += line;
            out.push_back('\n');
        }
        if (!out.empty()) {
            std::fclose(fp);
            return out;
        }
    }

    char line[4096];
    while (std::fgets(line, sizeof(line), fp)) {
        out += line;
    }
    std::fclose(fp);
    return out;
}

void log_session_separator(const char* log_file) {
    std::time_t now = std::time(nullptr);
    char ts[64]{};
    std::tm tm_buf{};
#if defined(_WIN32)
    localtime_s(&tm_buf, &now);
#else
    localtime_r(&now, &tm_buf);
#endif
    if (std::strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", &tm_buf) == 0) {
        std::strncpy(ts, "unknown-time", sizeof(ts) - 1);
        ts[sizeof(ts) - 1] = '\0';
    }
    log_and_print(log_file, "\n===== SESSION %s =====\n", ts);
}
