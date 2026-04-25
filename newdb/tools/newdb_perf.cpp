#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

namespace {

struct PerfArgs {
    std::string demo_exe{"newdb_demo.exe"};
    std::string data_dir{};
    std::string sizes_csv{"1000000"};
    int query_loops{1};
    int txn_per_mode{60};
    int build_chunk_size{50000};
};

void print_usage() {
    std::cout
        << "newdb_perf - run scripts/bench/million_scale_bench.ps1 via dedicated executable\n"
        << "Usage:\n"
        << "  newdb_perf [--demo-exe <path>] [--data-dir <path>] [--sizes <csv>]\n"
        << "             [--query-loops <n>] [--txn-per-mode <n>] [--build-chunk-size <n>]\n";
}

bool parse_int_arg(const char* text, int& out) {
    try {
        out = std::stoi(text);
        return true;
    } catch (...) {
        return false;
    }
}

bool parse_args(int argc, char** argv, PerfArgs& out) {
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        auto need_val = [&](const char* name) -> const char* {
            if (i + 1 >= argc) {
                std::cerr << "[newdb_perf] missing value for " << name << "\n";
                return nullptr;
            }
            return argv[++i];
        };
        if (arg == "--help" || arg == "-h") {
            print_usage();
            return false;
        }
        if (arg == "--demo-exe") {
            const char* v = need_val("--demo-exe");
            if (!v) return false;
            out.demo_exe = v;
            continue;
        }
        if (arg == "--data-dir") {
            const char* v = need_val("--data-dir");
            if (!v) return false;
            out.data_dir = v;
            continue;
        }
        if (arg == "--sizes") {
            const char* v = need_val("--sizes");
            if (!v) return false;
            out.sizes_csv = v;
            continue;
        }
        if (arg == "--query-loops") {
            const char* v = need_val("--query-loops");
            if (!v || !parse_int_arg(v, out.query_loops)) return false;
            continue;
        }
        if (arg == "--txn-per-mode") {
            const char* v = need_val("--txn-per-mode");
            if (!v || !parse_int_arg(v, out.txn_per_mode)) return false;
            continue;
        }
        if (arg == "--build-chunk-size") {
            const char* v = need_val("--build-chunk-size");
            if (!v || !parse_int_arg(v, out.build_chunk_size)) return false;
            continue;
        }
        std::cerr << "[newdb_perf] unknown argument: " << arg << "\n";
        return false;
    }
    return true;
}

std::string quote_ps(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 2);
    out.push_back('"');
    for (const char ch : s) {
        if (ch == '"') {
            out.append("\\\"");
        } else {
            out.push_back(ch);
        }
    }
    out.push_back('"');
    return out;
}

} // namespace

int main(int argc, char** argv) {
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--help" || arg == "-h") {
            print_usage();
            return 0;
        }
    }
    PerfArgs args;
    if (!parse_args(argc, argv, args)) {
        return 2;
    }
    if (args.data_dir.empty()) {
        args.data_dir = (std::filesystem::temp_directory_path() / "newdb_perf_workspace").string();
    }

#if !defined(_WIN32)
    std::cerr << "[newdb_perf] currently supports Windows PowerShell only.\n";
    return 3;
#else
    const std::filesystem::path exe_path =
        std::filesystem::weakly_canonical(std::filesystem::path(argv[0]));
    const std::filesystem::path script =
        exe_path.parent_path().parent_path() / "scripts" / "bench" / "million_scale_bench.ps1";
    if (!std::filesystem::exists(script)) {
        std::cerr << "[newdb_perf] benchmark script not found: " << script.string() << "\n";
        return 4;
    }

    std::string cmd;
    cmd.reserve(1024);
    cmd += "powershell -ExecutionPolicy Bypass -File ";
    cmd += quote_ps(script.string());
    cmd += " -DemoExe ";
    cmd += quote_ps(args.demo_exe);
    cmd += " -DataDir ";
    cmd += quote_ps(args.data_dir);
    cmd += " -SizesCsv ";
    cmd += quote_ps(args.sizes_csv);
    cmd += " -QueryLoops ";
    cmd += std::to_string(args.query_loops);
    cmd += " -TxnPerMode ";
    cmd += std::to_string(args.txn_per_mode);
    cmd += " -BuildChunkSize ";
    cmd += std::to_string(args.build_chunk_size);

    std::cout << "[newdb_perf] running: " << cmd << "\n";
    const int code = std::system(cmd.c_str());
    if (code != 0) {
        std::cerr << "[newdb_perf] benchmark failed, exit code=" << code << "\n";
        return 1;
    }
    std::cout << "[newdb_perf] benchmark finished.\n";
    return 0;
#endif
}
