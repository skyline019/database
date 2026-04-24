#include <newdb/wal_manager.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

namespace {

struct Args {
    std::string data_dir{"."};
    std::string db_name{"concurrent_million"};
    int threads{16};
    int total_ops{1000000};
    std::uint64_t segment_bytes{0};
    bool verify{false};
};

void usage() {
    std::cout
        << "newdb_concurrent_perf\n"
        << "  --data-dir <path>\n"
        << "  --db-name <name>\n"
        << "  --threads <n>\n"
        << "  --total-ops <n>\n"
        << "  --segment-bytes <n>\n"
        << "  --verify\n";
}

bool parse_int(const char* s, int& out) {
    try {
        out = std::stoi(s);
        return true;
    } catch (...) {
        return false;
    }
}

bool parse_u64(const char* s, std::uint64_t& out) {
    try {
        out = static_cast<std::uint64_t>(std::stoull(s));
        return true;
    } catch (...) {
        return false;
    }
}

bool parse_args(int argc, char** argv, Args& out) {
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        auto next = [&]() -> const char* {
            if (i + 1 >= argc) return nullptr;
            return argv[++i];
        };
        if (arg == "--help" || arg == "-h") {
            usage();
            return false;
        } else if (arg == "--data-dir") {
            const char* v = next();
            if (!v) return false;
            out.data_dir = v;
        } else if (arg == "--db-name") {
            const char* v = next();
            if (!v) return false;
            out.db_name = v;
        } else if (arg == "--threads") {
            const char* v = next();
            if (!v || !parse_int(v, out.threads)) return false;
        } else if (arg == "--total-ops") {
            const char* v = next();
            if (!v || !parse_int(v, out.total_ops)) return false;
        } else if (arg == "--segment-bytes") {
            const char* v = next();
            if (!v || !parse_u64(v, out.segment_bytes)) return false;
        } else if (arg == "--verify") {
            out.verify = true;
        } else {
            std::cerr << "unknown arg: " << arg << "\n";
            return false;
        }
    }
    return out.threads > 0 && out.total_ops > 0;
}

} // namespace

int main(int argc, char** argv) {
    Args args;
    if (!parse_args(argc, argv, args)) {
        usage();
        return 2;
    }

    std::filesystem::create_directories(args.data_dir);
    newdb::WalManager wal(args.db_name, args.data_dir);
    wal.set_sync_mode(newdb::WalSyncMode::Off);
    if (args.segment_bytes > 0) wal.set_segment_max_bytes(args.segment_bytes);
    if (!wal.open().ok) {
        std::cerr << "open wal failed\n";
        return 3;
    }

    const int base = args.total_ops / args.threads;
    const int rem = args.total_ops % args.threads;
    std::atomic<int> done{0};
    std::atomic<bool> ok{true};
    std::vector<std::thread> threads;
    threads.reserve(static_cast<std::size_t>(args.threads));

    auto t0 = std::chrono::steady_clock::now();
    for (int t = 0; t < args.threads; ++t) {
        const int cnt = base + (t < rem ? 1 : 0);
        threads.emplace_back([&, t, cnt]() {
            const std::uint64_t txn_base = static_cast<std::uint64_t>(t) * 1000000000ull;
            for (int i = 0; i < cnt; ++i) {
                newdb::Row r;
                r.id = t * 10000000 + i + 1;
                r.attrs["name"] = "u" + std::to_string(r.id);
                const std::uint64_t txn = txn_base + static_cast<std::uint64_t>(i + 1);
                if (!wal.append_record(txn, newdb::WalOp::INSERT, "users", &r).ok ||
                    !wal.commit_transaction(txn).ok) {
                    ok.store(false, std::memory_order_relaxed);
                    return;
                }
                done.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (auto& th : threads) th.join();
    if (!ok.load(std::memory_order_relaxed)) {
        std::cerr << "append/commit failed under concurrency\n";
        return 4;
    }
    if (!wal.flush().ok) {
        std::cerr << "flush failed\n";
        return 5;
    }
    auto t1 = std::chrono::steady_clock::now();

    std::size_t ins = 0;
    std::size_t com = 0;
    if (args.verify) {
        std::vector<newdb::WalDecodedRecord> recs;
        newdb::TableSchema schema;
        if (!wal.read_all_records(schema, recs).ok) {
            std::cerr << "read_all_records failed\n";
            return 6;
        }
        for (const auto& r : recs) {
            if (r.op == newdb::WalOp::INSERT) ++ins;
            if (r.op == newdb::WalOp::COMMIT) ++com;
        }
        if (ins != static_cast<std::size_t>(args.total_ops) || com != static_cast<std::size_t>(args.total_ops)) {
            std::cerr << "record count mismatch inserts=" << ins << " commits=" << com
                      << " expected=" << args.total_ops << "\n";
            return 7;
        }
    }
    wal.close();

    const auto elapsed_ms =
        static_cast<double>(std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count());
    const double tps = (args.total_ops * 1000.0) / (elapsed_ms > 0.0 ? elapsed_ms : 1.0);
    std::cout << "concurrent_ops=" << args.total_ops << "\n";
    std::cout << "threads=" << args.threads << "\n";
    std::cout << "elapsed_ms=" << elapsed_ms << "\n";
    std::cout << "tps=" << tps << "\n";
    std::cout << "wal_bytes=" << wal.wal_file_size_bytes() << "\n";
    std::cout << "verify=" << (args.verify ? 1 : 0) << "\n";
    std::cout << "status=ok\n";
    return 0;
}
