#include <cstdlib>
#include <fstream>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

namespace {

struct Args {
    std::string input_jsonl;
    std::string output_json;
    std::string label_prefix;
    double min_vacuum_efficiency{-1.0};
    double max_conflict_rate{-1.0};
    int last_n{0};
};

struct Row {
    std::string label;
    std::uint64_t trigger_count{0};
    std::uint64_t execute_count{0};
    std::uint64_t cooldown_skips{0};
    std::uint64_t write_conflicts{0};
    bool ok{false};
};

void print_usage() {
    std::cout
        << "newdb_runtime_report\n"
        << "  --input <runtime_stats.jsonl> [--output <summary.json>]\n"
        << "  [--label-prefix <prefix>] [--last-n <N>=2 for before/after]\n"
        << "  [--min-vacuum-efficiency <0..1>] [--max-conflict-rate <0..1>]\n";
}

bool parse_double(const char* s, double& out) {
    try {
        out = std::stod(s);
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
            print_usage();
            return false;
        }
        if (arg == "--input") {
            const char* v = next();
            if (!v) return false;
            out.input_jsonl = v;
            continue;
        }
        if (arg == "--output") {
            const char* v = next();
            if (!v) return false;
            out.output_json = v;
            continue;
        }
        if (arg == "--min-vacuum-efficiency") {
            const char* v = next();
            if (!v || !parse_double(v, out.min_vacuum_efficiency)) return false;
            continue;
        }
        if (arg == "--max-conflict-rate") {
            const char* v = next();
            if (!v || !parse_double(v, out.max_conflict_rate)) return false;
            continue;
        }
        if (arg == "--label-prefix") {
            const char* v = next();
            if (!v) return false;
            out.label_prefix = v;
            continue;
        }
        if (arg == "--last-n") {
            const char* v = next();
            if (!v) return false;
            try {
                out.last_n = std::stoi(v);
            } catch (...) {
                return false;
            }
            if (out.last_n < 2) return false;
            continue;
        }
        std::cerr << "unknown argument: " << arg << "\n";
        return false;
    }
    return !out.input_jsonl.empty();
}

bool extract_u64_field(const std::string& line, const std::string& key, std::uint64_t& out) {
    const std::string needle = "\"" + key + "\":";
    const std::size_t pos = line.find(needle);
    if (pos == std::string::npos) return false;
    std::size_t i = pos + needle.size();
    while (i < line.size() && (line[i] == ' ' || line[i] == '\t')) ++i;
    if (i >= line.size()) return false;
    std::size_t j = i;
    while (j < line.size() && line[j] >= '0' && line[j] <= '9') ++j;
    if (j == i) return false;
    try {
        out = static_cast<std::uint64_t>(std::stoull(line.substr(i, j - i)));
        return true;
    } catch (...) {
        return false;
    }
}

bool extract_string_field(const std::string& line, const std::string& key, std::string& out) {
    const std::string needle = "\"" + key + "\":\"";
    const std::size_t pos = line.find(needle);
    if (pos == std::string::npos) return false;
    std::size_t i = pos + needle.size();
    const std::size_t j = line.find('"', i);
    if (j == std::string::npos) return false;
    out = line.substr(i, j - i);
    return true;
}

Row parse_row(const std::string& line) {
    Row r{};
    bool ok = true;
    std::string label;
    if (extract_string_field(line, "label", label)) r.label = label;
    ok = ok && extract_u64_field(line, "vacuum_trigger_count", r.trigger_count);
    ok = ok && extract_u64_field(line, "vacuum_execute_count", r.execute_count);
    ok = ok && extract_u64_field(line, "vacuum_cooldown_skip_count", r.cooldown_skips);
    ok = ok && extract_u64_field(line, "write_conflicts", r.write_conflicts);
    r.ok = ok;
    return r;
}

std::string build_summary_json(std::size_t samples,
                               std::uint64_t trigger_delta,
                               std::uint64_t execute_delta,
                               std::uint64_t skip_delta,
                               std::uint64_t conflict_delta,
                               double vacuum_efficiency,
                               double conflict_rate) {
    return std::string("{") +
           "\"samples\":" + std::to_string(samples) + "," +
           "\"vacuum_trigger_delta\":" + std::to_string(trigger_delta) + "," +
           "\"vacuum_execute_delta\":" + std::to_string(execute_delta) + "," +
           "\"vacuum_cooldown_skip_delta\":" + std::to_string(skip_delta) + "," +
           "\"write_conflict_delta\":" + std::to_string(conflict_delta) + "," +
           "\"vacuum_efficiency\":" + std::to_string(vacuum_efficiency) + "," +
           "\"conflict_rate\":" + std::to_string(conflict_rate) +
           "}";
}

} // namespace

int main(int argc, char** argv) {
    Args args;
    if (!parse_args(argc, argv, args)) {
        print_usage();
        return 2;
    }

    std::ifstream in(args.input_jsonl);
    if (!in.good()) {
        std::cerr << "failed to open input: " << args.input_jsonl << "\n";
        return 3;
    }

    std::vector<Row> rows;
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        Row r = parse_row(line);
        if (!r.ok) continue;
        if (!args.label_prefix.empty() && r.label.rfind(args.label_prefix, 0) != 0) continue;
        rows.push_back(r);
    }
    if (args.last_n > 0 && static_cast<std::size_t>(args.last_n) < rows.size()) {
        rows.erase(rows.begin(), rows.end() - args.last_n);
    }
    if (rows.size() < 2) {
        std::cerr << "need at least 2 valid snapshot rows\n";
        return 4;
    }

    const Row& first = rows.front();
    const Row& last = rows.back();
    const std::uint64_t trigger_delta = (last.trigger_count >= first.trigger_count)
                                            ? (last.trigger_count - first.trigger_count)
                                            : 0;
    const std::uint64_t execute_delta = (last.execute_count >= first.execute_count)
                                            ? (last.execute_count - first.execute_count)
                                            : 0;
    const std::uint64_t skip_delta = (last.cooldown_skips >= first.cooldown_skips)
                                         ? (last.cooldown_skips - first.cooldown_skips)
                                         : 0;
    const std::uint64_t conflict_delta = (last.write_conflicts >= first.write_conflicts)
                                             ? (last.write_conflicts - first.write_conflicts)
                                             : 0;

    const double vacuum_efficiency =
        (trigger_delta == 0) ? 1.0 : (static_cast<double>(execute_delta) / static_cast<double>(trigger_delta));
    const double conflict_rate =
        static_cast<double>(conflict_delta) / static_cast<double>(rows.size() - 1);

    const std::string summary = build_summary_json(
        rows.size(), trigger_delta, execute_delta, skip_delta, conflict_delta, vacuum_efficiency, conflict_rate);

    std::cout << summary << "\n";
    if (!args.output_json.empty()) {
        std::ofstream out(args.output_json, std::ios::out | std::ios::trunc);
        if (!out.good()) {
            std::cerr << "failed to write output: " << args.output_json << "\n";
            return 5;
        }
        out << summary << "\n";
    }

    if (args.min_vacuum_efficiency >= 0.0 && vacuum_efficiency < args.min_vacuum_efficiency) {
        std::cerr << "gate failed: vacuum_efficiency(" << vacuum_efficiency
                  << ") < min_vacuum_efficiency(" << args.min_vacuum_efficiency << ")\n";
        return 10;
    }
    if (args.max_conflict_rate >= 0.0 && conflict_rate > args.max_conflict_rate) {
        std::cerr << "gate failed: conflict_rate(" << conflict_rate
                  << ") > max_conflict_rate(" << args.max_conflict_rate << ")\n";
        return 11;
    }

    return 0;
}

