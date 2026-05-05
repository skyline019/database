#include "cli/modules/where/executor/stats/table_stats.h"

#include <algorithm>
#include <cmath>
#include <charconv>
#include <filesystem>
#include <fstream>
#include <string_view>
#include <sstream>
#include <system_error>
#include <chrono>

#include <newdb/heap_table.h>
#include <newdb/row.h>
#include <newdb/schema.h>

#include <unordered_map>
#include <unordered_set>

namespace {

bool row_slot_decode(const newdb::HeapTable& tbl, std::size_t slot, newdb::Row& out) {
    if (tbl.is_heap_storage_backed()) {
        return tbl.decode_heap_slot(slot, out);
    }
    if (slot < tbl.rows.size()) {
        out = tbl.rows[slot];
        return true;
    }
    return false;
}

bool split_fixed6(const std::string& line, std::string* out6) {
    std::size_t pos = 0;
    for (int i = 0; i < 5; ++i) {
        const auto p = line.find(';', pos);
        if (p == std::string::npos) {
            return false;
        }
        out6[i] = line.substr(pos, p - pos);
        pos = p + 1;
    }
    out6[5] = line.substr(pos);
    return true;
}

bool stats_segment_file_safe(const std::string& s) {
    return s.find_first_of(";\n\r|") == std::string::npos;
}

std::string stats_segment_or_dash(const std::string& s) {
    if (s.empty() || !stats_segment_file_safe(s)) {
        return "-";
    }
    return s;
}

std::string join_top_k_field(const std::vector<std::string>& tk) {
    std::string out;
    for (const std::string& t : tk) {
        if (!stats_segment_file_safe(t)) {
            continue;
        }
        if (!out.empty()) {
            out.push_back('|');
        }
        out += t;
    }
    return out.empty() ? "-" : out;
}

void parse_top_k_field(const std::string& seg, ColumnStats* cs) {
    if (seg.empty() || seg == "-") {
        return;
    }
    std::stringstream ss(seg);
    std::string item;
    while (std::getline(ss, item, '|')) {
        if (item.empty() || item == "-") {
            continue;
        }
        if (cs->top_k.size() < 3) {
            cs->top_k.push_back(item);
        }
    }
}

}  // namespace

std::uint64_t fnv1a64_update(std::uint64_t h, const std::string_view sv) {
    constexpr std::uint64_t kFnvOffset = 14695981039346656037ULL;
    constexpr std::uint64_t kFnvPrime = 1099511628211ULL;
    if (h == 0) {
        h = kFnvOffset;
    }
    for (unsigned char c : sv) {
        h ^= static_cast<std::uint64_t>(c);
        h *= kFnvPrime;
    }
    return h;
}

std::string table_stats_file_path_for_data_file(const std::string& data_file) {
    return data_file + ".tablestats";
}

std::uint64_t table_stats_schema_fingerprint(const newdb::TableSchema& schema) {
    std::uint64_t h = 0;
    h = fnv1a64_update(h, schema.primary_key);
    for (const auto& am : schema.attrs) {
        h = fnv1a64_update(h, "|");
        h = fnv1a64_update(h, am.name);
    }
    return h;
}

void invalidate_table_stats_for_data_file(const std::string& data_file) {
    if (data_file.empty()) {
        return;
    }
    std::error_code ec;
    std::filesystem::remove(table_stats_file_path_for_data_file(data_file), ec);
}

bool save_table_stats_file(const std::string& data_file,
                           const newdb::TableSchema& schema,
                           const TableStats& stats) {
    if (data_file.empty()) {
        return false;
    }
    const std::string path = table_stats_file_path_for_data_file(data_file);
    const std::string tmp_path = path + ".tmp";
    std::ofstream out(tmp_path, std::ios::out | std::ios::trunc);
    if (!out) {
        return false;
    }
    out << "NEWDB_TABLESTATS_V3\n";
    out << "fp=" << table_stats_schema_fingerprint(schema) << "\n";
    const auto now = std::chrono::system_clock::now();
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    out << "built_ts_ms=" << static_cast<long long>(ms) << "\n";
    out << "row_count=" << stats.row_count << "\n";
    for (const auto& am : schema.attrs) {
        const auto it = stats.columns.find(am.name);
        const std::uint64_t nn = (it == stats.columns.end()) ? 0u : it->second.non_null_count;
        const std::uint64_t d = (it == stats.columns.end()) ? 0u : it->second.distinct_count;
        const std::string min_s =
            (it == stats.columns.end()) ? "-" : stats_segment_or_dash(it->second.min_value);
        const std::string max_s =
            (it == stats.columns.end()) ? "-" : stats_segment_or_dash(it->second.max_value);
        const std::string top_s =
            (it == stats.columns.end()) ? "-" : join_top_k_field(it->second.top_k);
        out << am.name << ";" << nn << ";" << d << ";" << min_s << ";" << max_s << ";" << top_s << "\n";
    }
    bool any_hist = false;
    for (const auto& am : schema.attrs) {
        const auto it = stats.columns.find(am.name);
        if (it == stats.columns.end()) {
            continue;
        }
        if (it->second.histogram_buckets.size() == 8) {
            any_hist = true;
            break;
        }
    }
    if (any_hist) {
        out << "__column_histograms__\n";
        for (const auto& am : schema.attrs) {
            const auto it = stats.columns.find(am.name);
            if (it == stats.columns.end() || it->second.histogram_buckets.size() != 8) {
                continue;
            }
            out << am.name << ":";
            for (std::size_t i = 0; i < 8; ++i) {
                if (i > 0) {
                    out << ',';
                }
                out << it->second.histogram_buckets[i];
            }
            out << "\n";
        }
    }
    out.flush();
    out.close();
    if (!out) {
        std::error_code ec;
        std::filesystem::remove(tmp_path, ec);
        return false;
    }
    std::error_code ec;
    std::filesystem::remove(path, ec);
    ec.clear();
    std::filesystem::rename(tmp_path, path, ec);
    return !ec;
}

bool load_table_stats_file(const std::string& data_file,
                           const newdb::TableSchema& schema,
                           TableStats* out) {
    if (out == nullptr || data_file.empty()) {
        return false;
    }
    const std::string path = table_stats_file_path_for_data_file(data_file);
    std::ifstream in(path, std::ios::in);
    if (!in) {
        return false;
    }
    std::string line;
    if (!std::getline(in, line)) {
        return false;
    }
    const bool file_v3 = (line == "NEWDB_TABLESTATS_V3");
    const bool file_v2 = (line == "NEWDB_TABLESTATS_V2");
    const bool file_v1 = (line == "NEWDB_TABLESTATS_V1");
    if (!file_v1 && !file_v2 && !file_v3) {
        return false;
    }
    if (!std::getline(in, line) || line.rfind("fp=", 0) != 0) {
        return false;
    }
    const std::string fp_s = line.substr(3);
    std::uint64_t fp = 0;
    {
        const char* p = fp_s.c_str();
        const char* end = p + fp_s.size();
        const auto r = std::from_chars(p, end, fp);
        if (r.ec != std::errc{} || r.ptr != end) {
            return false;
        }
    }
    const std::uint64_t schema_fp = table_stats_schema_fingerprint(schema);
    if (fp != schema_fp) {
        return false;
    }
    if (!std::getline(in, line)) {
        return false;
    }
    std::uint64_t built_ts_ms = 0;
    std::string row_count_line = line;
    if (line.rfind("built_ts_ms=", 0) == 0) {
        const std::string ts_s = line.substr(12);
        const char* p = ts_s.c_str();
        const char* end = p + ts_s.size();
        long long ts_ll = 0;
        const auto r = std::from_chars(p, end, ts_ll);
        if (r.ec != std::errc{} || r.ptr != end || ts_ll < 0) {
            return false;
        }
        built_ts_ms = static_cast<std::uint64_t>(ts_ll);
        if (!std::getline(in, row_count_line)) {
            return false;
        }
    }
    if (row_count_line.rfind("row_count=", 0) != 0) {
        return false;
    }
    const std::string rc_s = row_count_line.substr(10);
    std::uint64_t row_count = 0;
    {
        const char* p = rc_s.c_str();
        const char* end = p + rc_s.size();
        const auto r = std::from_chars(p, end, row_count);
        if (r.ec != std::errc{} || r.ptr != end) {
            return false;
        }
    }
    *out = TableStats{};
    out->row_count = row_count;
    out->stats_built_ts_ms = built_ts_ms;
    out->stats_schema_fp = schema_fp;
    for (const auto& am : schema.attrs) {
        out->columns[am.name] = ColumnStats{};
    }
    bool hist_section = false;
    while (std::getline(in, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (line.empty()) {
            continue;
        }
        if (!hist_section && line == "__column_histograms__") {
            hist_section = true;
            continue;
        }
        if (hist_section) {
            const auto colon = line.find(':');
            if (colon == std::string::npos) {
                return false;
            }
            const std::string hname = line.substr(0, colon);
            const std::string buckets_csv = line.substr(colon + 1);
            if (out->columns.count(hname) == 0) {
                return false;
            }
            ColumnStats& hcs = out->columns[hname];
            hcs.histogram_buckets.clear();
            std::stringstream hbss(buckets_csv);
            std::string bitem;
            while (std::getline(hbss, bitem, ',')) {
                std::uint64_t bv = 0;
                const char* pb = bitem.c_str();
                const char* eb = pb + bitem.size();
                const auto br = std::from_chars(pb, eb, bv);
                if (br.ec != std::errc{} || br.ptr != eb) {
                    return false;
                }
                hcs.histogram_buckets.push_back(bv);
            }
            if (hcs.histogram_buckets.size() != 8) {
                return false;
            }
            continue;
        }
        if (file_v2 || file_v3) {
            std::string f[6];
            if (!split_fixed6(line, f)) {
                return false;
            }
            const std::string& name = f[0];
            std::uint64_t nn = 0;
            std::uint64_t d = 0;
            {
                const char* p = f[1].c_str();
                const char* end = p + f[1].size();
                const auto r = std::from_chars(p, end, nn);
                if (r.ec != std::errc{} || r.ptr != end) {
                    return false;
                }
            }
            {
                const char* p = f[2].c_str();
                const char* end = p + f[2].size();
                const auto r = std::from_chars(p, end, d);
                if (r.ec != std::errc{} || r.ptr != end) {
                    return false;
                }
            }
            if (out->columns.count(name) == 0) {
                return false;
            }
            ColumnStats& cs = out->columns[name];
            cs.non_null_count = nn;
            cs.distinct_count = d;
            if (f[3] != "-") {
                cs.min_value = f[3];
            }
            if (f[4] != "-") {
                cs.max_value = f[4];
            }
            parse_top_k_field(f[5], &cs);
            continue;
        }
        const auto sc1 = line.find(';');
        const auto sc2 = line.find(';', sc1 == std::string::npos ? 0 : sc1 + 1);
        if (sc1 == std::string::npos || sc2 == std::string::npos) {
            return false;
        }
        const std::string name = line.substr(0, sc1);
        std::uint64_t nn = 0;
        std::uint64_t d = 0;
        {
            const char* p = line.c_str() + sc1 + 1;
            const char* end = line.c_str() + sc2;
            if (std::from_chars(p, end, nn).ec != std::errc{}) {
                return false;
            }
        }
        {
            const char* p = line.c_str() + sc2 + 1;
            const char* end = line.c_str() + line.size();
            if (std::from_chars(p, end, d).ec != std::errc{}) {
                return false;
            }
        }
        if (out->columns.count(name) == 0) {
            return false;
        }
        out->columns[name].non_null_count = nn;
        out->columns[name].distinct_count = d;
    }
    return true;
}

bool build_table_stats_from_heap(const newdb::HeapTable& tbl,
                                 const newdb::TableSchema& schema,
                                 TableStats* out) {
    if (out == nullptr) {
        return false;
    }
    *out = TableStats{};
    out->stats_schema_fp = table_stats_schema_fingerprint(schema);
    const std::size_t n = tbl.logical_row_count();
    out->row_count = static_cast<std::uint64_t>(n);
    for (const auto& am : schema.attrs) {
        out->columns[am.name] = ColumnStats{};
    }
    newdb::Row r;
    for (const auto& am : schema.attrs) {
        ColumnStats& cs = out->columns[am.name];
        std::unordered_set<std::string> distinct;
        distinct.reserve(std::min<std::size_t>(n, 4096));
        std::unordered_map<std::string, std::size_t> freq;
        bool have_min = false;
        for (std::size_t slot = 0; slot < n; ++slot) {
            if (!row_slot_decode(tbl, slot, r)) {
                continue;
            }
            auto it = r.attrs.find(am.name);
            if (it == r.attrs.end() || it->second.empty()) {
                continue;
            }
            const std::string& v = it->second;
            ++cs.non_null_count;
            distinct.insert(v);
            ++freq[v];
            if (!have_min || v < cs.min_value) {
                cs.min_value = v;
            }
            if (!have_min || v > cs.max_value) {
                cs.max_value = v;
            }
            have_min = true;
        }
        cs.distinct_count = static_cast<std::uint64_t>(distinct.size());
        if (!freq.empty()) {
            std::vector<std::pair<std::string, std::size_t>> fv(freq.begin(), freq.end());
            std::sort(fv.begin(), fv.end(), [](const auto& a, const auto& b) {
                if (a.second != b.second) {
                    return a.second > b.second;
                }
                return a.first < b.first;
            });
            for (std::size_t i = 0; i < fv.size() && cs.top_k.size() < 3; ++i) {
                cs.top_k.push_back(fv[i].first);
            }
        }
        std::vector<std::string> vals;
        vals.reserve(cs.non_null_count);
        for (std::size_t slot = 0; slot < n; ++slot) {
            if (!row_slot_decode(tbl, slot, r)) {
                continue;
            }
            auto itv = r.attrs.find(am.name);
            if (itv == r.attrs.end() || itv->second.empty()) {
                continue;
            }
            vals.push_back(itv->second);
        }
        if (!vals.empty()) {
            std::sort(vals.begin(), vals.end());
            constexpr std::size_t nb = 8;
            cs.histogram_buckets.assign(nb, 0);
            for (std::size_t i = 0; i < vals.size(); ++i) {
                std::size_t b = (i * nb) / vals.size();
                if (b >= nb) {
                    b = nb - 1;
                }
                cs.histogram_buckets[b] += 1;
            }
        }
    }
    return true;
}

double eq_selectivity_from_stats(const TableStats* stats,
                                 const std::string& attr,
                                 const std::size_t logical_rows) {
    if (stats == nullptr || logical_rows == 0) {
        return 0.0;
    }
    const auto it = stats->columns.find(attr);
    if (it == stats->columns.end() || it->second.distinct_count == 0) {
        return 0.0;
    }
    const long double ndv = static_cast<long double>(it->second.distinct_count);
    const long double rows = static_cast<long double>(logical_rows);
    const long double est = rows / ndv;
    if (est < 1.0L) {
        return 1.0 / static_cast<double>(ndv);
    }
    return static_cast<double>(est / rows);
}

double range_selectivity_from_stats(const TableStats* stats,
                                    const std::string& attr,
                                    const std::size_t logical_rows) {
    const double eq = eq_selectivity_from_stats(stats, attr, logical_rows);
    if (eq <= 0.0 || logical_rows == 0) {
        return 0.0;
    }
    const double span = std::sqrt(eq);
    return std::min(0.5, std::max(eq * 4.0, span));
}

bool table_stats_matches_schema(const TableStats& stats, const newdb::TableSchema& schema) {
    const std::uint64_t cur = table_stats_schema_fingerprint(schema);
    return stats.stats_schema_fp != 0 && stats.stats_schema_fp == cur;
}
