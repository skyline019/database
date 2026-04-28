#include <waterfall/config.h>

#include <newdb/lsm_lite.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace newdb::lsm_lite {
void shutdown_background_workers();

namespace {
namespace fs = std::filesystem;

struct SegmentRow {
    std::uint64_t seq{0};
    int id{0};
    bool deleted{false};
    std::string attrs_blob;
};

struct CacheEntry {
    std::uint64_t version{0};
    std::optional<SegmentRow> row;
};

struct SegmentMeta {
    int min_id{0};
    int max_id{0};
    bool initialized{false};
};

struct State {
    std::uint64_t next_seq{1};
    std::uint64_t next_segment_id{1};
    std::uint64_t version{1};
    std::vector<SegmentRow> memtable;
    std::uint64_t mem_bytes{0};
    std::unordered_map<int, CacheEntry> find_cache;
    std::unordered_map<std::string, SegmentMeta> segment_meta_by_path;
    std::unordered_map<std::int64_t, std::unordered_map<int, SegmentRow>> txn_rows_by_txn;

    Options last_opt{};
    Hooks last_hooks{};

    std::uint64_t compaction_pending{0};
    std::uint64_t compaction_inflight{0};
};

std::mutex g_mu;
std::unordered_map<std::string, State> g_states;

void maybe_compact_locked(const Options& opt,
                          const std::string& data_file,
                          State& state,
                          const fs::path& seg_dir,
                          Hooks* hooks);

struct CompactionJob {
    std::string data_file;
};

std::mutex g_compact_mu;
std::condition_variable g_compact_cv;
std::deque<CompactionJob> g_compact_q;
bool g_compact_shutdown = false;
std::uint64_t g_compact_worker_count = 0;
std::vector<std::thread> g_compact_threads;

void ensure_compaction_workers(const std::uint64_t n) {
    if (n == 0) return;
    std::lock_guard<std::mutex> lk(g_compact_mu);
    static bool atexit_registered = false;
    if (!atexit_registered) {
        std::atexit([]() { shutdown_background_workers(); });
        atexit_registered = true;
    }
    if (g_compact_shutdown) g_compact_shutdown = false;
    if (g_compact_worker_count >= n) return;
    const std::uint64_t to_add = n - g_compact_worker_count;
    for (std::uint64_t i = 0; i < to_add; ++i) {
        g_compact_threads.emplace_back([]() {
            for (;;) {
                CompactionJob job;
                {
                    std::unique_lock<std::mutex> lk(g_compact_mu);
                    g_compact_cv.wait(lk, []() { return g_compact_shutdown || !g_compact_q.empty(); });
                    if (g_compact_shutdown && g_compact_q.empty()) return;
                    job = std::move(g_compact_q.front());
                    g_compact_q.pop_front();
                }

                std::optional<Options> opt;
                std::optional<Hooks> hooks;
                fs::path seg_dir;
                {
                    std::lock_guard<std::mutex> lk(g_mu);
                    auto it = g_states.find(job.data_file);
                    if (it == g_states.end()) continue;
                    State& st = it->second;
                    if (st.compaction_pending > 0) st.compaction_pending--;
                    st.compaction_inflight++;
                    opt = st.last_opt;
                    hooks = st.last_hooks;
                    seg_dir = fs::path(job.data_file + ".lsm");
                    if (hooks && hooks->on_compaction_queue_depth) {
                        hooks->on_compaction_queue_depth(st.compaction_pending, st.compaction_inflight);
                    }
                }

                if (!opt.has_value()) continue;
                Options opt_sync = *opt;
                opt_sync.compaction_async = false;

                // Run compaction (coarse lock for simplicity/correctness).
                {
                    std::lock_guard<std::mutex> lk(g_mu);
                    auto it = g_states.find(job.data_file);
                    if (it != g_states.end()) {
                        maybe_compact_locked(opt_sync, job.data_file, it->second, seg_dir, hooks ? &*hooks : nullptr);
                    }
                }

                {
                    std::lock_guard<std::mutex> lk(g_mu);
                    auto it = g_states.find(job.data_file);
                    if (it == g_states.end()) continue;
                    State& st = it->second;
                    if (st.compaction_inflight > 0) st.compaction_inflight--;
                    if (hooks && hooks->on_compaction_queue_depth) {
                        hooks->on_compaction_queue_depth(st.compaction_pending, st.compaction_inflight);
                    }
                }
                g_compact_cv.notify_all();
            }
        });
        g_compact_worker_count++;
    }
}

std::uint64_t effective_segment_target_bytes(const Options& opt) {
    if (opt.segment_target_bytes != 0) return opt.segment_target_bytes;
    return 256;
}

std::string row_to_blob(const newdb::Row& row) {
    std::string out;
    for (const auto& kv : row.attrs) {
        out += kv.first;
        out += '=';
        out += kv.second;
        out += ';';
    }
    return out;
}

newdb::Row blob_to_row(const SegmentRow& sr) {
    newdb::Row r;
    r.id = sr.id;
    std::string token;
    std::stringstream ss(sr.attrs_blob);
    while (std::getline(ss, token, ';')) {
        if (token.empty()) continue;
        const std::size_t eq = token.find('=');
        if (eq == std::string::npos) continue;
        r.attrs[token.substr(0, eq)] = token.substr(eq + 1);
    }
    return r;
}

bool parse_segment_row_line(const std::string& line, SegmentRow& out) {
    std::istringstream iss(line);
    int deleted = 0;
    if (!(iss >> out.seq >> out.id >> deleted)) return false;
    out.deleted = (deleted != 0);
    std::getline(iss, out.attrs_blob);
    if (!out.attrs_blob.empty() && out.attrs_blob.front() == '\t') {
        out.attrs_blob.erase(out.attrs_blob.begin());
    }
    return true;
}

std::vector<fs::path> list_segment_logs(const fs::path& dir) {
    std::error_code ec;
    std::vector<fs::path> out;
    if (!fs::exists(dir, ec)) return out;
    for (fs::directory_iterator it(dir, ec), end; !ec && it != end; it.increment(ec)) {
        if (!it->is_regular_file(ec)) continue;
        if (it->path().extension() == ".log") out.push_back(it->path());
    }
    std::sort(out.begin(), out.end());
    return out;
}

SegmentMeta meta_from_rows(const std::vector<SegmentRow>& rows) {
    SegmentMeta m;
    if (rows.empty()) return m;
    int minv = rows.front().id;
    int maxv = rows.front().id;
    for (const auto& r : rows) {
        minv = std::min(minv, r.id);
        maxv = std::max(maxv, r.id);
    }
    m.min_id = minv;
    m.max_id = maxv;
    m.initialized = true;
    return m;
}

SegmentMeta meta_from_file(const fs::path& p) {
    SegmentMeta m;
    std::ifstream in(p);
    std::string line;
    bool first = true;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        SegmentRow r;
        if (!parse_segment_row_line(line, r)) continue;
        if (first) {
            m.min_id = r.id;
            m.max_id = r.id;
            m.initialized = true;
            first = false;
        } else {
            m.min_id = std::min(m.min_id, r.id);
            m.max_id = std::max(m.max_id, r.id);
        }
    }
    return m;
}

bool ranges_overlap(const SegmentMeta& a, const SegmentMeta& b) {
    if (!a.initialized || !b.initialized) return false;
    return !(a.max_id < b.min_id || b.max_id < a.min_id);
}

std::size_t overlap_score(const std::vector<SegmentMeta>& metas, const std::size_t begin, const std::size_t end) {
    std::size_t score = 0;
    for (std::size_t i = begin; i < end; ++i) {
        for (std::size_t j = i + 1; j < end; ++j) {
            if (ranges_overlap(metas[i], metas[j])) {
                ++score;
            }
        }
    }
    return score;
}

void hook_memtable_bytes(Hooks* h, const std::uint64_t v) {
    if (h && h->on_memtable_bytes) h->on_memtable_bytes(v);
}
void hook_segment_count(Hooks* h, const std::uint64_t v) {
    if (h && h->on_segment_count) h->on_segment_count(v);
}
void refresh_segment_count(Hooks* h, const fs::path& seg_dir) {
    hook_segment_count(h, static_cast<std::uint64_t>(list_segment_logs(seg_dir).size()));
}

void maybe_compact_locked(const Options& opt,
                          const std::string& data_file,
                          State& state,
                          const fs::path& seg_dir,
                          Hooks* hooks) {
    auto segs = list_segment_logs(seg_dir);
    if (segs.size() < static_cast<std::size_t>(opt.l0_compact_trigger)) {
        if (hooks && hooks->on_compaction_queue_depth) {
            hooks->on_compaction_queue_depth(0, 0);
        }
        refresh_segment_count(hooks, seg_dir);
        return;
    }

    if (opt.compaction_async) {
        ensure_compaction_workers(opt.compaction_workers);
        if (opt.compaction_max_pending != 0 && state.compaction_pending >= opt.compaction_max_pending) {
            if (hooks && hooks->on_compaction_enqueue_skipped_backpressure) {
                hooks->on_compaction_enqueue_skipped_backpressure();
            }
            if (hooks && hooks->on_compaction_queue_depth) {
                hooks->on_compaction_queue_depth(state.compaction_pending, state.compaction_inflight);
            }
            return;
        }
        {
            std::lock_guard<std::mutex> lk(g_compact_mu);
            g_compact_q.push_back(CompactionJob{data_file});
        }
        state.compaction_pending++;
        if (hooks && hooks->on_compaction_queue_depth) {
            hooks->on_compaction_queue_depth(state.compaction_pending, state.compaction_inflight);
        }
        g_compact_cv.notify_one();
        return;
    }

    const std::size_t batch_size = std::max<std::size_t>(1, static_cast<std::size_t>(opt.l0_compact_batch));
    std::size_t max_batches = (opt.compaction_reap_budget == 0)
                                  ? (segs.size() + batch_size - 1) / batch_size
                                  : static_cast<std::size_t>(opt.compaction_reap_budget);
    if (max_batches == 0) max_batches = 1;
    if (opt.compaction_policy == CompactionPolicy::LeveledLite &&
        opt.leveled_l1_hard_segments > 0 &&
        segs.size() > static_cast<std::size_t>(opt.leveled_l1_hard_segments)) {
        const auto overflow = segs.size() - static_cast<std::size_t>(opt.leveled_l1_hard_segments);
        max_batches = std::max<std::size_t>(max_batches, (overflow + batch_size - 1) / batch_size);
    }

    std::error_code ec;
    while (!segs.empty() && max_batches-- > 0) {
        std::size_t start = 0;
        std::size_t end = std::min<std::size_t>(segs.size(), batch_size);
        if (opt.compaction_policy == CompactionPolicy::LeveledLite) {
            // Prefer windows with maximal key-range overlap; tie-break toward newer windows.
            std::vector<SegmentMeta> metas;
            metas.reserve(segs.size());
            for (const auto& seg : segs) {
                const std::string seg_path = seg.string();
                auto it = state.segment_meta_by_path.find(seg_path);
                if (it == state.segment_meta_by_path.end()) {
                    state.segment_meta_by_path[seg_path] = meta_from_file(seg);
                    it = state.segment_meta_by_path.find(seg_path);
                }
                metas.push_back(it->second);
            }
            std::size_t best_begin = 0;
            std::size_t best_score = 0;
            const std::size_t max_begin = segs.size() > batch_size ? (segs.size() - batch_size) : 0;
            for (std::size_t b = 0; b <= max_begin; ++b) {
                const std::size_t e = std::min<std::size_t>(segs.size(), b + batch_size);
                const std::size_t score = overlap_score(metas, b, e);
                if (score >= best_score) {
                    best_score = score;
                    best_begin = b;
                }
            }
            start = best_begin;
            end = std::min<std::size_t>(segs.size(), start + batch_size);
        }
        std::unordered_map<int, SegmentRow> latest;
        std::uint64_t bytes_in = 0;
        for (std::size_t i = start; i < end; ++i) {
            const auto& seg = segs[i];
            bytes_in += static_cast<std::uint64_t>(fs::file_size(seg, ec));
            std::ifstream in(seg);
            std::string line;
            while (std::getline(in, line)) {
                if (line.empty()) continue;
                SegmentRow r;
                if (!parse_segment_row_line(line, r)) continue;
                auto it = latest.find(r.id);
                if (it == latest.end() || it->second.seq < r.seq) {
                    latest[r.id] = std::move(r);
                }
            }
        }
        std::vector<SegmentRow> merged;
        merged.reserve(latest.size());
        for (auto& kv : latest) merged.push_back(std::move(kv.second));
        std::sort(merged.begin(), merged.end(), [](const SegmentRow& a, const SegmentRow& b) { return a.seq < b.seq; });

        const fs::path out_path = seg_dir / ("L1_" + std::to_string(state.next_segment_id++) + ".log");
        std::ofstream out(out_path);
        for (const auto& r : merged) {
            out << r.seq << ' ' << r.id << ' ' << (r.deleted ? 1 : 0) << '\t' << r.attrs_blob << '\n';
        }
        out.flush();
        const std::uint64_t bytes_out = static_cast<std::uint64_t>(fs::file_size(out_path, ec));

        for (std::size_t i = start; i < end; ++i) {
            const auto& seg = segs[i];
            state.segment_meta_by_path.erase(seg.string());
            fs::remove(seg, ec);
        }
        state.segment_meta_by_path[out_path.string()] = meta_from_rows(merged);
        segs = list_segment_logs(seg_dir);

        if (hooks && hooks->on_compaction) hooks->on_compaction();
        if (hooks && hooks->on_compaction_bytes) hooks->on_compaction_bytes(bytes_in, bytes_out);
        state.version++;
        if (opt.compaction_policy == CompactionPolicy::LeveledLite &&
            opt.leveled_l1_hard_segments > 0 &&
            segs.size() <= static_cast<std::size_t>(opt.leveled_l1_soft_segments)) {
            break;
        }
    }

    if (hooks && hooks->on_compaction_queue_depth) hooks->on_compaction_queue_depth(0, 0);
    refresh_segment_count(hooks, seg_dir);
}

void flush_memtable_locked(const Options& opt,
                           const std::string& data_file,
                           State& state,
                           const fs::path& seg_dir,
                           Hooks* hooks) {
    if (state.memtable.empty()) return;
    std::error_code ec;
    fs::create_directories(seg_dir, ec);
    const fs::path out_path = seg_dir / ("L0_" + std::to_string(state.next_segment_id++) + ".log");
    std::ofstream out(out_path);
    for (const auto& r : state.memtable) {
        out << r.seq << ' ' << r.id << ' ' << (r.deleted ? 1 : 0) << '\t' << r.attrs_blob << '\n';
    }
    out.flush();
    state.segment_meta_by_path[out_path.string()] = meta_from_rows(state.memtable);

    state.memtable.clear();
    state.mem_bytes = 0;
    state.version++;
    hook_memtable_bytes(hooks, 0);
    if (hooks && hooks->on_memtable_flush) hooks->on_memtable_flush();
    refresh_segment_count(hooks, seg_dir);
    maybe_compact_locked(opt, data_file, state, seg_dir, hooks);
}
} // namespace

void record_writes(const Options& opt,
                   const std::string& data_file,
                   const std::vector<newdb::Row>& rows,
                   const bool deleted_flag,
                   const TxnContext* txn,
                   Hooks* hooks) {
    if (!opt.enabled || data_file.empty() || rows.empty()) return;
    const fs::path seg_dir = fs::path(data_file + ".lsm");
    std::lock_guard<std::mutex> lk(g_mu);
    State& state = g_states[data_file];
    state.last_opt = opt;
    if (hooks) state.last_hooks = *hooks;
    if (txn && txn->in_txn && txn->txn_id > 0) {
        auto& txn_map = state.txn_rows_by_txn[txn->txn_id];
        for (const auto& row : rows) {
            SegmentRow sr;
            sr.seq = state.next_seq++;
            sr.id = row.id;
            sr.deleted = deleted_flag;
            sr.attrs_blob = row_to_blob(row);
            txn_map[row.id] = std::move(sr);
        }
        return;
    }
    for (const auto& row : rows) {
        SegmentRow sr;
        sr.seq = state.next_seq++;
        sr.id = row.id;
        sr.deleted = deleted_flag;
        sr.attrs_blob = row_to_blob(row);
        state.mem_bytes += static_cast<std::uint64_t>(sr.attrs_blob.size() + 32);
        state.memtable.push_back(std::move(sr));
    }
    hook_memtable_bytes(hooks, state.mem_bytes);
    if (state.mem_bytes >= effective_segment_target_bytes(opt)) {
        flush_memtable_locked(opt, data_file, state, seg_dir, hooks);
    }
}

void on_txn_commit(const Options& opt,
                   const std::string& data_file,
                   const TxnContext& txn,
                   Hooks* hooks) {
    if (!opt.enabled || !txn.in_txn || txn.txn_id <= 0 || data_file.empty()) return;
    const fs::path seg_dir = fs::path(data_file + ".lsm");
    std::lock_guard<std::mutex> lk(g_mu);
    auto it_state = g_states.find(data_file);
    if (it_state == g_states.end()) return;
    State& state = it_state->second;
    state.last_opt = opt;
    if (hooks) state.last_hooks = *hooks;
    auto it_txn = state.txn_rows_by_txn.find(txn.txn_id);
    if (it_txn == state.txn_rows_by_txn.end()) return;

    for (auto& kv : it_txn->second) {
        SegmentRow sr = std::move(kv.second);
        state.mem_bytes += static_cast<std::uint64_t>(sr.attrs_blob.size() + 32);
        state.memtable.push_back(std::move(sr));
    }
    state.txn_rows_by_txn.erase(it_txn);
    hook_memtable_bytes(hooks, state.mem_bytes);
    if (state.mem_bytes >= effective_segment_target_bytes(opt)) {
        flush_memtable_locked(opt, data_file, state, seg_dir, hooks);
    }
}

void on_txn_rollback(const Options& opt,
                     const std::string& data_file,
                     const TxnContext& txn,
                     Hooks* hooks) {
    if (!opt.enabled || !txn.in_txn || txn.txn_id <= 0 || data_file.empty()) return;
    std::lock_guard<std::mutex> lk(g_mu);
    auto it_state = g_states.find(data_file);
    if (it_state == g_states.end()) return;
    State& state = it_state->second;
    state.last_opt = opt;
    if (hooks) state.last_hooks = *hooks;
    state.txn_rows_by_txn.erase(txn.txn_id);
}

void clear_txn_views_for_data_file(const std::string& data_file) {
    std::lock_guard<std::mutex> lk(g_mu);
    auto it_state = g_states.find(data_file);
    if (it_state == g_states.end()) return;
    it_state->second.txn_rows_by_txn.clear();
}

bool drain_compaction(const std::string& data_file, const std::uint64_t timeout_ms) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    std::unique_lock<std::mutex> lk(g_compact_mu);
    return g_compact_cv.wait_until(lk, deadline, [&]() {
        std::lock_guard<std::mutex> lg(g_mu);
        auto it = g_states.find(data_file);
        if (it == g_states.end()) return true;
        return it->second.compaction_pending == 0 && it->second.compaction_inflight == 0;
    });
}

void shutdown_background_workers() {
    {
        std::lock_guard<std::mutex> lk(g_compact_mu);
        g_compact_shutdown = true;
    }
    g_compact_cv.notify_all();
    for (auto& th : g_compact_threads) {
        if (th.joinable()) th.join();
    }
    g_compact_threads.clear();
    g_compact_worker_count = 0;
    {
        std::lock_guard<std::mutex> lk(g_compact_mu);
        g_compact_q.clear();
        g_compact_shutdown = false;
    }
}

std::optional<FindResult> find_by_id(const Options& opt,
                                     const std::string& data_file,
                                     const int id,
                                     const TxnContext* txn,
                                     Hooks* hooks) {
    if (!opt.enabled || data_file.empty()) return std::nullopt;
    const fs::path seg_dir = fs::path(data_file + ".lsm");
    std::lock_guard<std::mutex> lk(g_mu);
    auto it_state = g_states.find(data_file);
    if (it_state == g_states.end()) return std::nullopt;
    State& state = it_state->second;
    state.last_opt = opt;
    if (hooks) state.last_hooks = *hooks;

    if (txn && txn->in_txn && txn->txn_id > 0) {
        auto it_txn = state.txn_rows_by_txn.find(txn->txn_id);
        if (it_txn != state.txn_rows_by_txn.end()) {
            auto it_row = it_txn->second.find(id);
            if (it_row != it_txn->second.end()) {
                FindResult out;
                out.found = true;
                out.deleted = it_row->second.deleted;
                if (!out.deleted) out.row = blob_to_row(it_row->second);
                return out;
            }
        }
    }

    auto it_cache = state.find_cache.find(id);
    if (it_cache != state.find_cache.end() && it_cache->second.version == state.version) {
        if (hooks && hooks->on_cache_lookup) hooks->on_cache_lookup(true);
        if (!it_cache->second.row.has_value()) return std::nullopt;
        FindResult out;
        out.found = true;
        out.deleted = it_cache->second.row->deleted;
        if (!out.deleted) out.row = blob_to_row(*it_cache->second.row);
        return out;
    }
    if (hooks && hooks->on_cache_lookup) hooks->on_cache_lookup(false);

    auto segs = list_segment_logs(seg_dir);
    std::uint64_t scanned = 0;
    for (auto it = segs.rbegin(); it != segs.rend(); ++it) {
        const std::string seg_path = it->string();
        auto meta_it = state.segment_meta_by_path.find(seg_path);
        if (meta_it == state.segment_meta_by_path.end()) {
            state.segment_meta_by_path[seg_path] = meta_from_file(*it);
            meta_it = state.segment_meta_by_path.find(seg_path);
        }
        if (meta_it != state.segment_meta_by_path.end() && meta_it->second.initialized) {
            if (id < meta_it->second.min_id || id > meta_it->second.max_id) {
                continue; // definite-miss filter
            }
        }

        ++scanned;
        std::ifstream in(*it);
        std::string line;
        SegmentRow best;
        bool found = false;
        while (std::getline(in, line)) {
            if (line.empty()) continue;
            SegmentRow r;
            if (!parse_segment_row_line(line, r)) continue;
            if (r.id != id) continue;
            if (!found || best.seq < r.seq) {
                best = std::move(r);
                found = true;
            }
        }
        if (found) {
            if (hooks && hooks->on_read_segments_scanned) hooks->on_read_segments_scanned(scanned);
            state.find_cache[id] = CacheEntry{state.version, best};
            FindResult out;
            out.found = true;
            out.deleted = best.deleted;
            if (!out.deleted) out.row = blob_to_row(best);
            return out;
        }
    }
    if (hooks && hooks->on_read_segments_scanned) hooks->on_read_segments_scanned(scanned);
    state.find_cache[id] = CacheEntry{state.version, std::nullopt};
    return std::nullopt;
}

} // namespace newdb::lsm_lite

