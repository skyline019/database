#include "where.h"
#include "equality_index_sidecar.h"
#include "visibility_checkpoint_sidecar.h"

#include <algorithm>
#include <atomic>
#include <charconv>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <limits>
#include <mutex>
#include <newdb/row.h>
#include <numeric>
#include <optional>
#include <sstream>
#include <unordered_set>
#include <unordered_map>
#include <list>

namespace {

struct PreparedCond {
    WhereCond cond;
    newdb::AttrType attr_type{newdb::AttrType::String};
    bool rhs_int_ready{false};
    long long rhs_int{0};
    bool attr_idx_ready{false};
    std::size_t attr_idx{0};
};

WhereQueryContext& default_where_context() {
    static WhereQueryContext ctx;
    return ctx;
}

struct QueryTraceGuard {
    const char* mode{"compute"};
    std::size_t rows{0};
    std::size_t logical_rows{0};
    std::size_t cond_count{0};
    std::chrono::steady_clock::time_point start{std::chrono::steady_clock::now()};
    bool enabled{false};
    std::size_t or_union_size{0};
    std::size_t or_verified_size{0};
    WhereQueryContext* context{nullptr};

    QueryTraceGuard(const std::size_t n, const std::size_t conds, WhereQueryContext* ctx)
        : logical_rows(n), cond_count(conds), context(ctx) {
        const char* v = std::getenv("NEWDB_QUERY_TRACE");
        enabled = (v != nullptr && std::string(v) == "1");
    }

    ~QueryTraceGuard() {
        if (enabled) {
            const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start).count();
            std::fprintf(stderr,
                         "[QUERY_TRACE] mode=%s conds=%zu rows=%zu logical=%zu or_union=%zu or_verified=%zu cache_hits=%llu cache_lookups=%llu elapsed_us=%lld\n",
                         mode,
                         cond_count,
                         rows,
                         logical_rows,
                         or_union_size,
                         or_verified_size,
                         static_cast<unsigned long long>(context ? context->cache_hits.load(std::memory_order_relaxed) : 0),
                         static_cast<unsigned long long>(context ? context->cache_lookups.load(std::memory_order_relaxed) : 0),
                         static_cast<long long>(elapsed));
        }
        const char* w = std::getenv("NEWDB_WHERE_WARN_HEAVY");
        if (w) {
            std::string s(w);
            for (auto& c : s) {
                c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
            }
            if (s == "1" || s == "on" || s == "true" || s == "yes") {
                std::size_t min_n = 10000;
                if (const char* mn = std::getenv("NEWDB_WHERE_WARN_HEAVY_MIN_ROWS")) {
                    std::size_t v = 0;
                    if (std::from_chars(mn, mn + std::strlen(mn), v).ec == std::errc{} && v > 0) {
                        min_n = v;
                    }
                }
                if (logical_rows >= min_n &&
                    (std::strcmp(mode, "and_ordered_scan") == 0 || std::strcmp(mode, "fallback_scan") == 0 ||
                     std::strcmp(mode, "full_scan_all") == 0)) {
                    std::fprintf(
                        stderr,
                        "[WHERE_WARN] plan=%s logical_rows=%zu: multi-condition or non-indexed path may full-scan the "
                        "heap. Prefer: narrow with indexed/equality (eq sidecar) conditions first, then apply the rest; "
                        "for stable sort use PAGE; split huge tables (time / key range). "
                        "See WRITE_PATH_TUNING_RUNBOOK.md \"Big data and WHERE\".\n",
                        mode,
                        logical_rows);
                }
            }
        }
    }
};

std::string build_conds_signature(const std::vector<WhereCond>& conds) {
    std::ostringstream oss;
    for (std::size_t i = 0; i < conds.size(); ++i) {
        const WhereCond& c = conds[i];
        if (i > 0) {
            oss << "|";
        }
        oss << c.logic_with_prev << "#"
            << c.attr << "#"
            << static_cast<int>(c.op) << "#"
            << c.value;
    }
    return oss.str();
}

std::string build_query_cache_key(const newdb::HeapTable& tbl,
                                  const newdb::TableSchema& schema,
                                  const std::vector<WhereCond>& conds) {
    std::uint64_t id_fp = static_cast<std::uint64_t>(tbl.index_by_id.size());
    std::size_t sample = 0;
    for (const auto& kv : tbl.index_by_id) {
        id_fp ^= (static_cast<std::uint64_t>(kv.first) << (sample % 13));
        id_fp ^= (static_cast<std::uint64_t>(kv.second) << ((sample + 3) % 17));
        if (++sample >= 8) {
            break;
        }
    }
    std::ostringstream oss;
    oss << tbl.name << "@"
        << schema.primary_key << "@"
        << tbl.logical_row_count() << "@"
        << id_fp << "@"
        << build_conds_signature(conds);
    return oss.str();
}

bool query_cache_get(WhereQueryContext& ctx, const std::string& key, std::vector<std::size_t>& out) {
    ctx.cache_lookups.fetch_add(1, std::memory_order_relaxed);
    std::lock_guard<std::mutex> lk(ctx.mu);
    const auto it = ctx.query_cache.find(key);
    if (it == ctx.query_cache.end()) {
        return false;
    }
    ctx.cache_hits.fetch_add(1, std::memory_order_relaxed);
    const auto pos_it = ctx.query_cache_lru_pos.find(key);
    if (pos_it != ctx.query_cache_lru_pos.end()) {
        ctx.query_cache_lru.splice(ctx.query_cache_lru.end(), ctx.query_cache_lru, pos_it->second);
        pos_it->second = std::prev(ctx.query_cache_lru.end());
    }
    out = it->second;
    return true;
}

void query_cache_put(WhereQueryContext& ctx, const std::string& key, std::vector<std::size_t> slots) {
    std::lock_guard<std::mutex> lk(ctx.mu);
    const auto cache_it = ctx.query_cache.find(key);
    if (cache_it == ctx.query_cache.end()) {
        ctx.query_cache_lru.push_back(key);
        ctx.query_cache_lru_pos[key] = std::prev(ctx.query_cache_lru.end());
    } else {
        const auto pos_it = ctx.query_cache_lru_pos.find(key);
        if (pos_it != ctx.query_cache_lru_pos.end()) {
            ctx.query_cache_lru.splice(ctx.query_cache_lru.end(), ctx.query_cache_lru, pos_it->second);
            pos_it->second = std::prev(ctx.query_cache_lru.end());
        }
    }
    ctx.query_cache[key] = std::move(slots);
    while (ctx.query_cache_lru.size() > WhereQueryContext::kMaxQueryCacheEntries) {
        const std::string old = ctx.query_cache_lru.front();
        ctx.query_cache_lru.pop_front();
        ctx.query_cache.erase(old);
        ctx.query_cache_lru_pos.erase(old);
    }
}

bool row_at_logical_slot(const newdb::HeapTable& tbl, const std::size_t slot, newdb::Row& out) {
    if (tbl.is_heap_storage_backed()) {
        return tbl.decode_heap_slot(slot, out);
    }
    out = tbl.rows[slot];
    return true;
}

std::vector<std::size_t> visible_slots_for_query(const newdb::HeapTable& tbl,
                                                 const newdb::TableSchema& schema,
                                                 const std::size_t n) {
    if (tbl.name.empty()) {
        std::vector<std::size_t> all(n);
        std::iota(all.begin(), all.end(), 0);
        return all;
    }
    std::vector<std::size_t> slots =
        load_or_build_visibility_checkpoint_sidecar(tbl.name + ".bin", schema, tbl);
    return slots;
}

std::string where_policy_mode() {
    const char* env = std::getenv("NEWDB_WHERE_POLICY_MODE");
    if (env == nullptr) {
        return "ratelimit";
    }
    std::string v(env);
    for (char& c : v) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    if (v == "off" || v == "warn" || v == "ratelimit" || v == "reject") {
        return v;
    }
    return "ratelimit";
}

std::size_t where_policy_min_rows() {
    std::size_t min_rows = 5000;
    if (const char* env = std::getenv("NEWDB_WHERE_POLICY_MIN_ROWS")) {
        std::size_t v = 0;
        if (std::from_chars(env, env + std::strlen(env), v).ec == std::errc{} && v > 0) {
            min_rows = v;
        }
    }
    return min_rows;
}

std::size_t where_policy_qps_limit(const std::size_t logical_rows, const std::size_t cond_count) {
    std::size_t qps = logical_rows >= 1000000 ? 1 : 2;
    if (cond_count >= 4) {
        qps = std::min<std::size_t>(qps, 1);
    }
    if (const char* env = std::getenv("NEWDB_WHERE_RATE_LIMIT_QPS")) {
        std::size_t v = 0;
        if (std::from_chars(env, env + std::strlen(env), v).ec == std::errc{}) {
            qps = v;
        }
    }
    return qps;
}

std::size_t where_policy_scan_cap_rows(const std::size_t logical_rows,
                                       const std::size_t cond_count,
                                       const bool has_or) {
    long double ratio = 0.85L;
    if (logical_rows >= 1000000) {
        ratio = has_or ? 0.45L : 0.60L;
    } else if (logical_rows >= 300000) {
        ratio = has_or ? 0.55L : 0.70L;
    } else if (logical_rows >= 100000) {
        ratio = has_or ? 0.65L : 0.78L;
    }
    if (cond_count >= 4) {
        ratio *= 0.9L;
    }
    if (const char* env = std::getenv("NEWDB_WHERE_SCAN_CAP_RATIO_PCT")) {
        std::size_t pct = 0;
        if (std::from_chars(env, env + std::strlen(env), pct).ec == std::errc{} && pct > 0 && pct <= 100) {
            ratio = static_cast<long double>(pct) / 100.0L;
        }
    }
    const auto cap = static_cast<std::size_t>(static_cast<long double>(logical_rows) * ratio);
    return std::max<std::size_t>(1, cap);
}

void where_policy_set(WhereQueryContext& ctx, const bool blocked, const std::string& msg) {
    std::lock_guard<std::mutex> lk(ctx.mu);
    ctx.policy.blocked = blocked;
    ctx.policy.message = msg;
}

bool where_policy_gate(const char* plan,
                       const std::size_t logical_rows,
                       const std::size_t cond_count,
                       const std::size_t estimated_scan_rows,
                       const bool has_or,
                       WhereQueryContext& ctx) {
    ctx.policy_checks.fetch_add(1, std::memory_order_relaxed);
    ctx.estimated_scan_rows_total.fetch_add(static_cast<std::uint64_t>(estimated_scan_rows), std::memory_order_relaxed);
    ctx.estimated_scan_rows_samples.fetch_add(1, std::memory_order_relaxed);
    const std::string mode = where_policy_mode();
    if (mode == "off" || logical_rows < where_policy_min_rows()) {
        where_policy_set(ctx, false, "");
        return true;
    }
    std::ostringstream oss;
    oss << "plan=" << plan << " logical_rows=" << logical_rows << " conds=" << cond_count
        << " heavy path detected; prefer index-friendly equality narrowing first";
    if (mode == "warn") {
        std::fprintf(stderr, "[WHERE_POLICY] warn %s\n", oss.str().c_str());
        where_policy_set(ctx, false, "");
        return true;
    }
    if (mode == "reject") {
        ctx.policy_rejects.fetch_add(1, std::memory_order_relaxed);
        where_policy_set(ctx, true, oss.str());
        return false;
    }
    const std::size_t scan_cap = where_policy_scan_cap_rows(logical_rows, cond_count, has_or);
    if (estimated_scan_rows >= scan_cap) {
        ctx.policy_rejects.fetch_add(1, std::memory_order_relaxed);
        std::fprintf(stderr,
                     "[WHERE_POLICY] reject plan=%s logical_rows=%zu conds=%zu has_or=%d est_scan_rows=%zu scan_cap=%zu reason=estimated_scan_too_wide\n",
                     plan,
                     logical_rows,
                     cond_count,
                     has_or ? 1 : 0,
                     estimated_scan_rows,
                     scan_cap);
        where_policy_set(ctx, true, oss.str() + " (estimated scan too wide)");
        return false;
    }
    const std::size_t qps = where_policy_qps_limit(logical_rows, cond_count);
    if (qps == 0) {
        where_policy_set(ctx, false, "");
        return true;
    }
    const auto now_sec = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
    {
        std::lock_guard<std::mutex> lk(ctx.mu);
        if (now_sec != ctx.policy.window_sec) {
            ctx.policy.window_sec = now_sec;
            ctx.policy.window_count = 0;
        }
        ++ctx.policy.window_count;
        if (ctx.policy.window_count > qps) {
            ctx.policy_rejects.fetch_add(1, std::memory_order_relaxed);
            std::fprintf(stderr,
                         "[WHERE_POLICY] reject plan=%s logical_rows=%zu conds=%zu has_or=%d est_scan_rows=%zu qps=%zu reason=rate_limited\n",
                         plan,
                         logical_rows,
                         cond_count,
                         has_or ? 1 : 0,
                         estimated_scan_rows,
                         qps);
            ctx.policy.blocked = true;
            ctx.policy.message = oss.str() + " (rate-limited)";
            return false;
        }
        ctx.policy.blocked = false;
        ctx.policy.message.clear();
    }
    return true;
}

bool collect_single_condition_candidates(const newdb::HeapTable& tbl,
                                         const newdb::TableSchema& schema,
                                         const WhereCond& c,
                                         const std::size_t n,
                                         std::vector<std::size_t>& out_slots,
                                         const char*& trace_mode) {
    if (c.attr == "id") {
        try {
            const int target_id = std::stoi(c.value);
            const auto it = tbl.index_by_id.find(target_id);
            if (it != tbl.index_by_id.end()) {
                if (c.op == CondOp::Eq) {
                    out_slots.push_back(it->second);
                } else {
                    newdb::Row r;
                    if (row_at_logical_slot(tbl, it->second, r) &&
                        row_match_condition(r, schema, c.attr, c.op, c.value)) {
                        out_slots.push_back(it->second);
                    }
                }
            }
            trace_mode = "id_lookup";
            return true;
        } catch (...) {
            return false;
        }
    }

    const std::string& pk = schema.primary_key;
    if (c.attr == pk && !tbl.index_by_pk_value.empty()) {
        const auto it = tbl.index_by_pk_value.find(c.value);
        if (it != tbl.index_by_pk_value.end()) {
            if (c.op == CondOp::Eq) {
                out_slots.push_back(it->second);
            } else {
                newdb::Row r;
                if (row_at_logical_slot(tbl, it->second, r) &&
                    row_match_condition(r, schema, c.attr, c.op, c.value)) {
                    out_slots.push_back(it->second);
                }
            }
        }
        trace_mode = "pk_lookup";
        return true;
    }

    if (c.op == CondOp::Eq && c.attr != "id" && c.attr != pk) {
        const EqLookupResult eq_lookup = lookup_or_build_eq_index_sidecar(
            EqIndexRequest{
                .data_file = tbl.name + ".bin",
                .attr_name = c.attr,
            },
            schema,
            tbl,
            c.value);
        if (eq_lookup.used_index) {
            out_slots.reserve(eq_lookup.slots.size());
            for (const std::size_t slot : eq_lookup.slots) {
                if (slot < n) {
                    out_slots.push_back(slot);
                }
            }
            trace_mode = "eq_sidecar";
            return true;
        }
    }
    return false;
}

void maybe_prewarm_eq_sidecars(const newdb::HeapTable& tbl,
                               const newdb::TableSchema& schema,
                               const std::vector<WhereCond>& conds,
                               const std::size_t logical_rows,
                               WhereQueryContext& ctx) {
    if (tbl.name.empty() || logical_rows < 100000) {
        return;
    }
    for (const WhereCond& c : conds) {
        if (c.op != CondOp::Eq || c.attr == "id" || c.attr == schema.primary_key) {
            continue;
        }
        const std::string key = tbl.name + "|" + c.attr;
        {
            std::lock_guard<std::mutex> lk(ctx.mu);
            if (ctx.eq_sidecar_prewarmed.find(key) != ctx.eq_sidecar_prewarmed.end()) {
                continue;
            }
        }
        (void)lookup_or_build_eq_index_sidecar(
            EqIndexRequest{
                .data_file = tbl.name + ".bin",
                .attr_name = c.attr,
            },
            schema,
            tbl,
            c.value);
        std::lock_guard<std::mutex> lk(ctx.mu);
        ctx.eq_sidecar_prewarmed.insert(std::move(key));
    }
}

bool is_single_cond_index_friendly(const WhereCond& c, const newdb::TableSchema& schema) {
    if (c.attr == "id") {
        return true;
    }
    if (c.attr == schema.primary_key) {
        return true;
    }
    return c.op == CondOp::Eq;
}

std::size_t estimated_cond_cost(const WhereCond& c, const newdb::TableSchema& schema) {
    // Lower means better seed candidate for AND chains.
    if (c.attr == "id" && c.op == CondOp::Eq) return 0;
    if (c.attr == schema.primary_key && c.op == CondOp::Eq) return 1;
    if (c.op == CondOp::Eq) return 2;
    if (c.op == CondOp::Ge || c.op == CondOp::Gt || c.op == CondOp::Le || c.op == CondOp::Lt) return 3;
    if (c.op == CondOp::Ne) return 4;
    return 5;
}

bool collect_single_condition_candidates_sanitized(const newdb::HeapTable& tbl,
                                                   const newdb::TableSchema& schema,
                                                   const WhereCond& c,
                                                   const std::size_t n,
                                                   std::vector<std::size_t>& out_slots) {
    std::vector<std::size_t> raw_slots;
    const char* mode = "";
    if (!collect_single_condition_candidates(tbl, schema, c, n, raw_slots, mode)) {
        return false;
    }
    out_slots.clear();
    out_slots.reserve(raw_slots.size());
    std::unordered_set<std::size_t> seen;
    seen.reserve(raw_slots.size());
    for (const std::size_t slot : raw_slots) {
        if (slot < n && seen.insert(slot).second) {
            out_slots.push_back(slot);
        }
    }
    return true;
}

bool all_and_chain(const std::vector<WhereCond>& conds) {
    for (std::size_t i = 1; i < conds.size(); ++i) {
        if (conds[i].logic_with_prev != "AND") {
            return false;
        }
    }
    return true;
}

bool all_or_chain(const std::vector<WhereCond>& conds) {
    for (std::size_t i = 1; i < conds.size(); ++i) {
        if (conds[i].logic_with_prev != "OR") {
            return false;
        }
    }
    return true;
}

bool row_match_all_conditions_ordered(const newdb::Row& r,
                                      const newdb::TableSchema& schema,
                                      const std::vector<WhereCond>& conds,
                                      const std::vector<std::size_t>& order,
                                      const std::size_t skip_idx) {
    for (const std::size_t idx : order) {
        if (idx == skip_idx) {
            continue;
        }
        const WhereCond& c = conds[idx];
        if (!row_match_condition(r, schema, c.attr, c.op, c.value)) {
            return false;
        }
    }
    return true;
}

bool prepare_int_rhs(const std::string& value, long long& out) {
    const char* begin = value.data();
    const char* end = value.data() + value.size();
    const auto [ptr, ec] = std::from_chars(begin, end, out);
    return ec == std::errc{} && ptr == end;
}

std::vector<PreparedCond> prepare_conditions(const newdb::TableSchema& schema,
                                             const std::vector<WhereCond>& conds) {
    std::vector<PreparedCond> prepared;
    prepared.reserve(conds.size());
    for (const auto& c : conds) {
        PreparedCond pc;
        pc.cond = c;
        pc.attr_type = (c.attr == "id") ? newdb::AttrType::Int : schema.type_of(c.attr);
        if (pc.attr_type == newdb::AttrType::Int) {
            pc.rhs_int_ready = prepare_int_rhs(c.value, pc.rhs_int);
        }
        if (c.attr != "id") {
            for (std::size_t i = 0; i < schema.attrs.size(); ++i) {
                if (schema.attrs[i].name == c.attr) {
                    pc.attr_idx_ready = true;
                    pc.attr_idx = i;
                    break;
                }
            }
        }
        prepared.push_back(std::move(pc));
    }
    return prepared;
}

bool row_match_prepared_condition(const newdb::Row& r,
                                  const newdb::TableSchema& schema,
                                  const PreparedCond& pc) {
    if (pc.attr_type == newdb::AttrType::Int && pc.rhs_int_ready && pc.cond.op != CondOp::Contains) {
        long long lhs = 0;
        if (pc.cond.attr == "id") {
            lhs = static_cast<long long>(r.id);
        } else {
            bool found = false;
            if (pc.attr_idx_ready && pc.attr_idx < r.values.size()) {
                found = prepare_int_rhs(r.values[pc.attr_idx], lhs);
            }
            if (!found) {
                const auto it = r.attrs.find(pc.cond.attr);
                if (it == r.attrs.end()) {
                    return false;
                }
                if (!prepare_int_rhs(it->second, lhs)) {
                    return false;
                }
            }
        }
        switch (pc.cond.op) {
        case CondOp::Eq: return lhs == pc.rhs_int;
        case CondOp::Ne: return lhs != pc.rhs_int;
        case CondOp::Gt: return lhs > pc.rhs_int;
        case CondOp::Lt: return lhs < pc.rhs_int;
        case CondOp::Ge: return lhs >= pc.rhs_int;
        case CondOp::Le: return lhs <= pc.rhs_int;
        default: break;
        }
    }
    return row_match_condition(r, schema, pc.cond.attr, pc.cond.op, pc.cond.value);
}

bool row_match_all_conditions_ordered_prepared(const newdb::Row& r,
                                               const newdb::TableSchema& schema,
                                               const std::vector<PreparedCond>& prepared,
                                               const std::vector<std::size_t>& order,
                                               const std::size_t skip_idx) {
    for (const std::size_t idx : order) {
        if (idx == skip_idx) {
            continue;
        }
        if (!row_match_prepared_condition(r, schema, prepared[idx])) {
            return false;
        }
    }
    return true;
}

bool row_match_multi_conditions_prepared(const newdb::Row& r,
                                         const newdb::TableSchema& schema,
                                         const std::vector<PreparedCond>& prepared) {
    if (prepared.empty()) return true;
    bool result = row_match_prepared_condition(r, schema, prepared[0]);
    for (std::size_t i = 1; i < prepared.size(); ++i) {
        const bool cur = row_match_prepared_condition(r, schema, prepared[i]);
        if (prepared[i].cond.logic_with_prev == "AND") {
            result = result && cur;
        } else {
            result = result || cur;
        }
    }
    return result;
}

std::vector<std::size_t> sanitize_sort_slots(const std::vector<std::size_t>& in, const std::size_t n) {
    std::vector<std::size_t> out;
    out.reserve(in.size());
    for (const std::size_t slot : in) {
        if (slot < n) {
            out.push_back(slot);
        }
    }
    std::sort(out.begin(), out.end());
    out.erase(std::unique(out.begin(), out.end()), out.end());
    return out;
}

std::vector<std::size_t> intersect_sorted_slots(const std::vector<std::size_t>& a,
                                                const std::vector<std::size_t>& b) {
    std::vector<std::size_t> out;
    out.reserve(std::min(a.size(), b.size()));
    std::size_t i = 0;
    std::size_t j = 0;
    while (i < a.size() && j < b.size()) {
        if (a[i] == b[j]) {
            out.push_back(a[i]);
            ++i;
            ++j;
        } else if (a[i] < b[j]) {
            ++i;
        } else {
            ++j;
        }
    }
    return out;
}

} // namespace

bool row_match_condition(const newdb::Row& r,
                         const newdb::TableSchema& schema,
                         const std::string& attr,
                         const CondOp op,
                         const std::string& value) {
    std::string lhs;
    newdb::AttrType tp = newdb::AttrType::String;
    if (attr == "id") {
        lhs = std::to_string(r.id);
        tp = newdb::AttrType::Int;
    } else {
        const auto it = r.attrs.find(attr);
        if (it != r.attrs.end()) lhs = it->second;
        tp = schema.type_of(attr);
    }

    if (op == CondOp::Contains) {
        return lhs.find(value) != std::string::npos;
    }

    const int cmp = schema.compare_attr(attr, lhs, value);
    switch (op) {
    case CondOp::Eq: return cmp == 0;
    case CondOp::Ne: return cmp != 0;
    case CondOp::Gt: return cmp > 0;
    case CondOp::Lt: return cmp < 0;
    case CondOp::Ge: return cmp >= 0;
    case CondOp::Le: return cmp <= 0;
    default: break;
    }
    return false;
}

bool parse_where_args_to_conds(const std::vector<std::string>& args,
                               std::vector<WhereCond>& conds,
                               std::string& err_msg) {
    conds.clear();
    err_msg.clear();
    if (args.size() < 3 || ((args.size() - 3) % 4) != 0) {
        err_msg = "expect (attr, op, value, [AND|OR, attr, op, value] ...)";
        return false;
    }
    auto make_cond = [](const std::string& a,
                        const std::string& op_str,
                        const std::string& v,
                        const std::string& logic) -> std::optional<WhereCond> {
        const CondOp op = parse_cond_op(op_str);
        if (op == CondOp::Unknown) return std::nullopt;
        WhereCond c;
        c.attr = a;
        c.op = op;
        c.value = v;
        c.logic_with_prev = logic;
        return c;
    };

    {
        const auto c = make_cond(args[0], args[1], args[2], "");
        if (!c.has_value()) {
            err_msg = "unknown op '" + args[1] + "'";
            return false;
        }
        conds.push_back(*c);
    }

    for (std::size_t i = 3; i + 3 < args.size(); i += 4) {
        std::string logic = args[i];
        for (auto& ch : logic) {
            ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
        }
        if (logic != "AND" && logic != "OR") {
            err_msg = "unknown logical operator '" + args[i] + "', expect AND/OR";
            return false;
        }
        const auto c = make_cond(args[i + 1], args[i + 2], args[i + 3], logic);
        if (!c.has_value()) {
            err_msg = "unknown op '" + args[i + 2] + "'";
            return false;
        }
        conds.push_back(*c);
    }
    return !conds.empty();
}

bool row_match_multi_conditions(const newdb::Row& r,
                                const newdb::TableSchema& schema,
                                const std::vector<WhereCond>& conds) {
    if (conds.empty()) return true;
    bool result = row_match_condition(r, schema, conds[0].attr, conds[0].op, conds[0].value);
    for (std::size_t i = 1; i < conds.size(); ++i) {
        const bool cur = row_match_condition(r, schema, conds[i].attr, conds[i].op, conds[i].value);
        if (conds[i].logic_with_prev == "AND") {
            result = result && cur;
        } else {
            result = result || cur;
        }
    }
    return result;
}

bool parse_agg_args_with_optional_where(const std::vector<std::string>& args,
                                        std::string& target_attr,
                                        std::vector<WhereCond>& conds,
                                        std::string& err_msg) {
    target_attr.clear();
    conds.clear();
    err_msg.clear();
    if (args.empty()) {
        err_msg = "expect at least attribute name";
        return false;
    }
    target_attr = args[0];
    if (args.size() == 1) return true;
    if (args.size() < 5) {
        err_msg = "usage: FUNC(attr) or FUNC(attr, WHERE, attr1, op1, value [, AND|OR, attr2, op2, value] ...)";
        return false;
    }
    std::string kw = args[1];
    for (auto& ch : kw) {
        ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
    }
    if (kw != "WHERE") {
        err_msg = "second argument must be WHERE when specifying conditions";
        return false;
    }
    std::vector<std::string> where_args(args.begin() + 2, args.end());
    if (!parse_where_args_to_conds(where_args, conds, err_msg)) {
        return false;
    }
    return true;
}

std::vector<std::size_t> query_with_index(const newdb::HeapTable& tbl,
                                          const newdb::TableSchema& schema,
                                          const std::vector<WhereCond>& conds,
                                          WhereQueryContext* ctx_ptr) {
    std::vector<std::size_t> result;
    WhereQueryContext& ctx = (ctx_ptr != nullptr) ? *ctx_ptr : default_where_context();
    where_policy_set(ctx, false, "");

    const std::size_t n = tbl.logical_row_count();
    const bool has_or = std::any_of(conds.begin(), conds.end(), [](const WhereCond& c) {
        return c.logic_with_prev == "OR";
    });
    auto estimate_scan_rows = [&](const std::vector<WhereCond>& in) -> std::size_t {
        if (in.empty()) {
            return n;
        }
        long double est = static_cast<long double>(n);
        for (const auto& c : in) {
            long double factor = 1.0L;
            if (c.attr == "id" && c.op == CondOp::Eq) {
                factor = 0.0001L;
            } else if (c.op == CondOp::Eq) {
                factor = 0.08L;
            } else if (c.op == CondOp::Contains) {
                factor = 0.35L;
            } else {
                factor = 0.45L;
            }
            if (c.logic_with_prev == "OR") {
                est = std::min<long double>(static_cast<long double>(n), est + static_cast<long double>(n) * factor);
            } else {
                est *= factor;
            }
        }
        if (est < 1.0L) est = 1.0L;
        if (est > static_cast<long double>(n)) est = static_cast<long double>(n);
        return static_cast<std::size_t>(est);
    };
    QueryTraceGuard trace(n, conds.size(), &ctx);
    const std::string cache_key = build_query_cache_key(tbl, schema, conds);
    if (query_cache_get(ctx, cache_key, result)) {
        trace.mode = "cache_hit";
        trace.rows = result.size();
        return result;
    }

    if (conds.empty()) {
        result = visible_slots_for_query(tbl, schema, n);
        query_cache_put(ctx, cache_key, result);
        trace.mode = "full_scan_all";
        trace.rows = result.size();
        return result;
    }
    maybe_prewarm_eq_sidecars(tbl, schema, conds, n, ctx);

    if (conds.size() == 1) {
        const WhereCond& c = conds[0];
        const char* single_mode = "single_scan";
        if (collect_single_condition_candidates(tbl, schema, c, n, result, single_mode)) {
            query_cache_put(ctx, cache_key, result);
            trace.mode = single_mode;
            trace.rows = result.size();
            return result;
        }
    }
    const std::vector<PreparedCond> prepared_conds = prepare_conditions(schema, conds);

    // Batch optimization for multi-condition AND:
    // pick the most selective seed condition and filter remaining conditions on seed candidates.
    if (conds.size() > 1 && all_and_chain(conds)) {
        // Two-condition fast path: intersect indexed candidates directly.
        if (conds.size() == 2 &&
            is_single_cond_index_friendly(conds[0], schema) &&
            is_single_cond_index_friendly(conds[1], schema)) {
            std::vector<std::size_t> a_raw;
            std::vector<std::size_t> b_raw;
            const char* mode_a = "";
            const char* mode_b = "";
            const bool a_ok = collect_single_condition_candidates(tbl, schema, conds[0], n, a_raw, mode_a);
            const bool b_ok = collect_single_condition_candidates(tbl, schema, conds[1], n, b_raw, mode_b);
            if (!a_ok || !b_ok) {
                goto and_fast_path_fallback;
            }
            const std::vector<std::size_t> a = sanitize_sort_slots(a_raw, n);
            const std::vector<std::size_t> b = sanitize_sort_slots(b_raw, n);
            const std::vector<std::size_t> inter = intersect_sorted_slots(a, b);
            result.reserve(inter.size());
            newdb::Row r;
            for (const std::size_t slot : inter) {
                if (!row_at_logical_slot(tbl, slot, r)) {
                    continue;
                }
                if (row_match_multi_conditions_prepared(r, schema, prepared_conds)) {
                    result.push_back(slot);
                }
            }
            query_cache_put(ctx, cache_key, result);
            trace.mode = "and_intersect_2";
            trace.rows = result.size();
            return result;
        }
and_fast_path_fallback:

        std::size_t seed = 0;
        std::size_t best_cost = std::numeric_limits<std::size_t>::max();
        std::size_t best_card = std::numeric_limits<std::size_t>::max();
        std::vector<std::vector<std::size_t>> seed_candidates(conds.size());
        std::vector<bool> seed_candidates_ready(conds.size(), false);
        for (std::size_t i = 0; i < conds.size(); ++i) {
            if (!is_single_cond_index_friendly(conds[i], schema)) {
                continue;
            }
            const std::size_t cost = estimated_cond_cost(conds[i], schema);
            std::vector<std::size_t> candidates;
            if (!collect_single_condition_candidates_sanitized(tbl, schema, conds[i], n, candidates)) {
                continue;
            }
            const std::size_t card_value = candidates.size();
            seed_candidates[i] = std::move(candidates);
            seed_candidates_ready[i] = true;
            if (cost < best_cost || (cost == best_cost && card_value < best_card)) {
                best_cost = cost;
                best_card = card_value;
                seed = i;
            }
        }
        if (best_cost != std::numeric_limits<std::size_t>::max()) {
            std::vector<std::size_t> eval_order;
            eval_order.reserve(conds.size());
            for (std::size_t i = 0; i < conds.size(); ++i) {
                if (i != seed) {
                    eval_order.push_back(i);
                }
            }
            std::sort(eval_order.begin(), eval_order.end(), [&](const std::size_t a, const std::size_t b) {
                return estimated_cond_cost(conds[a], schema) < estimated_cond_cost(conds[b], schema);
            });

            if (!seed_candidates_ready[seed]) {
                query_cache_put(ctx, cache_key, result);
                trace.mode = "and_seed_filter";
                trace.rows = result.size();
                return result;
            }
            const std::vector<std::size_t>& seed_slots = seed_candidates[seed];
            result.reserve(seed_slots.size());
            newdb::Row r;
            for (const std::size_t slot : seed_slots) {
                if (slot >= n) {
                    continue;
                }
                if (!row_at_logical_slot(tbl, slot, r)) {
                    continue;
                }
                if (row_match_all_conditions_ordered_prepared(r, schema, prepared_conds, eval_order, seed)) {
                    result.push_back(slot);
                }
            }
            query_cache_put(ctx, cache_key, result);
            trace.mode = "and_seed_filter";
            trace.rows = result.size();
            return result;
        }

        // No index-friendly seed, still keep ordered short-circuit evaluation.
        std::vector<std::size_t> eval_order;
        eval_order.reserve(conds.size());
        for (std::size_t i = 0; i < conds.size(); ++i) {
            eval_order.push_back(i);
        }
        std::sort(eval_order.begin(), eval_order.end(), [&](const std::size_t a, const std::size_t b) {
            return estimated_cond_cost(conds[a], schema) < estimated_cond_cost(conds[b], schema);
        });

        if (!where_policy_gate("and_ordered_scan", n, conds.size(), estimate_scan_rows(conds), has_or, ctx)) {
            trace.mode = "policy_block";
            trace.rows = 0;
            return {};
        }
        const std::vector<std::size_t> visible_slots = visible_slots_for_query(tbl, schema, n);
        result.reserve(visible_slots.size());
        newdb::Row r;
        for (const std::size_t i : visible_slots) {
            if (!row_at_logical_slot(tbl, i, r)) {
                continue;
            }
            if (row_match_all_conditions_ordered_prepared(r, schema, prepared_conds, eval_order, std::numeric_limits<std::size_t>::max())) {
                result.push_back(i);
            }
        }
        query_cache_put(ctx, cache_key, result);
        trace.mode = "and_ordered_scan";
        trace.rows = result.size();
        return result;
    }

    // Batch optimization for pure OR chain:
    // union indexed single-condition candidates first, then verify with full OR expression.
    if (conds.size() > 1 && all_or_chain(conds)) {
        bool all_indexable = true;
        for (const auto& c : conds) {
            if (!is_single_cond_index_friendly(c, schema)) {
                all_indexable = false;
                break;
            }
        }
        if (all_indexable) {
            std::unordered_set<std::size_t> union_slots;
            for (const auto& c : conds) {
                std::vector<WhereCond> one{c};
                std::vector<std::size_t> one_slots;
                const char* one_mode = "";
                if (!collect_single_condition_candidates(tbl, schema, c, n, one_slots, one_mode)) {
                    all_indexable = false;
                    break;
                }
                union_slots.insert(one_slots.begin(), one_slots.end());
            }
            if (!all_indexable) {
                goto or_chain_fallback;
            }
            trace.or_union_size = union_slots.size();
            std::vector<std::size_t> ordered_slots;
            ordered_slots.reserve(union_slots.size());
            for (const std::size_t slot : union_slots) {
                if (slot < n) {
                    ordered_slots.push_back(slot);
                }
            }
            std::sort(ordered_slots.begin(), ordered_slots.end());

            result.reserve(ordered_slots.size());
            newdb::Row r;
            for (const std::size_t slot : ordered_slots) {
                if (!row_at_logical_slot(tbl, slot, r)) {
                    continue;
                }
                if (row_match_multi_conditions_prepared(r, schema, prepared_conds)) {
                    result.push_back(slot);
                }
            }
            trace.or_verified_size = result.size();
            query_cache_put(ctx, cache_key, result);
            trace.mode = "or_union_verify";
            trace.rows = result.size();
            return result;
        }
    }
or_chain_fallback:

    if (!where_policy_gate("fallback_scan", n, conds.size(), estimate_scan_rows(conds), has_or, ctx)) {
        trace.mode = "policy_block";
        trace.rows = 0;
        return {};
    }
    ctx.fallback_scans.fetch_add(1, std::memory_order_relaxed);
    {
        const auto checks = ctx.policy_checks.load(std::memory_order_relaxed);
        const auto rejects = ctx.policy_rejects.load(std::memory_order_relaxed);
        const auto fallback = ctx.fallback_scans.load(std::memory_order_relaxed);
        const auto est_total = ctx.estimated_scan_rows_total.load(std::memory_order_relaxed);
        const auto est_samples = ctx.estimated_scan_rows_samples.load(std::memory_order_relaxed);
        const std::uint64_t est_avg = est_samples == 0 ? 0 : (est_total / est_samples);
        std::fprintf(stderr,
                     "[WHERE_POLICY] fallback plan=fallback_scan logical_rows=%zu checks=%llu rejects=%llu fallback_scans=%llu est_scan_rows_avg=%llu\n",
                     n,
                     static_cast<unsigned long long>(checks),
                     static_cast<unsigned long long>(rejects),
                     static_cast<unsigned long long>(fallback),
                     static_cast<unsigned long long>(est_avg));
    }
    const std::vector<std::size_t> visible_slots = visible_slots_for_query(tbl, schema, n);
    result.reserve(visible_slots.size());
    newdb::Row r;
    for (const std::size_t i : visible_slots) {
        if (!row_at_logical_slot(tbl, i, r)) {
            continue;
        }
        if (row_match_multi_conditions_prepared(r, schema, prepared_conds)) {
            result.push_back(i);
        }
    }
    query_cache_put(ctx, cache_key, result);
    trace.mode = "fallback_scan";
    trace.rows = result.size();
    return result;
}

std::vector<std::size_t> build_candidate_slots(const newdb::HeapTable& tbl,
                                               const newdb::TableSchema& schema,
                                               const std::vector<WhereCond>& conds,
                                               WhereQueryContext* ctx) {
    return query_with_index(tbl, schema, conds, ctx);
}

bool where_policy_last_blocked(const WhereQueryContext* ctx_ptr) {
    const WhereQueryContext& ctx = (ctx_ptr != nullptr) ? *ctx_ptr : default_where_context();
    std::lock_guard<std::mutex> lk(ctx.mu);
    return ctx.policy.blocked;
}

std::string where_policy_last_message(const WhereQueryContext* ctx_ptr) {
    const WhereQueryContext& ctx = (ctx_ptr != nullptr) ? *ctx_ptr : default_where_context();
    std::lock_guard<std::mutex> lk(ctx.mu);
    return ctx.policy.message;
}
