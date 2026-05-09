#include <waterfall/config.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <mutex>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include <newdb/page_io.h>
#include <newdb/heap_table.h>

#include "cli/modules/sidecar/covering/covering_index_sidecar.h"
#include "cli/shell/dispatch/shared/dispatch_internal.h"
#include "cli/modules/import_export/demo_export.h"
#include "cli/modules/import_export/import.h"
#include "cli/modules/common/logging/logging.h"
#include "cli/modules/sidecar/page/page_index_sidecar.h"
#include "cli/modules/catalog/schema_catalog.h"
#include "cli/shell/state/shell_state_heap_read_guard.h"
#include "cli/shell/state/shell_state_facade.h"
#include "cli/shell/dispatch/services/lsm/lsm_lite_service.h"
#include "cli/modules/common/view/table_view.h"
#include "cli/modules/txn/coordinator/txn_manager.h"
#include "cli/modules/common/util/utils.h"
#include "cli/modules/where/executor/where.h"
#include "cli/modules/where/executor/stats/table_stats.h"

namespace {

bool txn_isolation_readpath_enabled() {
    const char* opt = std::getenv("NEWDB_TXN_ISOLATION_READPATH");
    if (opt == nullptr || opt[0] == '\0') {
        return true;
    }
    std::string v = opt;
    for (auto& ch : v) {
        ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    }
    if (v == "0" || v == "off" || v == "false" || v == "no") {
        return false;
    }
    return true;
}

std::string json_string_escape(const std::string& s) {
    std::string o;
    o.reserve(s.size() + 8);
    for (const char c : s) {
        if (c == '\\' || c == '"') {
            o.push_back('\\');
        }
        o.push_back(c);
    }
    return o;
}

struct WhereQueryStatsHintScope {
    WhereQueryContext& ctx;
    const TableStats* prev;
    WhereQueryStatsHintScope(WhereQueryContext& c, const TableStats* hint)
        : ctx(c), prev(c.query_stats_hint) {
        ctx.query_stats_hint = hint;
    }
    ~WhereQueryStatsHintScope() { ctx.query_stats_hint = prev; }
};

}  // namespace

bool handle_query_page_command(ShellState& st,
                               const char* line,
                               const char* log_file,
                               const std::string& eff_data,
                               newdb::HeapTable& tbl) {
    ShellStateFacade f(st);
    if (strncasecmp_ascii(line, "PAGE", 4) != 0) {
        return false;
    }
    std::vector<std::string> args;
    if (!parse_comma_args(line + 4, args) || args.size() < 2 || args.size() > 5) {
        log_and_print(log_file,
                      "[PAGE] usage: PAGE(page, page_size, [order_key], [desc|asc], [after=<id>])\n"
                      "       order_key=id supports optional keyset cursor after=<int>\n");
        return true;
    }
    int page = 0;
    int page_size = 0;
    try {
        page = std::stoi(args[0]);
        page_size = std::stoi(args[1]);
    } catch (...) {
        log_and_print(log_file,
                      "[PAGE] page and page_size must be integers.\n");
        return true;
    }
    std::string order_key = "id";
    bool desc = false;
    std::optional<int> after_id;
    auto parse_after = [&](const std::string& s) -> bool {
        static constexpr char kPref[] = "after=";
        if (s.size() <= sizeof(kPref) - 1 || strncasecmp_ascii(s.c_str(), kPref, static_cast<int>(sizeof(kPref) - 1)) != 0) {
            return false;
        }
        try {
            after_id = std::stoi(s.substr(sizeof(kPref) - 1));
            return true;
        } catch (...) {
            return false;
        }
    };

    if (args.size() >= 3 && !args[2].empty()) {
        order_key = args[2];
    }
    if (args.size() == 3) {
        // page, size, order only
    } else if (args.size() == 4) {
        if (parse_after(args[3])) {
            // page, size, order, after=
        } else if (args[3].size() == 4 && strcasecmp_ascii(args[3].c_str(), "desc") == 0) {
            desc = true;
        }
    } else if (args.size() >= 5) {
        if (args[3].size() == 4 && strcasecmp_ascii(args[3].c_str(), "desc") == 0) {
            desc = true;
        }
        if (!parse_after(args[4])) {
            log_and_print(log_file, "[PAGE] expected after=<id> as last argument\n");
            return true;
        }
    }

    const bool use_keyset = after_id.has_value();
    if (use_keyset && order_key != "id") {
        log_and_print(log_file, "[PAGE] keyset after= is only supported for order_key=id\n");
        return true;
    }
    const HeapReadViewGuard _heap_read_view(st, tbl);
    std::vector<std::size_t> sorted_idx = load_or_build_page_index_sidecar(
        PageSidecarRequest{
            .data_file = eff_data,
            .table_name = f.table_name(),
            .order_key = order_key,
            .descending = desc,
        },
        f.schema(),
        tbl);
    if (use_keyset) {
        sorted_idx = table_view::page_indices_keyset_after_id(tbl, sorted_idx, desc, *after_id, true);
    }
    log_and_print(log_file,
                  "[PAGE] table=%s order_by=%s %s%s\n",
                  tbl.name.c_str(),
                  order_key.c_str(),
                  desc ? "desc" : "asc",
                  use_keyset ? (" after_id=" + std::to_string(*after_id)).c_str() : "");
    table_view::print_page_indexed(f.schema(),
                                   tbl,
                                   sorted_idx,
                                   static_cast<std::size_t>(page),
                                   static_cast<std::size_t>(page_size));
    return true;
}


bool handle_query_where_count_commands(ShellState& st,
                                       const char* line,
                                       const char* log_file,
                                       const std::string& eff_data,
                                       const std::string& current_table,
                                       newdb::HeapTable& tbl) {
    ShellStateFacade f(st);
    const HeapReadViewGuard _heap_read_view(st, tbl);
    TableStats where_stats_buf{};
    const TableStats* where_stats_hint = nullptr;
    bool loaded_table_stats_file = false;
    bool table_stats_file_stale = false;
    if (const char* raw = std::getenv("NEWDB_QUERY_USE_TABLE_STATS")) {
        if (raw[0] == '1' && raw[1] == '\0') {
            if (const char* pr = std::getenv("NEWDB_QUERY_PERSIST_TABLE_STATS")) {
                if (pr[0] == '1' && pr[1] == '\0') {
                    namespace fs = std::filesystem;
                    std::error_code ec;
                    const std::string sp = table_stats_file_path_for_data_file(eff_data);
                    if (fs::exists(sp, ec)) {
                        loaded_table_stats_file = load_table_stats_file(eff_data, f.schema(), &where_stats_buf);
                        if (!loaded_table_stats_file) {
                            table_stats_file_stale = true;
                        }
                    }
                }
            }
            if (!loaded_table_stats_file) {
                (void)build_table_stats_from_heap(tbl, f.schema(), &where_stats_buf);
                if (const char* pr = std::getenv("NEWDB_QUERY_PERSIST_TABLE_STATS")) {
                    if (pr[0] == '1' && pr[1] == '\0') {
                        (void)save_table_stats_file(eff_data, f.schema(), where_stats_buf);
                    }
                }
            }
            where_stats_hint = &where_stats_buf;
        }
    }
    const WhereQueryStatsHintScope _where_stats_hint(f.where(), where_stats_hint);
    // Bare COUNT hot path: heap read guard is active; skip SHOW PLAN / WHERE / COUNT(cond) branches.
    if (line != nullptr) {
        const char* p = line;
        while (*p == ' ' || *p == '\t') {
            ++p;
        }
        if (strcasecmp_ascii(p, "COUNT") == 0 && p[5] == '\0') {
            const std::size_t visible = tbl.logical_row_count();
            log_and_print(log_file,
                          "[COUNT] table='%s' rows=%zu decode_calls=%llu decode_hits=%llu decode_misses=%llu\n",
                          current_table.c_str(),
                          visible,
                          static_cast<unsigned long long>(tbl.decode_heap_slot_calls),
                          static_cast<unsigned long long>(tbl.decode_heap_slot_hits),
                          static_cast<unsigned long long>(tbl.decode_heap_slot_misses));
            return true;
        }
    }
    const bool show_plan_json = strncasecmp_ascii(line, "SHOW PLAN", 9) == 0;
    const bool explain_where = strncasecmp_ascii(line, "EXPLAIN WHERE", 14) == 0;
    if (show_plan_json || explain_where) {
        const char* tail = show_plan_json ? (line + 9) : (line + 14);
        std::vector<std::string> args;
        if (!parse_comma_args(tail, args) || args.empty()) {
            log_and_print(log_file,
                          show_plan_json
                              ? "[SHOW PLAN] usage: SHOW PLAN(attr, op, value [, AND|OR, attr, op, value] ...)\n"
                              : "[EXPLAIN WHERE] usage: EXPLAIN WHERE(attr, op, value [, AND|OR, attr, op, value] ...)\n");
            return true;
        }
        std::vector<WhereCond> conds;
        std::string err_msg;
        if (!parse_where_args_to_conds(args, conds, err_msg)) {
            log_and_print(log_file,
                          show_plan_json ? "[SHOW PLAN] invalid arguments: %s\n" : "[EXPLAIN WHERE] invalid arguments: %s\n",
                          err_msg.c_str());
            return true;
        }
        const HeapReadViewGuard _heap_read_view_plan(st, tbl);
        const TxnRuntimeStats plan_stats = f.txn().runtimeStats();
        const std::uint64_t fb0 = f.where().fallback_scans.load(std::memory_order_relaxed);
        const std::uint64_t eq0 = f.where().plan_eq_sidecar_count.load(std::memory_order_relaxed);
        const std::uint64_t id0 = f.where().plan_id_pk_count.load(std::memory_order_relaxed);
        const std::uint64_t pf0 = f.where().plan_fallback_count.load(std::memory_order_relaxed);
        const std::uint64_t sc0 = f.where().query_rows_scanned_total.load(std::memory_order_relaxed);
        const std::uint64_t rt0 = f.where().query_rows_returned_total.load(std::memory_order_relaxed);
        const std::size_t estimated_scan_rows =
            where_estimate_scan_rows(tbl, f.schema(), conds, &f.where());
        const std::vector<std::size_t> matched_idx =
            query_with_index(tbl, f.schema(), conds, &f.where());
        const std::uint32_t plan_candidates_considered =
            f.where().last_plan_candidates_considered.load(std::memory_order_relaxed);
        if (where_policy_last_blocked(&f.where())) {
            log_and_print(log_file,
                          show_plan_json ? "[SHOW PLAN] blocked: %s\n" : "[EXPLAIN WHERE] blocked: %s\n",
                          where_policy_last_message(&f.where()).c_str());
            return true;
        }
        const std::uint64_t fb1 = f.where().fallback_scans.load(std::memory_order_relaxed);
        const std::uint64_t eq1 = f.where().plan_eq_sidecar_count.load(std::memory_order_relaxed);
        const std::uint64_t id1 = f.where().plan_id_pk_count.load(std::memory_order_relaxed);
        const std::uint64_t pf1 = f.where().plan_fallback_count.load(std::memory_order_relaxed);
        const std::uint64_t sc1 = f.where().query_rows_scanned_total.load(std::memory_order_relaxed);
        const std::uint64_t rt1 = f.where().query_rows_returned_total.load(std::memory_order_relaxed);
        std::string plan_id;
        {
            std::lock_guard<std::mutex> lk(f.where().mu);
            plan_id = f.where().last_plan_id;
        }
        if (show_plan_json) {
            const std::size_t logical_rows = tbl.logical_row_count();
            const std::uint64_t snapshot_lsn =
                tbl.active_snapshot.has_value() ? tbl.active_snapshot->snapshot_lsn : 0;
            const char* readpath_json = txn_isolation_readpath_enabled() ? "true" : "false";
            const std::vector<PlanCandidate> plan_cands =
                where_build_plan_candidates(tbl, f.schema(), conds,
                                            WherePlanningStatsRef{where_stats_hint});
            const std::string plan_id_chosen = plan_id.empty() ? "?" : plan_id;
            std::string chosen_reason;
            for (const auto& pcn : plan_cands) {
                if (pcn.id == plan_id_chosen) {
                    chosen_reason = pcn.rationale;
                    break;
                }
            }
            std::ostringstream cand_json;
            cand_json << "[";
            for (std::size_t i = 0; i < plan_cands.size(); ++i) {
                if (i > 0) {
                    cand_json << ',';
                }
                const bool is_chosen = (plan_cands[i].id == plan_id_chosen);
                cand_json << "{\"id\":\"" << json_string_escape(plan_cands[i].id) << "\",\"estimated_cost\":"
                          << plan_cands[i].estimated_cost << ",\"cost\":{\"estimated_rows\":"
                          << plan_cands[i].cost.estimated_rows
                          << "},\"rationale\":\"" << json_string_escape(plan_cands[i].rationale) << "\"";
                if (is_chosen) {
                    cand_json << ",\"chosen\":true";
                } else if (plan_id_chosen != "?") {
                    cand_json << ",\"reason_rejected\":\"not_chosen\"";
                }
                cand_json << "}";
            }
            cand_json << "]";
            const std::string plan_id_esc = json_string_escape(plan_id_chosen);
            const std::string snap_esc = json_string_escape(plan_stats.last_snapshot_source);
            const std::string chosen_reason_esc = json_string_escape(chosen_reason);
            std::ostringstream json;
            json << "{\"plan_id\":\"" << plan_id_esc << "\",\"chosen_reason\":\"" << chosen_reason_esc
                 << "\",\"logical_rows\":" << logical_rows
                 << ",\"matched_rows\":" << matched_idx.size() << ",\"estimated_scan_rows\":" << estimated_scan_rows
                 << ",\"plan_candidates_considered\":"
                 << static_cast<unsigned>(plan_candidates_considered == 0u ? 1u : plan_candidates_considered)
                 << ",\"plan_candidates\":" << cand_json.str() << ",\"table_stats_stale\":"
                 << (table_stats_file_stale ? "true" : "false") << ",\"path\":\"where_executor\""
                 << ",\"snapshot_lsn\":" << static_cast<unsigned long long>(snapshot_lsn) << ",\"snapshot_source\":\""
                 << snap_esc << "\",\"readpath_enabled\":" << readpath_json << ",\"delta\":{\"fallback_scans\":"
                 << static_cast<unsigned long long>(fb1 - fb0) << ",\"eq_sidecar\":"
                 << static_cast<unsigned long long>(eq1 - eq0) << ",\"id_pk\":"
                 << static_cast<unsigned long long>(id1 - id0) << ",\"plan_fallback\":"
                 << static_cast<unsigned long long>(pf1 - pf0) << ",\"rows_scanned\":"
                 << static_cast<unsigned long long>(sc1 - sc0) << ",\"rows_returned\":"
                 << static_cast<unsigned long long>(rt1 - rt0) << "}}\n";
            log_and_print(log_file, "%s", json.str().c_str());
        } else {
            log_and_print(log_file,
                          "[EXPLAIN WHERE] plan_id=%s matched=%zu logical_rows=%zu delta{fallback_scans=%llu eq_sidecar=%llu "
                          "id_pk=%llu plan_fallback=%llu rows_scanned=%llu rows_returned=%llu}\n",
                          plan_id.empty() ? "?" : plan_id.c_str(),
                          matched_idx.size(),
                          tbl.logical_row_count(),
                          static_cast<unsigned long long>(fb1 - fb0),
                          static_cast<unsigned long long>(eq1 - eq0),
                          static_cast<unsigned long long>(id1 - id0),
                          static_cast<unsigned long long>(pf1 - pf0),
                          static_cast<unsigned long long>(sc1 - sc0),
                          static_cast<unsigned long long>(rt1 - rt0));
        }
        return true;
    }
    if (strncasecmp_ascii(line, "WHEREP", 6) == 0) {
        std::vector<std::string> args;
        if (!parse_comma_args(line + 6, args) || args.size() < 5) {
            log_and_print(log_file,
                          "[WHEREP] usage: WHEREP(proj_attr, WHERE, key_attr, =, key_value)\n");
            return true;
        }
        // WHEREP(proj_attr, WHERE, key_attr, =, key_value)
        if (args[1].empty() || strcasecmp_ascii(args[1].c_str(), "WHERE") != 0) {
            log_and_print(log_file,
                          "[WHEREP] usage: WHEREP(proj_attr, WHERE, key_attr, =, key_value)\n");
            return true;
        }
        const std::string proj_attr = args[0];
        const std::string key_attr = args[2];
        const std::string op = args[3];
        const std::string key_value = args[4];
        if (op != "=") {
            log_and_print(log_file, "[WHEREP] only '=' is supported.\n");
            return true;
        }
        if (key_attr == "id") {
            log_and_print(log_file, "[WHEREP] key_attr must be non-id equality (use FIND).\n");
            return true;
        }
        const std::size_t limit = 50;
        const auto rows = lookup_or_build_covering_proj_sidecar(
            eff_data, key_attr, proj_attr, key_value, limit, f.schema(), tbl);
        log_and_print(log_file,
                      "[WHEREP] key=%s=%s proj=%s rows=%zu (covering sidecar)\n",
                      key_attr.c_str(),
                      key_value.c_str(),
                      proj_attr.c_str(),
                      rows.size());
        for (const auto& r : rows) {
            log_and_print(log_file, "  id=%d %s=%s\n", r.id, proj_attr.c_str(), r.value.c_str());
        }
        return true;
    }

    if (strncasecmp_ascii(line, "WHERE", 5) == 0) {
        std::vector<std::string> args;
        if (!parse_comma_args(line + 5, args) || args.empty()) {
            log_and_print(log_file,
                          "[WHERE] usage: WHERE(attr, op, value [, AND|OR, attr, op, value] ...)\n");
            return true;
        }
        std::vector<WhereCond> conds;
        std::string err_msg;
        if (!parse_where_args_to_conds(args, conds, err_msg)) {
            log_and_print(log_file,
                          "[WHERE] invalid arguments: %s\n", err_msg.c_str());
            log_and_print(log_file,
                          "[WHERE] usage: WHERE(attr, op, value [, AND|OR, attr, op, value] ...)\n");
            return true;
        }
        const std::vector<std::size_t> matched_idx = query_with_index(tbl, f.schema(), conds, &f.where());
        if (where_policy_last_blocked(&f.where())) {
            log_and_print(log_file, "[WHERE] blocked: %s\n", where_policy_last_message(&f.where()).c_str());
            return true;
        }
        log_and_print(log_file,
                      "[WHERE] matched %zu / %zu rows\n",
                      matched_idx.size(),
                      tbl.logical_row_count(),
                      "");
        if (matched_idx.empty()) {
            return true;
        }
        const std::size_t page_size = std::min<std::size_t>(50, matched_idx.size());
        table_view::print_page_indexed(f.schema(), tbl, matched_idx, 1, page_size);
        if (matched_idx.size() > page_size) {
            log_and_print(log_file,
                          "[WHERE] showing first %zu rows.\n",
                          page_size);
        }
        return true;
    }

    if (strncasecmp_ascii(line, "COUNT", 5) == 0) {
        std::vector<std::string> args;
        if (!parse_comma_args(line + 5, args) || args.empty()) {
            log_and_print(log_file,
                          "[COUNT] usage: COUNT  or  COUNT(attr, op, value [, AND|OR, attr, op, value] ...)\n");
            return true;
        }
        std::vector<WhereCond> conds;
        std::string err_msg;
        if (!parse_where_args_to_conds(args, conds, err_msg)) {
            log_and_print(log_file,
                          "[COUNT] invalid arguments: %s\n", err_msg.c_str());
            log_and_print(log_file,
                          "[COUNT] usage: COUNT  or  COUNT(attr, op, value [, AND|OR, attr, op, value] ...)\n");
            return true;
        }
        if (conds.size() == 1 && conds[0].op == CondOp::Eq && conds[0].attr != "id") {
            const std::uint64_t before_calls = tbl.decode_heap_slot_calls;
            const CoveringAggLookup cov = lookup_or_build_covering_agg_sidecar(
                eff_data, conds[0].attr, "__count__", conds[0].value, f.schema(), tbl);
            const std::uint64_t after_calls = tbl.decode_heap_slot_calls;
            if (cov.used) {
                log_and_print(log_file,
                              "[COUNT] %zu / %zu rows (with conditions, covering sidecar, decode_delta=%llu)\n",
                              cov.count,
                              tbl.logical_row_count(),
                              static_cast<unsigned long long>(after_calls - before_calls));
                return true;
            }
        }
        const std::vector<std::size_t> candidates = build_candidate_slots(tbl, f.schema(), conds, &f.where());
        if (where_policy_last_blocked(&f.where())) {
            log_and_print(log_file, "[COUNT] blocked: %s\n", where_policy_last_message(&f.where()).c_str());
            return true;
        }
        const std::size_t cnt = candidates.size();
        log_and_print(log_file,
                      "[COUNT] %zu / %zu rows (with conditions)\n",
                      cnt,
                      tbl.logical_row_count());
        return true;
    }
    return false;
}


bool handle_query_min_max_commands(ShellState& st,
                                   const char* line,
                                   const char* log_file,
                                   newdb::HeapTable& tbl) {
    ShellStateFacade f(st);
    if (strncasecmp_ascii(line, "MIN", 3) == 0) {
        std::vector<std::string> args;
        if (!parse_comma_args(line + 3, args) || args.empty()) {
            log_and_print(log_file,
                          "[MIN] usage: MIN(attr) or MIN(attr, WHERE, attr1, op1, value [, AND|OR, attr2, op2, value] ...)\n");
            return true;
        }
        std::string attr;
        std::vector<WhereCond> conds;
        std::string err_msg;
        if (!parse_agg_args_with_optional_where(args, attr, conds, err_msg)) {
            log_and_print(log_file, "[MIN] invalid arguments: %s\n", err_msg.c_str());
            return true;
        }
        newdb::AttrType tp = f.schema().type_of(attr);
        if (!(tp == newdb::AttrType::Int || tp == newdb::AttrType::Float || tp == newdb::AttrType::Double)) {
            log_and_print(log_file,
                          "[MIN] attribute '%s' is not numeric (int/float/double)\n",
                          attr.c_str());
            return true;
        }
        bool has_val = false;
        std::string best;
        const std::vector<std::size_t> candidates = build_candidate_slots(tbl, f.schema(), conds, &f.where());
        if (where_policy_last_blocked(&f.where())) {
            log_and_print(log_file, "[MIN] blocked: %s\n", where_policy_last_message(&f.where()).c_str());
            return true;
        }
        newdb::Row r;
        for (const std::size_t slot : candidates) {
            if (!row_at_slot_read(tbl, slot, r)) {
                continue;
            }
            std::string vstr;
            if (attr == "id") {
                vstr = std::to_string(r.id);
            } else {
                auto it = r.attrs.find(attr);
                if (it == r.attrs.end()) {
                    continue;
                }
                vstr = it->second;
            }
            if (!has_val) {
                best = vstr;
                has_val = true;
            } else {
                int cmp = f.schema().compare_attr(attr, vstr, best);
                if (cmp < 0) {
                    best = vstr;
                }
            }
        }
        if (!has_val) {
            log_and_print(log_file,
                          "[MIN] attr='%s' no rows with this attribute\n",
                          attr.c_str());
        } else {
            log_and_print(log_file,
                          "[MIN] attr='%s' min=%s%s\n",
                          attr.c_str(), best.c_str(),
                          conds.empty() ? "" : " (with conditions)");
        }
        return true;
    }

    if (strncasecmp_ascii(line, "MAX", 3) == 0) {
        std::vector<std::string> args;
        if (!parse_comma_args(line + 3, args) || args.empty()) {
            log_and_print(log_file,
                          "[MAX] usage: MAX(attr) or MAX(attr, WHERE, attr1, op1, value [, AND|OR, attr2, op2, value] ...)\n");
            return true;
        }
        std::string attr;
        std::vector<WhereCond> conds;
        std::string err_msg;
        if (!parse_agg_args_with_optional_where(args, attr, conds, err_msg)) {
            log_and_print(log_file, "[MAX] invalid arguments: %s\n", err_msg.c_str());
            return true;
        }
        newdb::AttrType tp = f.schema().type_of(attr);
        if (!(tp == newdb::AttrType::Int || tp == newdb::AttrType::Float || tp == newdb::AttrType::Double)) {
            log_and_print(log_file,
                          "[MAX] attribute '%s' is not numeric (int/float/double)\n",
                          attr.c_str());
            return true;
        }
        bool has_val = false;
        std::string best;
        const std::vector<std::size_t> candidates = build_candidate_slots(tbl, f.schema(), conds, &f.where());
        if (where_policy_last_blocked(&f.where())) {
            log_and_print(log_file, "[MAX] blocked: %s\n", where_policy_last_message(&f.where()).c_str());
            return true;
        }
        newdb::Row r;
        for (const std::size_t slot : candidates) {
            if (!row_at_slot_read(tbl, slot, r)) {
                continue;
            }
            std::string vstr;
            if (attr == "id") {
                vstr = std::to_string(r.id);
            } else {
                auto it = r.attrs.find(attr);
                if (it == r.attrs.end()) {
                    continue;
                }
                vstr = it->second;
            }
            if (!has_val) {
                best = vstr;
                has_val = true;
            } else {
                int cmp = f.schema().compare_attr(attr, vstr, best);
                if (cmp > 0) {
                    best = vstr;
                }
            }
        }
        if (!has_val) {
            log_and_print(log_file,
                          "[MAX] attr='%s' no rows with this attribute\n",
                          attr.c_str());
        } else {
            log_and_print(log_file,
                          "[MAX] attr='%s' max=%s%s\n",
                          attr.c_str(), best.c_str(),
                          conds.empty() ? "" : " (with conditions)");
        }
        return true;
    }
    return false;
}


bool handle_query_find_commands(ShellState& st,
                                const char* line,
                                const char* log_file,
                                const std::string& eff_data,
                                newdb::HeapTable& tbl) {
    ShellStateFacade f(st);
    const HeapReadViewGuard _heap_read_view(st, tbl);
    if (strncasecmp_ascii(line, "FIND(", 5) == 0) {
        std::vector<std::string> args;
        if (!parse_comma_args(line + 4, args) || args.size() != 1) {
            log_and_print(log_file, "[FIND] usage: FIND(id)\n");
            return true;
        }
        int id = 0;
        try { id = std::stoi(args[0]); } catch (...) {
            log_and_print(log_file, "[FIND] argument must be integer id.\n");
            return true;
        }
        const auto lsm_hit = lsm_lite_find_by_id(st, eff_data, id);
        if (lsm_hit.has_value()) {
            if (lsm_hit->deleted) {
                log_and_print(log_file, "[FIND] id=%d lsm_tombstone not found\n", id);
                return true;
            }
            log_and_print(log_file, "[FIND] id=%d", lsm_hit->row.id);
            for (const auto& kv : lsm_hit->row.attrs) {
                log_and_print(log_file, " %s=%s", kv.first.c_str(), kv.second.c_str());
            }
            log_and_print(log_file, "\n");
            return true;
        }
        const newdb::Row* r = tbl.find_by_id(id);
        if (r) {
            log_and_print(log_file, "[FIND] id=%d", r->id);
            for (const auto& kv : r->attrs) {
                log_and_print(log_file, " %s=%s", kv.first.c_str(), kv.second.c_str());
            }
            log_and_print(log_file, "\n");
        } else {
            log_and_print(log_file, "[FIND] id=%d not found\n", id);
        }
        return true;
    }

    if (strncasecmp_ascii(line, "FINDPK", 6) == 0) {
        std::vector<std::string> args;
        if (!parse_comma_args(line + 6, args) || args.size() != 1) {
            log_and_print(log_file, "[FINDPK] usage: FINDPK(value)\n");
            return true;
        }
        const std::string pk = f.schema().primary_key.empty() ? "id" : f.schema().primary_key;
        const std::string& val = args[0];
        newdb::Row scratch;
        const newdb::Row* found = nullptr;
        for (std::size_t i = 0; i < tbl.logical_row_count(); ++i) {
            newdb::Row r;
            if (!row_at_slot_read(tbl, i, r)) continue;
            std::string v;
            if (!newdb::HeapTable::row_get_pk_value(r, pk, v)) continue;
            if (v == val) {
                scratch = std::move(r);
                found = &scratch;
                break;
            }
        }
        if (!found) {
            log_and_print(log_file, "[FINDPK] not found: %s=%s\n", pk.c_str(), val.c_str());
            return true;
        }
        log_and_print(log_file, "[FINDPK] %s=%s id=%d", pk.c_str(), val.c_str(), found->id);
        for (const auto& kv : found->attrs) {
            log_and_print(log_file, " %s=%s", kv.first.c_str(), kv.second.c_str());
        }
        log_and_print(log_file, "\n");
        return true;
    }

    if (strncasecmp_ascii(line, "QBAL", 4) == 0) {
        std::vector<std::string> args;
        if (!parse_comma_args(line + 4, args) || args.size() != 1) {
            log_and_print(log_file, "[QBAL] usage: QBAL(min_balance)\n");
            return true;
        }
        int min_bal = 0;
        try { min_bal = std::stoi(args[0]); } catch (...) {
            log_and_print(log_file, "[QBAL] argument must be integer.\n");
            return true;
        }
        newdb::io::query_attr_int_ge(eff_data.c_str(),
                                     f.schema().default_int_predicate_attr().c_str(),
                                     min_bal);
        return true;
    }
    return false;
}


bool handle_query_sum_avg_commands(ShellState& st,
                                   const char* line,
                                   const char* log_file,
                                   const std::string& eff_data,
                                   newdb::HeapTable& tbl) {
    ShellStateFacade f(st);
    const HeapReadViewGuard _heap_read_view(st, tbl);
    auto run_numeric_agg = [&](const char* tag, bool is_avg, const char* args_text) -> bool {
        std::vector<std::string> args;
        if (!parse_comma_args(args_text, args) || args.empty()) {
            log_and_print(log_file, "[%s] usage: %s(attr) or %s(attr, WHERE, ...)\n", tag, tag, tag);
            return true;
        }
        std::string attr;
        std::vector<WhereCond> conds;
        std::string err_msg;
        if (!parse_agg_args_with_optional_where(args, attr, conds, err_msg)) {
            log_and_print(log_file, "[%s] invalid arguments: %s\n", tag, err_msg.c_str());
            return true;
        }
        newdb::AttrType tp = f.schema().type_of(attr);
        if (!(tp == newdb::AttrType::Int || tp == newdb::AttrType::Float || tp == newdb::AttrType::Double)) {
            log_and_print(log_file, "[%s] attribute '%s' is not numeric (int/float/double)\n", tag, attr.c_str());
            return true;
        }
        if (conds.size() == 1 && conds[0].op == CondOp::Eq && conds[0].attr != "id") {
            const std::uint64_t before_calls = tbl.decode_heap_slot_calls;
            const CoveringAggLookup cov = lookup_or_build_covering_agg_sidecar(
                eff_data, conds[0].attr, attr, conds[0].value, f.schema(), tbl);
            const std::uint64_t after_calls = tbl.decode_heap_slot_calls;
            if (cov.used) {
                const std::uint64_t decode_delta = after_calls - before_calls;
                if (is_avg) {
                    if (cov.count == 0) {
                        log_and_print(log_file, "[AVG] attr='%s' no numeric rows\n", attr.c_str());
                    } else {
                        log_and_print(log_file,
                                      "[AVG] attr='%s' avg=%.6Lf (matched=%zu / %zu rows) with conditions, covering sidecar, decode_delta=%llu\n",
                                      attr.c_str(),
                                      cov.sum / static_cast<long double>(cov.count),
                                      cov.count,
                                      tbl.logical_row_count(),
                                      static_cast<unsigned long long>(decode_delta));
                    }
                } else {
                    log_and_print(log_file,
                                  "[SUM] attr='%s' sum=%.6Lf (matched=%zu / %zu rows) with conditions, covering sidecar, decode_delta=%llu\n",
                                  attr.c_str(),
                                  cov.sum,
                                  cov.count,
                                  tbl.logical_row_count(),
                                  static_cast<unsigned long long>(decode_delta));
                }
                return true;
            }
        }
        long double sum = 0.0L;
        long long sum_int = 0;
        std::size_t matched = 0;
        newdb::Row r;
        const bool int_fast_path = (tp == newdb::AttrType::Int);
        auto accumulate_row = [&](const newdb::Row& row) {
            if (int_fast_path) {
                long long v = 0;
                if (attr == "id") {
                    v = static_cast<long long>(row.id);
                } else {
                    auto it = row.attrs.find(attr);
                    if (it == row.attrs.end()) return;
                    if (!parse_int64_fast(it->second, v)) return;
                }
                sum_int += v;
                ++matched;
                return;
            }
            std::string vstr;
            if (attr == "id") vstr = std::to_string(row.id);
            else {
                auto it = row.attrs.find(attr);
                if (it == row.attrs.end()) return;
                vstr = it->second;
            }
            try { sum += std::stold(vstr); ++matched; } catch (...) { return; }
        };

        bool fused_done = false;
        if (!conds.empty() && conds.size() >= 3 && all_and_chain_fast(conds)) {
            std::size_t seed_idx = conds.size();
            std::size_t best_cost = std::numeric_limits<std::size_t>::max();
            for (std::size_t i = 0; i < conds.size(); ++i) {
                if (!is_index_friendly_single(conds[i], f.schema())) {
                    continue;
                }
                const std::size_t cost = seed_cost_simple(conds[i], f.schema());
                if (cost < best_cost) {
                    best_cost = cost;
                    seed_idx = i;
                }
            }
            if (seed_idx < conds.size()) {
                std::vector<WhereCond> seed_cond{conds[seed_idx]};
                const std::vector<std::size_t> seed_slots = build_candidate_slots(tbl, f.schema(), seed_cond, &f.where());
                if (where_policy_last_blocked(&f.where())) {
                    log_and_print(log_file, "[%s] blocked: %s\n", tag, where_policy_last_message(&f.where()).c_str());
                    return true;
                }
                for (const std::size_t slot : seed_slots) {
                    if (!row_at_slot_read(tbl, slot, r)) continue;
                    if (!row_match_multi_conditions(r, f.schema(), conds)) continue;
                    accumulate_row(r);
                }
                fused_done = true;
            }
        }

        if (!fused_done) {
            const std::vector<std::size_t> candidates = build_candidate_slots(tbl, f.schema(), conds, &f.where());
            if (where_policy_last_blocked(&f.where())) {
                log_and_print(log_file, "[%s] blocked: %s\n", tag, where_policy_last_message(&f.where()).c_str());
                return true;
            }
            for (const std::size_t slot : candidates) {
                if (!row_at_slot_read(tbl, slot, r)) continue;
                accumulate_row(r);
            }
        }
        if (int_fast_path) {
            sum = static_cast<long double>(sum_int);
        }
        if (is_avg) {
            if (matched == 0) log_and_print(log_file, "[AVG] attr='%s' no numeric rows\n", attr.c_str());
            else log_and_print(log_file, "[AVG] attr='%s' avg=%.6Lf (matched=%zu / %zu rows)%s\n",
                               attr.c_str(), sum / static_cast<long double>(matched), matched, tbl.logical_row_count(),
                               conds.empty() ? "" : " with conditions");
        } else {
            log_and_print(log_file, "[SUM] attr='%s' sum=%.6Lf (matched=%zu / %zu rows)%s\n",
                          attr.c_str(), sum, matched, tbl.logical_row_count(),
                          conds.empty() ? "" : " with conditions");
        }
        return true;
    };
    if (strncasecmp_ascii(line, "SUM", 3) == 0) return run_numeric_agg("SUM", false, line + 3);
    if (strncasecmp_ascii(line, "AVG", 3) == 0) return run_numeric_agg("AVG", true, line + 3);
    return false;
}




