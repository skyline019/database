#include <waterfall/config.h>

#include "cli/modules/where/executor/plan/plan_scan_estimate.h"

#include "cli/modules/where/executor/stats/table_stats.h"

#include <algorithm>
#include <cstdlib>

std::size_t where_estimate_scan_rows(const newdb::HeapTable& tbl,
                                     const newdb::TableSchema& schema,
                                     const std::vector<WhereCond>& conds,
                                     WhereQueryContext* ctx_ptr) {
    (void)schema;
    const std::size_t n = tbl.logical_row_count();
    if (conds.empty()) {
        return n;
    }
    long double est = static_cast<long double>(n);
    for (const auto& c : conds) {
        long double factor = 1.0L;
        if (c.attr == "id" && c.op == CondOp::Eq) {
            factor = 0.0001L;
        } else if (c.op == CondOp::Eq) {
            factor = 0.08L;
            if (const char* raw = std::getenv("NEWDB_QUERY_USE_TABLE_STATS")) {
                if (raw[0] == '1' && raw[1] == '\0' && ctx_ptr != nullptr && ctx_ptr->query_stats_hint != nullptr) {
                    const double sel = eq_selectivity_from_stats(ctx_ptr->query_stats_hint, c.attr, n);
                    if (sel > 0.0) {
                        factor = static_cast<long double>(sel);
                    }
                }
            }
        } else if (c.op == CondOp::Contains) {
            factor = 0.35L;
            if (const char* raw = std::getenv("NEWDB_QUERY_USE_TABLE_STATS")) {
                if (raw[0] == '1' && raw[1] == '\0' && ctx_ptr != nullptr && ctx_ptr->query_stats_hint != nullptr) {
                    const double span = range_selectivity_from_stats(ctx_ptr->query_stats_hint, c.attr, n);
                    if (span > 0.0) {
                        factor = static_cast<long double>(span);
                    }
                }
            }
        } else if (c.op == CondOp::Ge || c.op == CondOp::Gt || c.op == CondOp::Le || c.op == CondOp::Lt) {
            factor = 0.45L;
            if (const char* raw = std::getenv("NEWDB_QUERY_USE_TABLE_STATS")) {
                if (raw[0] == '1' && raw[1] == '\0' && ctx_ptr != nullptr && ctx_ptr->query_stats_hint != nullptr) {
                    const double span = range_selectivity_from_stats(ctx_ptr->query_stats_hint, c.attr, n);
                    if (span > 0.0) {
                        factor = static_cast<long double>(span);
                    }
                }
            }
        } else if (c.op == CondOp::Ne) {
            factor = 0.45L;
            if (const char* raw = std::getenv("NEWDB_QUERY_USE_TABLE_STATS")) {
                if (raw[0] == '1' && raw[1] == '\0' && ctx_ptr != nullptr && ctx_ptr->query_stats_hint != nullptr) {
                    const double eq = eq_selectivity_from_stats(ctx_ptr->query_stats_hint, c.attr, n);
                    if (eq > 0.0) {
                        factor = static_cast<long double>(std::min(0.49, std::max(0.05, 1.0 - eq * 0.95)));
                    }
                }
            }
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
}
