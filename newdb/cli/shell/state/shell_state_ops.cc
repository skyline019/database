#include <waterfall/config.h>

#include "cli/shell/state/shell_state_ops.h"
#include "cli/shell/state/shell_state.h"
#include "cli/shell/state/shell_state_facade.h"
#include "cli/shell/state/shell_state_heap_guard_internal.h"
#include "cli/shell/state/shell_state_impl.h"

#include <newdb/error_format.h>
#include <newdb/heap_table.h>
#include <newdb/page_io.h>
#include <newdb/schema.h>
#include <newdb/schema_io.h>

#include "cli/modules/common/logging/logging.h"
#include "cli/modules/common/view/table_view.h"
#include "cli/modules/sidecar/page/page_index_sidecar.h"
#include "cli/modules/txn/coordinator/txn_manager.h"
#include "cli/modules/where/executor/where.h"
#include "cli/shell/bootstrap/demo_runner_cli_batch.h"
#include "cli/shell/diag/demo_diag.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <sstream>
#include <string>
#include <vector>

namespace {

std::string json_escape_where_plan(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 8);
    for (unsigned char c : s) {
        switch (c) {
            case '\"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\b': out += "\\b"; break;
            case '\f': out += "\\f"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (c < 0x20) {
                    char buf[7];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned int>(c));
                    out += buf;
                } else {
                    out.push_back(static_cast<char>(c));
                }
        }
    }
    return out;
}

} // namespace

newdb::HeapTable* get_cached_table(ShellState& st) {
    ShellStateFacade f(st);
    auto& g = st.impl_->heap_guard_box_->session_heap_guard;
    if (!g.has_value() || !g.value()) {
        g.emplace(f.session().lock_heap(f.log_file_path().c_str()));
    }
    newdb::Session::HeapAccess& acc = g.value();
    return acc ? &f.heap_table() : nullptr;
}

void shell_invalidate_session_table(ShellState& st) {
    st.impl_->heap_guard_box_->session_heap_guard.reset();
    ShellStateFacade(st).session().invalidate();
}

newdb::Status newdb_materialize_heap_if_lazy(newdb::HeapTable& t,
                                             const newdb::TableSchema& sch,
                                             ShellState* stats_sink) {
    if (!t.is_heap_storage_backed()) {
        return newdb::Status::Ok();
    }
    const std::size_t logical_rows = t.logical_row_count();
    std::size_t warn_rows = 10000;
    if (const char* env = std::getenv("NEWDB_LAZY_MATERIALIZE_WARN_ROWS")) {
        try {
            const std::size_t v = static_cast<std::size_t>(std::stoull(env));
            if (v > 0) {
                warn_rows = v;
            }
        } catch (...) {
        }
    }
    if (logical_rows >= warn_rows) {
        std::fprintf(stderr,
                     "[LAZY_MATERIALIZE] forcing full materialization rows=%zu (warn_rows=%zu). "
                     "Prefer PAGE/indexed WHERE/streaming reads for large tables.\n",
                     logical_rows,
                     warn_rows);
    }
    const auto t0 = std::chrono::steady_clock::now();
    const newdb::Status row_st = t.materialize_all_rows(sch);
    if (row_st.ok && stats_sink != nullptr) {
        const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
        stats_sink->txn().noteLazyMaterialize(
            static_cast<std::uint64_t>(logical_rows),
            static_cast<std::uint64_t>(ms < 0 ? 0 : ms));
    }
    return row_st;
}

void reload_schema_from_data_path(ShellState& st, const std::string& data_path) {
    st.impl_->heap_guard_box_->session_heap_guard.reset();
    ShellStateFacade f(st);
    f.data_path() = f.resolve_table_file(data_path);
    (void)newdb::load_schema_text(newdb::schema_sidecar_path_for_data_file(f.data_path()), f.schema());
    f.session().invalidate();
}

bool ShellState::emit_where_plan_json(const char* log_path,
                                      const WhereCond* conds,
                                      const std::size_t cond_count,
                                      std::string* out_json) {
    if (out_json == nullptr || log_path == nullptr) {
        return false;
    }
    if (cond_count > 0 && conds == nullptr) {
        return false;
    }
    try {
        std::vector<WhereCond> cond_vec;
        if (cond_count > 0) {
            cond_vec.assign(conds, conds + cond_count);
        }
        auto ha = this->session().lock_heap(log_path);
        if (!ha) {
            *out_json = "{\"ok\":0,\"error\":\"lock_heap_failed\"}\n";
            return false;
        }
        const auto plan = where_build_plan_candidates(*ha.table(), this->session_schema(), cond_vec,
                                                      WherePlanningStatsRef{this->where_ctx().query_stats_hint});
        std::size_t chosen = 0;
        for (std::size_t i = 1; i < plan.size(); ++i) {
            if (plan[i].estimated_cost < plan[chosen].estimated_cost) {
                chosen = i;
            }
        }
        std::ostringstream oss;
        oss << "{\"ok\":1";
        if (!plan.empty()) {
            oss << ",\"chosen_plan_id\":\"" << json_escape_where_plan(plan[chosen].id) << "\""
                << ",\"chosen_reason\":\"" << json_escape_where_plan(plan[chosen].rationale) << "\""
                << ",\"chosen_estimated_cost\":" << plan[chosen].estimated_cost;
        }
        oss << ",\"candidates\":[";
        for (std::size_t i = 0; i < plan.size(); ++i) {
            if (i > 0) {
                oss << ",";
            }
            const auto& p = plan[i];
            oss << "{\"id\":\"" << json_escape_where_plan(p.id) << "\",\"estimated_cost\":" << p.estimated_cost
                << ",\"estimated_rows\":" << p.cost.estimated_rows << ",\"rationale\":\""
                << json_escape_where_plan(p.rationale) << "\"";
            if (!plan.empty() && i != chosen) {
                oss << ",\"reason_rejected\":\"not_chosen\"";
            } else if (!plan.empty() && i == chosen) {
                oss << ",\"chosen\":true";
            }
            oss << "}";
        }
        oss << "]}\n";
        *out_json = oss.str();
        return true;
    } catch (...) {
        *out_json = "{\"ok\":0,\"error\":\"exception\"}\n";
        return false;
    }
}

namespace {

std::string json_escape_page_json(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 8);
    for (unsigned char c : s) {
        switch (c) {
            case '\"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\b': out += "\\b"; break;
            case '\f': out += "\\f"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (c < 0x20) {
                    char buf[7];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned int>(c));
                    out += buf;
                } else {
                    out.push_back(static_cast<char>(c));
                }
        }
    }
    return out;
}

bool row_at_slot_read_local_batch(const newdb::HeapTable& tbl, const std::size_t i, newdb::Row& r) {
    if (tbl.is_heap_storage_backed()) {
        return tbl.decode_heap_slot(i, r);
    }
    r = tbl.rows[i];
    return true;
}

void print_page_json_batch(const newdb::TableSchema& schema,
                           const newdb::HeapTable& tbl,
                           const std::vector<std::size_t>& sorted_idx,
                           const std::size_t page_no,
                           const std::size_t page_size,
                           const std::string& order_key,
                           const bool desc) {
    const std::size_t total = sorted_idx.size();
    const std::size_t total_pages = page_size == 0 ? 0 : (total + page_size - 1) / page_size;
    std::printf("[PAGE_JSON] {");
    std::printf("\"table\":\"%s\",", json_escape_page_json(tbl.name).c_str());
    std::printf("\"order\":\"%s\",", json_escape_page_json(order_key).c_str());
    std::printf("\"descending\":%s,", desc ? "true" : "false");
    std::printf("\"page_no\":%zu,\"page_size\":%zu,\"total\":%zu,\"total_pages\":%zu,", page_no, page_size, total, total_pages);

    std::printf("\"headers\":[\"id\"");
    for (const auto& m : schema.attrs) {
        if (m.name == "id") {
            continue;
        }
        std::printf(",\"%s\"", json_escape_page_json(m.name).c_str());
    }
    std::printf("],");

    std::printf("\"rows\":[");
    if (page_size > 0 && page_no > 0 && page_no <= total_pages) {
        const std::size_t begin = (page_no - 1) * page_size;
        const std::size_t end = std::min(begin + page_size, total);
        bool first_row = true;
        for (std::size_t p = begin; p < end; ++p) {
            newdb::Row r;
            if (!row_at_slot_read_local_batch(tbl, sorted_idx[p], r)) {
                continue;
            }
            if (!first_row) {
                std::printf(",");
            }
            first_row = false;
            std::printf("[\"%d\"", r.id);
            for (const auto& m : schema.attrs) {
                if (m.name == "id") {
                    continue;
                }
                const auto it = r.attrs.find(m.name);
                const std::string v = (it == r.attrs.end()) ? std::string() : it->second;
                std::printf(",\"%s\"", json_escape_page_json(v).c_str());
            }
            std::printf("]");
        }
    }
    std::printf("]}");
    std::printf("\n");
}

} // namespace

int demo_run_cli_batch_query_balance(ShellState& app,
                                     const std::string& resolved_data_path,
                                     const std::string& data_table,
                                     const CliBatchQueryBalance& qb) {
    newdb::Session batch;
    batch.data_path = resolved_data_path;
    batch.table_name = data_table;
    const newdb::Status lds = batch.reload();
    if (!lds.ok) {
        const std::string msg = newdb::format_error_line("session", "load_failed", lds.message);
        logging_stderr_printf("%s\n", msg.c_str());
        demo_verbose(ShellStateFacade(app), "batch data_path=%s table=%s\n", batch.data_path.c_str(), batch.table_name.c_str());
        return 1;
    }
    newdb::io::query_attr_int_ge(batch.data_path.c_str(),
                                 batch.schema.default_int_predicate_attr().c_str(),
                                 qb.min_balance);
    return 0;
}

int demo_run_cli_batch_find_id(ShellState& app,
                               const std::string& resolved_data_path,
                               const std::string& data_table,
                               const CliBatchFindId& bf) {
    newdb::Session batch;
    batch.data_path = resolved_data_path;
    batch.table_name = data_table;
    const newdb::Status lds = batch.reload();
    if (!lds.ok) {
        const std::string msg = newdb::format_error_line("session", "load_failed", lds.message);
        logging_stderr_printf("%s\n", msg.c_str());
        demo_verbose(ShellStateFacade(app), "batch data_path=%s table=%s\n", batch.data_path.c_str(), batch.table_name.c_str());
        return 1;
    }
    newdb::HeapTable& tbl = batch.table;
    const newdb::Row* r = tbl.find_by_id(bf.id);
    if (r) {
        logging_console_printf("[FIND] table=%s id=%d", tbl.name.c_str(), r->id);
        for (const auto& kv : r->attrs) {
            logging_console_printf(" %s=%s", kv.first.c_str(), kv.second.c_str());
        }
        logging_console_printf("\n");
        return 0;
    }
    logging_stderr_printf("[FIND] id=%d not found in table=%s\n", bf.id, tbl.name.c_str());
    return 1;
}

int demo_run_cli_batch_page(ShellState& app,
                            const std::string& resolved_data_path,
                            const std::string& data_table,
                            const CliBatchPage& pg) {
    newdb::Session batch;
    batch.data_path = resolved_data_path;
    batch.table_name = data_table;
    const newdb::Status lds = batch.reload();
    if (!lds.ok) {
        const std::string msg = newdb::format_error_line("session", "load_failed", lds.message);
        logging_stderr_printf("%s\n", msg.c_str());
        demo_verbose(ShellStateFacade(app), "batch data_path=%s table=%s\n", batch.data_path.c_str(), batch.table_name.c_str());
        return 1;
    }
    newdb::HeapTable& tbl = batch.table;
    const newdb::SortDir dir = pg.descending ? newdb::SortDir::Desc : newdb::SortDir::Asc;
    const std::vector<std::size_t> sorted_idx = load_or_build_page_index_sidecar(
        PageSidecarRequest{
            .data_file = batch.data_path,
            .table_name = batch.table_name,
            .order_key = pg.order_key,
            .descending = pg.descending,
        },
        batch.schema,
        tbl);
    logging_console_printf("[PAGE] table=%s order_by=%s %s\n",
                           tbl.name.c_str(),
                           pg.order_key.c_str(),
                           dir == newdb::SortDir::Asc ? "asc" : "desc");
    if (pg.json_output) {
        print_page_json_batch(batch.schema, tbl, sorted_idx, pg.page_no, pg.page_size, pg.order_key, pg.descending);
    } else {
        table_view::print_page_indexed(batch.schema, tbl, sorted_idx, pg.page_no, pg.page_size);
    }
    return 0;
}
