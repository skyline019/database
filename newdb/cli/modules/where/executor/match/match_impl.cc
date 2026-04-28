#include <waterfall/config.h>

#include "cli/modules/where/executor/where.h"
#include "cli/modules/where/executor/internal/query_internal.h"
#include "cli/modules/sidecar/eq/equality_index_sidecar.h"
#include "cli/modules/sidecar/visibility/visibility_checkpoint_sidecar.h"

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



