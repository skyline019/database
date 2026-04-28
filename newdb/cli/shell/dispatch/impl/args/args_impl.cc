#include <waterfall/config.h>

#include <charconv>
#include <cctype>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include <newdb/schema_io.h>

#include "cli/shell/dispatch/internal/dispatch_internal.h"
#include "cli/modules/logging/logging.h"
#include "cli/shell/state/shell_state.h"
#include "cli/modules/util/utils.h"
#include "cli/modules/where/executor/where.h"

namespace {
int ascii_tolower(int c) {
    return std::tolower(static_cast<unsigned char>(c));
}
} // namespace

int strncasecmp_ascii(const char* a, const char* b, std::size_t n) {
    for (std::size_t i = 0; i < n; ++i) {
        const unsigned char ca = static_cast<unsigned char>(a[i]);
        const unsigned char cb = static_cast<unsigned char>(b[i]);
        if (ca == 0 || cb == 0) {
            return (ca == cb) ? 0 : (ca == 0 ? -1 : 1);
        }
        const int la = ascii_tolower(ca);
        const int lb = ascii_tolower(cb);
        if (la != lb) {
            return la < lb ? -1 : 1;
        }
    }
    return 0;
}

int strcasecmp_ascii(const char* a, const char* b) {
    if (!a || !b) return a == b ? 0 : (a ? 1 : -1);
    std::size_t i = 0;
    for (;;) {
        const unsigned char ca = static_cast<unsigned char>(a[i]);
        const unsigned char cb = static_cast<unsigned char>(b[i]);
        if (ca == 0 || cb == 0) {
            return (ca == cb) ? 0 : (ca == 0 ? -1 : 1);
        }
        const int la = ascii_tolower(ca);
        const int lb = ascii_tolower(cb);
        if (la != lb) {
            return la < lb ? -1 : 1;
        }
        ++i;
    }
}

bool row_at_slot_read(const newdb::HeapTable& tbl, const std::size_t i, newdb::Row& r) {
    if (tbl.is_heap_storage_backed()) {
        return tbl.decode_heap_slot(i, r);
    }
    r = tbl.rows[i];
    return true;
}

bool parse_int64_fast(const std::string& s, long long& out) {
    const char* begin = s.data();
    const char* end = s.data() + s.size();
    const auto [ptr, ec] = std::from_chars(begin, end, out);
    return ec == std::errc{} && ptr == end;
}

bool validate_typed_attr_value(const char* tag,
                               const char* log_file,
                               const newdb::AttrMeta& meta,
                               const std::string& value) {
    std::string val = value;
    if (meta.type == newdb::AttrType::Date && (val == "now" || val == "NOW")) {
        val = get_current_date_str();
    } else if ((meta.type == newdb::AttrType::DateTime || meta.type == newdb::AttrType::Timestamp) &&
               (val == "now" || val == "NOW")) {
        val = get_current_datetime_str();
    }
    if (meta.type == newdb::AttrType::Date && !val.empty() && val != "0" && !is_valid_date_str(val)) {
        log_and_print(log_file, "[%s] attribute '%s' expects date YYYY-MM-DD, got '%s'\n",
                      tag, meta.name.c_str(), val.c_str());
        return false;
    }
    if ((meta.type == newdb::AttrType::DateTime || meta.type == newdb::AttrType::Timestamp) &&
        !val.empty() && val != "0" && !is_valid_datetime_str(val)) {
        log_and_print(log_file, "[%s] attribute '%s' expects datetime YYYY-MM-DD HH:MM:SS, got '%s'\n",
                      tag, meta.name.c_str(), val.c_str());
        return false;
    }
    switch (meta.type) {
    case newdb::AttrType::Int:
        try {
            (void)std::stoll(val);
        } catch (...) {
            log_and_print(log_file, "[%s] attribute '%s' expects int, got '%s'\n",
                          tag, meta.name.c_str(), val.c_str());
            return false;
        }
        break;
    case newdb::AttrType::Float:
        try {
            (void)std::stof(val);
        } catch (...) {
            log_and_print(log_file, "[%s] attribute '%s' expects float, got '%s'\n",
                          tag, meta.name.c_str(), val.c_str());
            return false;
        }
        break;
    case newdb::AttrType::Double:
        try {
            (void)std::stod(val);
        } catch (...) {
            log_and_print(log_file, "[%s] attribute '%s' expects double, got '%s'\n",
                          tag, meta.name.c_str(), val.c_str());
            return false;
        }
        break;
    case newdb::AttrType::Char:
        if (val.size() != 1) {
            log_and_print(log_file, "[%s] attribute '%s' expects single char, got '%s'\n",
                          tag, meta.name.c_str(), val.c_str());
            return false;
        }
        break;
    case newdb::AttrType::Bool: {
        std::string lv = val;
        for (auto& c : lv) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        if (!(lv == "0" || lv == "1" || lv == "true" || lv == "false" || lv == "y" || lv == "n" ||
              lv == "yes" || lv == "no")) {
            log_and_print(log_file, "[%s] attribute '%s' expects bool(0/1/true/false/yes/no), got '%s'\n",
                          tag, meta.name.c_str(), val.c_str());
            return false;
        }
        break;
    }
    default:
        break;
    }
    return true;
}

void refresh_schema_if_missing(ShellState& st, const std::string& eff_data) {
    if (!st.session.schema.attrs.empty() || eff_data.empty()) {
        return;
    }
    newdb::TableSchema loaded;
    const newdb::Status s = newdb::load_schema_text(
        newdb::schema_sidecar_path_for_data_file(eff_data), loaded);
    if (s.ok && (!loaded.attrs.empty() || !loaded.primary_key.empty())) {
        st.session.schema = std::move(loaded);
    }
}

bool all_and_chain_fast(const std::vector<WhereCond>& conds) {
    for (std::size_t i = 1; i < conds.size(); ++i) {
        if (conds[i].logic_with_prev != "AND") {
            return false;
        }
    }
    return !conds.empty();
}

bool is_index_friendly_single(const WhereCond& c, const newdb::TableSchema& schema) {
    return c.attr == "id" || c.attr == schema.primary_key || c.op == CondOp::Eq;
}

std::size_t seed_cost_simple(const WhereCond& c, const newdb::TableSchema& schema) {
    if (c.attr == "id" && c.op == CondOp::Eq) return 0;
    if (c.attr == schema.primary_key && c.op == CondOp::Eq) return 1;
    if (c.op == CondOp::Eq) return 2;
    if (c.op == CondOp::Ge || c.op == CondOp::Gt || c.op == CondOp::Le || c.op == CondOp::Lt) return 3;
    return 4;
}




