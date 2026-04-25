#include <waterfall/config.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <limits>
#include <map>
#include <numeric>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <newdb/error.h>
#include <newdb/page_io.h>
#include <newdb/schema_io.h>
#include <newdb/session.h>

#include "condition.h"
#include "demo_commands.h"
#include "demo_diag.h"
#include "demo_export.h"
#include "covering_index_sidecar.h"
#include "equality_index_sidecar.h"
#include "import.h"
#include "logging.h"
#include "page_index_sidecar.h"
#include "schema_catalog.h"
#include "shell_state.h"
#include "table_view.h"
#include "txn_manager.h"
#include "utils.h"
#include "sidecar_wal_lsn.h"
#include "visibility_checkpoint_sidecar.h"
#include "where.h"

namespace {

int ascii_tolower(int c) {
    return std::tolower(static_cast<unsigned char>(c));
}

int ascii_toupper(int c) {
    return std::toupper(static_cast<unsigned char>(c));
}

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
    } else if ((meta.type == newdb::AttrType::DateTime || meta.type == newdb::AttrType::Timestamp)
               && (val == "now" || val == "NOW")) {
        val = get_current_datetime_str();
    }
    if (meta.type == newdb::AttrType::Date && !val.empty() && val != "0" && !is_valid_date_str(val)) {
        log_and_print(log_file, "[%s] attribute '%s' expects date YYYY-MM-DD, got '%s'\n",
                      tag, meta.name.c_str(), val.c_str());
        return false;
    }
    if ((meta.type == newdb::AttrType::DateTime || meta.type == newdb::AttrType::Timestamp)
        && !val.empty() && val != "0" && !is_valid_datetime_str(val)) {
        log_and_print(log_file, "[%s] attribute '%s' expects datetime YYYY-MM-DD HH:MM:SS, got '%s'\n",
                      tag, meta.name.c_str(), val.c_str());
        return false;
    }
    switch (meta.type) {
    case newdb::AttrType::Int:
        try { (void)std::stoll(val); } catch (...) {
            log_and_print(log_file, "[%s] attribute '%s' expects int, got '%s'\n",
                          tag, meta.name.c_str(), val.c_str());
            return false;
        }
        break;
    case newdb::AttrType::Float:
        try { (void)std::stof(val); } catch (...) {
            log_and_print(log_file, "[%s] attribute '%s' expects float, got '%s'\n",
                          tag, meta.name.c_str(), val.c_str());
            return false;
        }
        break;
    case newdb::AttrType::Double:
        try { (void)std::stod(val); } catch (...) {
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
        if (!(lv == "0" || lv == "1" || lv == "true" || lv == "false" || lv == "y" || lv == "n" || lv == "yes" || lv == "no")) {
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

void fast_index_insert(newdb::HeapTable& tbl,
                       const newdb::TableSchema& schema,
                       const newdb::Row& row,
                       const std::size_t slot) {
    tbl.index_by_id[row.id] = slot;
    if (!schema.primary_key.empty() && schema.primary_key != "id") {
        std::string pkv;
        if (newdb::HeapTable::row_get_pk_value(row, schema.primary_key, pkv)) {
            tbl.index_by_pk_value[pkv] = slot;
        }
    }
    if (tbl.attr_index.empty() && !schema.attrs.empty()) {
        for (std::size_t i = 0; i < schema.attrs.size(); ++i) {
            tbl.attr_index[schema.attrs[i].name] = i;
        }
    }
    // Any write invalidates ordering caches.
    tbl.sorted_cache_asc.clear();
    tbl.sorted_cache_desc.clear();
}

void fast_index_update_slot(newdb::HeapTable& tbl,
                            const newdb::TableSchema& schema,
                            const newdb::Row& old_row,
                            const newdb::Row& new_row,
                            const std::size_t slot) {
    tbl.index_by_id.erase(old_row.id);
    tbl.index_by_id[new_row.id] = slot;
    if (!schema.primary_key.empty() && schema.primary_key != "id") {
        std::string old_pk;
        if (newdb::HeapTable::row_get_pk_value(old_row, schema.primary_key, old_pk)) {
            tbl.index_by_pk_value.erase(old_pk);
        }
        std::string new_pk;
        if (newdb::HeapTable::row_get_pk_value(new_row, schema.primary_key, new_pk)) {
            tbl.index_by_pk_value[new_pk] = slot;
        }
    }
    tbl.sorted_cache_asc.clear();
    tbl.sorted_cache_desc.clear();
}

void fast_index_remove_slot(newdb::HeapTable& tbl,
                            const newdb::TableSchema& schema,
                            const newdb::Row& removed_row,
                            const std::size_t removed_slot,
                            const std::optional<newdb::Row>& moved_row = std::nullopt) {
    tbl.index_by_id.erase(removed_row.id);
    if (!schema.primary_key.empty() && schema.primary_key != "id") {
        std::string old_pk;
        if (newdb::HeapTable::row_get_pk_value(removed_row, schema.primary_key, old_pk)) {
            tbl.index_by_pk_value.erase(old_pk);
        }
    }
    // O(1) remove path: swap-with-last then pop, only fix moved row slot.
    if (moved_row.has_value()) {
        tbl.index_by_id[moved_row->id] = removed_slot;
        if (!schema.primary_key.empty() && schema.primary_key != "id") {
            std::string moved_pk;
            if (newdb::HeapTable::row_get_pk_value(*moved_row, schema.primary_key, moved_pk)) {
                tbl.index_by_pk_value[moved_pk] = removed_slot;
            }
        }
    }
    tbl.sorted_cache_asc.clear();
    tbl.sorted_cache_desc.clear();
}

void invalidate_eq_sidecars_after_write(const std::string& eff_data) {
    if (eff_data.empty()) {
        return;
    }
    invalidate_eq_index_sidecars_for_data_file(eff_data);
    invalidate_page_index_sidecars_for_data_file(eff_data);
    invalidate_covering_sidecars_for_data_file(eff_data);
    invalidate_visibility_checkpoint_sidecars_for_data_file(eff_data);
}

void invalidate_eq_sidecars_after_write(const std::string& eff_data,
                                        const std::set<std::string>& attrs) {
    if (eff_data.empty()) {
        return;
    }
    if (attrs.empty()) {
        invalidate_eq_index_sidecars_for_data_file(eff_data);
        invalidate_page_index_sidecars_for_data_file(eff_data);
        invalidate_covering_sidecars_for_data_file(eff_data);
        invalidate_visibility_checkpoint_sidecars_for_data_file(eff_data);
        return;
    }
    invalidate_eq_index_sidecars_for_attrs(eff_data, attrs);
    invalidate_page_index_sidecars_for_order_attrs(eff_data, attrs);
    invalidate_covering_sidecars_for_attrs(eff_data, attrs);
    invalidate_visibility_checkpoint_sidecars_for_attrs(eff_data, attrs);
}

bool handle_txn_commands(ShellState& st, const char* line, const char* log_file, const std::string& current_table) {
    namespace fs = std::filesystem;
    auto txn_backup_enabled = [&]() {
        const char* env = std::getenv("NEWDB_TXN_SNAPSHOT_BACKUP");
        if (!env) return false;
        std::string v = env;
        for (auto& ch : v) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
        return v == "1" || v == "on" || v == "true" || v == "yes";
    };
    auto txn_backup_path = [&]() {
        return effective_data_path(st) + ".txn.bak";
    };
    // BEGIN [table_name] - 开始事务
    if (strncasecmp_ascii(line, "BEGIN", 5) == 0) {
        std::string table = current_table;
        if (std::strlen(line) > 5) {
            std::string arg = trim(line + 5);
            if (!arg.empty()) table = arg;
        }
        if (st.txn.begin(table)) {
            if (txn_backup_enabled()) {
                // Optional snapshot backup (off by default to reduce write amplification).
                std::error_code ec;
                const std::string data_file = effective_data_path(st);
                const std::string bak_file = txn_backup_path();
                if (!data_file.empty() && fs::exists(data_file, ec)) {
                    fs::copy_file(data_file, bak_file, fs::copy_options::overwrite_existing, ec);
                }
            }
            log_and_print(log_file, "[TXN] transaction started (id=%lld, table=%s)\n",
                          (long long)st.txn.getTxnId(), table.c_str());
        } else {
            log_and_print(log_file, "[TXN] failed to start transaction\n");
        }
        return true;
    }
    // COMMIT - 提交事务
    if (strcasecmp_ascii(line, "COMMIT") == 0) {
        if (st.txn.commit()) {
            std::error_code ec;
            fs::remove(txn_backup_path(), ec);
            log_and_print(log_file, "[TXN] transaction committed\n");
            st.txn.flushWAL();
        } else {
            log_and_print(log_file, "[TXN] no active transaction to commit\n");
        }
        return true;
    }
    // ROLLBACK - 回滚事务
    if (strcasecmp_ascii(line, "ROLLBACK") == 0) {
        if (st.txn.rollback()) {
            std::error_code ec;
            const std::string data_file = effective_data_path(st);
            const std::string bak_file = txn_backup_path();
            if (!data_file.empty() && fs::exists(bak_file, ec)) {
                fs::copy_file(bak_file, data_file, fs::copy_options::overwrite_existing, ec);
                fs::remove(bak_file, ec);
            }
            log_and_print(log_file, "[TXN] transaction rolled back\n");
            // Rollback appends compensation records to disk; invalidate cache so
            // subsequent reads (e.g. FIND/PAGE) observe rolled-back state.
            shell_invalidate_session_table(st);
        } else {
            log_and_print(log_file, "[TXN] no active transaction to rollback\n");
        }
        return true;
    }
    // SHOW TUNING - 输出写路径调优状态
    if (strcasecmp_ascii(line, "SHOW TUNING") == 0 || strcasecmp_ascii(line, "SHOW STATUS") == 0) {
        const TxnRuntimeStats stats = st.txn.runtimeStats();
        const auto mode = st.txn.walSyncMode();
        const char* mode_s = (mode == newdb::WalSyncMode::Off) ? "off" :
                             (mode == newdb::WalSyncMode::Normal) ? "normal" : "full";
        log_and_print(log_file,
                      "[TUNING] WALSYNC=%s normal_interval_ms=%llu AUTOVACUUM=%s ops_threshold=%zu min_interval_sec=%zu trigger_count=%llu execute_count=%llu cooldown_skips=%llu write_conflicts=%llu begin_lock_conflicts=%llu wal_compacts=%llu\n",
                      mode_s,
                      static_cast<unsigned long long>(st.txn.walNormalSyncIntervalMs()),
                      st.txn.vacuumRunning() ? "on" : "off",
                      st.txn.vacuumOpsThreshold(),
                      st.txn.vacuumMinIntervalSec(),
                      static_cast<unsigned long long>(stats.vacuum_trigger_count),
                      static_cast<unsigned long long>(stats.vacuum_execute_count),
                      static_cast<unsigned long long>(stats.vacuum_cooldown_skip_count),
                      static_cast<unsigned long long>(stats.write_conflict_count),
                      static_cast<unsigned long long>(stats.txn_begin_lock_conflict_count),
                      static_cast<unsigned long long>(stats.wal_compact_count));
        return true;
    }
    if (strcasecmp_ascii(line, "SHOW TUNING JSON") == 0 || strcasecmp_ascii(line, "SHOW STATUS JSON") == 0) {
        const TxnRuntimeStats stats = st.txn.runtimeStats();
        const auto mode = st.txn.walSyncMode();
        const char* mode_s = (mode == newdb::WalSyncMode::Off) ? "off" :
                             (mode == newdb::WalSyncMode::Normal) ? "normal" : "full";
        log_and_print(log_file,
                      "{\"walsync\":\"%s\",\"normal_interval_ms\":%llu,\"autovacuum\":%s,"
                      "\"vacuum_ops_threshold\":%zu,\"vacuum_min_interval_sec\":%zu,"
                      "\"vacuum_trigger_count\":%llu,\"vacuum_execute_count\":%llu,"
                      "\"vacuum_cooldown_skip_count\":%llu,\"write_conflicts\":%llu,"
                      "\"txn_begin_lock_conflicts\":%llu,\"wal_compact_count\":%llu}\n",
                      mode_s,
                      static_cast<unsigned long long>(st.txn.walNormalSyncIntervalMs()),
                      st.txn.vacuumRunning() ? "true" : "false",
                      st.txn.vacuumOpsThreshold(),
                      st.txn.vacuumMinIntervalSec(),
                      static_cast<unsigned long long>(stats.vacuum_trigger_count),
                      static_cast<unsigned long long>(stats.vacuum_execute_count),
                      static_cast<unsigned long long>(stats.vacuum_cooldown_skip_count),
                      static_cast<unsigned long long>(stats.write_conflict_count),
                      static_cast<unsigned long long>(stats.txn_begin_lock_conflict_count),
                      static_cast<unsigned long long>(stats.wal_compact_count));
        return true;
    }
    if (strcasecmp_ascii(line, "SHOW STORAGE") == 0) {
        namespace fs = std::filesystem;
        std::error_code ec;
        const fs::path ws = workspace_directory(st);
        const fs::path wal = ws / "demodb.wal";
        std::uint64_t wal_bytes{0};
        if (fs::exists(wal, ec)) {
            const auto sz = fs::file_size(wal, ec);
            if (!ec) {
                wal_bytes = static_cast<std::uint64_t>(sz);
            }
        }
        const std::uint64_t lsn = read_wal_lsn_for_workspace(ws.string());
        std::uint64_t bin_bytes{0};
        std::uint32_t bin_files{0};
        if (fs::is_directory(ws, ec)) {
            for (const auto& ent : fs::directory_iterator(ws, ec)) {
                if (ec) {
                    break;
                }
                if (!ent.is_regular_file(ec)) {
                    continue;
                }
                if (ent.path().extension() == ".bin") {
                    const auto fsz = fs::file_size(ent.path(), ec);
                    if (!ec) {
                        bin_bytes += static_cast<std::uint64_t>(fsz);
                    }
                    ++bin_files;
                }
            }
        }
        log_and_print(log_file,
                      "[STORAGE] workspace=%s demodb.wal bytes=%llu demodb.wal_lsn=%llu "
                      "total *.bin files=%u bytes=%llu\n",
                      ws.string().c_str(),
                      static_cast<unsigned long long>(wal_bytes),
                      static_cast<unsigned long long>(lsn),
                      static_cast<unsigned int>(bin_files),
                      static_cast<unsigned long long>(bin_bytes));
        return true;
    }
    // AUTOVACUUM - 设置自动 VACUUM
    if (strncasecmp_ascii(line, "AUTOVACUUM", 10) == 0) {
        if (std::strlen(line) > 10) {
            std::string arg = trim(line + 10);
            std::istringstream iss(arg);
            std::string mode;
            std::string threshold_token;
            iss >> mode >> threshold_token;
            if (mode == "1" || mode == "on") {
                if (!threshold_token.empty()) {
                    try {
                        st.txn.setVacuumOpsThreshold(static_cast<std::size_t>(std::stoull(threshold_token)));
                    } catch (...) {
                        log_and_print(log_file, "[VACUUM] invalid threshold: %s\n", threshold_token.c_str());
                        return true;
                    }
                }
                st.txn.startVacuumThread();
                log_and_print(log_file, "[VACUUM] auto vacuum enabled (ops_threshold=%zu)\n", st.txn.vacuumOpsThreshold());
            } else if (mode == "0" || mode == "off") {
                st.txn.stopVacuumThread();
                log_and_print(log_file, "[VACUUM] auto vacuum disabled\n");
            } else if (mode == "threshold") {
                if (threshold_token.empty()) {
                    log_and_print(log_file, "[VACUUM] usage: AUTOVACUUM threshold <ops>\n");
                    return true;
                }
                try {
                    st.txn.setVacuumOpsThreshold(static_cast<std::size_t>(std::stoull(threshold_token)));
                    log_and_print(log_file, "[VACUUM] ops threshold set to %zu\n", st.txn.vacuumOpsThreshold());
                } catch (...) {
                    log_and_print(log_file, "[VACUUM] invalid threshold: %s\n", threshold_token.c_str());
                }
            } else if (mode == "interval") {
                if (threshold_token.empty()) {
                    log_and_print(log_file, "[VACUUM] usage: AUTOVACUUM interval <sec>\n");
                    return true;
                }
                try {
                    st.txn.setVacuumMinIntervalSec(static_cast<std::size_t>(std::stoull(threshold_token)));
                    log_and_print(log_file, "[VACUUM] min interval set to %zu sec\n", st.txn.vacuumMinIntervalSec());
                } catch (...) {
                    log_and_print(log_file, "[VACUUM] invalid interval: %s\n", threshold_token.c_str());
                }
            } else {
                log_and_print(log_file, "[VACUUM] usage: AUTOVACUUM [0|1|on|off] [ops_threshold] | AUTOVACUUM threshold <ops> | AUTOVACUUM interval <sec>\n");
            }
        } else {
            log_and_print(log_file, "[VACUUM] auto=%s ops_threshold=%zu min_interval_sec=%zu\n",
                          st.txn.vacuumRunning() ? "on" : "off",
                          st.txn.vacuumOpsThreshold(),
                          st.txn.vacuumMinIntervalSec());
        }
        return true;
    }
    if (strncasecmp_ascii(line, "WALSYNC", 7) == 0) {
        std::string arg = trim(line + 7);
        if (arg.empty()) {
            const auto mode = st.txn.walSyncMode();
            const char* mode_s = (mode == newdb::WalSyncMode::Off) ? "off" :
                                 (mode == newdb::WalSyncMode::Normal) ? "normal" : "full";
            log_and_print(log_file, "[WAL] sync mode=%s normal_interval_ms=%llu\n",
                          mode_s,
                          static_cast<unsigned long long>(st.txn.walNormalSyncIntervalMs()));
            return true;
        }
        std::istringstream iss(arg);
        std::string lower;
        std::string interval_token;
        iss >> lower >> interval_token;
        for (auto& ch : lower) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
        if (lower == "off") {
            st.txn.setWalSyncMode(newdb::WalSyncMode::Off);
        } else if (lower == "normal") {
            st.txn.setWalSyncMode(newdb::WalSyncMode::Normal);
            if (!interval_token.empty()) {
                try {
                    st.txn.setWalNormalSyncIntervalMs(static_cast<std::uint64_t>(std::stoull(interval_token)));
                } catch (...) {
                    log_and_print(log_file, "[WAL] invalid normal interval: %s\n", interval_token.c_str());
                    return true;
                }
            }
        } else if (lower == "full") {
            st.txn.setWalSyncMode(newdb::WalSyncMode::Full);
        } else {
            log_and_print(log_file, "[WAL] usage: WALSYNC [full|normal [interval_ms]|off]\n");
            return true;
        }
        log_and_print(log_file, "[WAL] sync mode set to %s (normal_interval_ms=%llu)\n",
                      lower.c_str(),
                      static_cast<unsigned long long>(st.txn.walNormalSyncIntervalMs()));
        return true;
    }
    return false;
}

bool handle_query_page_command(ShellState& st,
                               const char* line,
                               const char* log_file,
                               const std::string& eff_data,
                               newdb::HeapTable& tbl) {
    if (strncasecmp_ascii(line, "PAGE", 4) != 0) {
        return false;
    }
    std::vector<std::string> args;
    if (!parse_comma_args(line + 4, args) || args.size() < 2 || args.size() > 4) {
        log_and_print(log_file,
                      "[PAGE] usage: PAGE(page, page_size, [order_key], [desc])\n");
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
    if (args.size() >= 3 && !args[2].empty()) {
        order_key = args[2];
    }
    if (args.size() == 4) {
        if (args[3].size() == 4 && strcasecmp_ascii(args[3].c_str(), "desc") == 0) {
            desc = true;
        }
    }
    const std::vector<std::size_t> sorted_idx = load_or_build_page_index_sidecar(
        PageSidecarRequest{
            .data_file = eff_data,
            .table_name = st.session.table_name,
            .order_key = order_key,
            .descending = desc,
        },
        st.session.schema,
        tbl);
    log_and_print(log_file,
                  "[PAGE] table=%s order_by=%s %s\n",
                  tbl.name.c_str(),
                  order_key.c_str(),
                  desc ? "desc" : "asc");
    table_view::print_page_indexed(st.session.schema,
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
            eff_data, key_attr, proj_attr, key_value, limit, st.session.schema, tbl);
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
        const std::vector<std::size_t> matched_idx = query_with_index(tbl, st.session.schema, conds, &st.where_ctx);
        if (where_policy_last_blocked(&st.where_ctx)) {
            log_and_print(log_file, "[WHERE] blocked: %s\n", where_policy_last_message(&st.where_ctx).c_str());
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
        table_view::print_page_indexed(st.session.schema, tbl, matched_idx, 1, page_size);
        if (matched_idx.size() > page_size) {
            log_and_print(log_file,
                          "[WHERE] showing first %zu rows.\n",
                          page_size);
        }
        return true;
    }

    if (strcasecmp_ascii(line, "COUNT") == 0) {
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
                eff_data, conds[0].attr, "__count__", conds[0].value, st.session.schema, tbl);
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
        const std::vector<std::size_t> candidates = build_candidate_slots(tbl, st.session.schema, conds, &st.where_ctx);
        if (where_policy_last_blocked(&st.where_ctx)) {
            log_and_print(log_file, "[COUNT] blocked: %s\n", where_policy_last_message(&st.where_ctx).c_str());
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
        newdb::AttrType tp = st.session.schema.type_of(attr);
        if (!(tp == newdb::AttrType::Int || tp == newdb::AttrType::Float || tp == newdb::AttrType::Double)) {
            log_and_print(log_file,
                          "[MIN] attribute '%s' is not numeric (int/float/double)\n",
                          attr.c_str());
            return true;
        }
        bool has_val = false;
        std::string best;
        const std::vector<std::size_t> candidates = build_candidate_slots(tbl, st.session.schema, conds, &st.where_ctx);
        if (where_policy_last_blocked(&st.where_ctx)) {
            log_and_print(log_file, "[MIN] blocked: %s\n", where_policy_last_message(&st.where_ctx).c_str());
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
                int cmp = st.session.schema.compare_attr(attr, vstr, best);
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
        newdb::AttrType tp = st.session.schema.type_of(attr);
        if (!(tp == newdb::AttrType::Int || tp == newdb::AttrType::Float || tp == newdb::AttrType::Double)) {
            log_and_print(log_file,
                          "[MAX] attribute '%s' is not numeric (int/float/double)\n",
                          attr.c_str());
            return true;
        }
        bool has_val = false;
        std::string best;
        const std::vector<std::size_t> candidates = build_candidate_slots(tbl, st.session.schema, conds, &st.where_ctx);
        if (where_policy_last_blocked(&st.where_ctx)) {
            log_and_print(log_file, "[MAX] blocked: %s\n", where_policy_last_message(&st.where_ctx).c_str());
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
                int cmp = st.session.schema.compare_attr(attr, vstr, best);
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

bool handle_ddl_create_use_commands(ShellState& st,
                                    const char* line,
                                    const char* log_file,
                                    const std::string& eff_data,
                                    std::string& current_table,
                                    std::string& current_file) {
    // CREATE TABLE(name)
    if (strncasecmp_ascii(line, "CREATE TABLE", 12) == 0) {
        std::vector<std::string> args;
        if (!parse_comma_args(line + 12, args) || args.size() != 1) {
            log_and_print(log_file,
                          "[CREATE] usage: CREATE TABLE(name)\n");
            return true;
        }
        std::string new_table = args[0];
        std::string new_file  = new_table + ".bin";
        if (!st.session.schema.valid_primary_key()) {
            st.session.schema.primary_key = "id";
        }
        std::vector<newdb::Row> empty_rows;
        const std::string abs_new = resolve_table_file(st, new_file);
        if (newdb::io::create_heap_file(abs_new.c_str(), empty_rows)) {
            newdb::save_schema_text(newdb::schema_sidecar_path_for_data_file(abs_new), st.session.schema);
            current_table = new_table;
            current_file  = new_file;
            reload_schema_from_data_path(st, new_file);
            shell_invalidate_session_table(st);
            log_and_print(log_file,
                          "[CREATE] table '%s' created, file=%s\n",
                          current_table.c_str(), st.session.data_path.c_str());
        } else {
            log_and_print(log_file,
                          "[CREATE] failed to create table '%s'\n",
                          new_table.c_str());
        }
        return true;
    }

    // USE(name)
    if (strncasecmp_ascii(line, "USE", 3) == 0) {
        std::vector<std::string> args;
        if (!parse_comma_args(line + 3, args) || args.size() != 1) {
            log_and_print(log_file, "[USE] usage: USE(name)\n");
            return true;
        }
        std::string new_table = args[0];
        std::string new_file  = new_table + ".bin";
        if (!current_table.empty() && !current_file.empty() &&
            new_table == current_table && new_file == current_file) {
            log_and_print(log_file,
                          "[USE] already using table '%s' (file=%s)\n",
                          current_table.c_str(), eff_data.c_str());
            return true;
        }
        const std::string try_path = resolve_table_file(st, new_file);
        FILE* tf = std::fopen(try_path.c_str(), "rb");
        if (!tf) {
            log_and_print(log_file,
                          "[USE] table '%s' not found (file=%s)\n",
                          new_table.c_str(), try_path.c_str());
        } else {
            std::fclose(tf);
            current_table = new_table;
            current_file  = new_file;
            st.session.schema = newdb::TableSchema{};
            reload_schema_from_data_path(st, current_file);
            shell_invalidate_session_table(st);
            log_and_print(log_file,
                          "[USE] now using table '%s' (file=%s)\n",
                          current_table.c_str(), effective_data_path(st).c_str());
        }
        return true;
    }
    return false;
}

bool handle_dml_insert_command(ShellState& st,
                               const char* line,
                               const char* log_file,
                               const std::string& eff_data,
                               const std::string& current_table,
                               const std::string& current_file) {
    if (strncasecmp_ascii(line, "BULKINSERT", 10) == 0) {
        const bool fast_mode = (strncasecmp_ascii(line, "BULKINSERTFAST", 14) == 0);
        const char* bulk_name = fast_mode ? "BULKINSERTFAST" : "BULKINSERT";
        const char* arg_start = fast_mode ? (line + 14) : (line + 10);
        if (current_file.empty()) {
            log_and_print(log_file,
                          "[%s] no table selected. Use CREATE TABLE or USE first.\n", bulk_name);
            return true;
        }
        newdb::HeapTable* tbl_ptr = get_cached_table(st);
        if (!tbl_ptr) {
            return true;
        }
        newdb::HeapTable& tbl = *tbl_ptr;
        const newdb::Status ist = newdb_materialize_heap_if_lazy(tbl, st.session.schema);
        if (!ist.ok) {
            log_and_print(log_file, "[%s] %s\n", bulk_name, ist.message.c_str());
            return true;
        }
        std::vector<std::string> args;
        if (!parse_comma_args(arg_start, args) || args.size() < 2 || args.size() > 3) {
            log_and_print(log_file, "[%s] usage: %s(start_id,count[,dept])\n", bulk_name, bulk_name);
            return true;
        }
        int start_id = 0;
        std::size_t count = 0;
        try {
            start_id = std::stoi(args[0]);
            count = static_cast<std::size_t>(std::stoull(args[1]));
        } catch (...) {
            log_and_print(log_file, "[%s] start_id/count must be integers.\n", bulk_name);
            return true;
        }
        if (count == 0) {
            log_and_print(log_file, "[%s] count must be > 0.\n", bulk_name);
            return true;
        }
        const std::string fixed_dept = (args.size() == 3) ? args[2] : std::string{};
        auto gen_attr = [&](const newdb::AttrMeta& meta, const int id) -> std::string {
            if (!st.session.schema.primary_key.empty() &&
                st.session.schema.primary_key != "id" &&
                meta.name == st.session.schema.primary_key) {
                if (meta.type == newdb::AttrType::Int) return std::to_string(id);
                return meta.name + "_" + std::to_string(id);
            }
            if (meta.name == "name") return "u" + std::to_string(id);
            if (meta.name == "dept") {
                if (!fixed_dept.empty()) return fixed_dept;
                switch (id % 4) {
                case 0: return "ENG";
                case 1: return "FIN";
                case 2: return "OPS";
                default: return "HR";
                }
            }
            if (meta.name == "age") return std::to_string(20 + (id % 40));
            if (meta.name == "salary") return std::to_string(8000 + (id % 50000));
            switch (meta.type) {
            case newdb::AttrType::Int: return std::to_string(id % 1000000);
            case newdb::AttrType::Float:
            case newdb::AttrType::Double: return std::to_string((id % 10000) / 10.0);
            case newdb::AttrType::Bool: return (id % 2 == 0) ? "1" : "0";
            case newdb::AttrType::Date: return get_current_date_str();
            case newdb::AttrType::DateTime:
            case newdb::AttrType::Timestamp: return get_current_datetime_str();
            case newdb::AttrType::Char: return "A";
            default: return meta.name + "_" + std::to_string(id);
            }
        };

        const auto begin_t = std::chrono::steady_clock::now();
        std::size_t ok = 0;
        std::size_t dup = 0;
        std::size_t failed = 0;
        std::vector<newdb::Row> pending_rows;
        pending_rows.reserve(count);
        bool can_skip_duplicate_check = false;
        if (fast_mode && st.session.schema.primary_key == "id") {
            can_skip_duplicate_check = true;
            for (std::size_t i = 0; i < count; ++i) {
                const int id = start_id + static_cast<int>(i);
                if (tbl.index_by_id.find(id) != tbl.index_by_id.end()) {
                    can_skip_duplicate_check = false;
                    break;
                }
            }
        }
        for (std::size_t i = 0; i < count; ++i) {
            const int id = start_id + static_cast<int>(i);
            if (!can_skip_duplicate_check) {
                if (tbl.index_by_id.find(id) != tbl.index_by_id.end()) {
                    ++dup;
                    continue;
                }
            }
            newdb::Row row;
            row.id = id;
            if (st.session.schema.attrs.empty()) {
                row.attrs["name"] = "u" + std::to_string(id);
                row.attrs["balance"] = std::to_string(1000 + (id % 10000));
            } else {
                for (const auto& meta : st.session.schema.attrs) {
                    row.attrs[meta.name] = gen_attr(meta, id);
                }
            }
            if (st.session.schema.primary_key != "id") {
                const auto itpk = row.attrs.find(st.session.schema.primary_key);
                if (itpk == row.attrs.end() ||
                    tbl.primary_key_value_exists(st.session.schema, st.session.schema.primary_key, itpk->second, 0)) {
                    ++dup;
                    continue;
                }
            }
            if (st.txn.inTransaction()) {
                std::string conflict_reason;
                if (!st.txn.tryReserveWriteKey(current_table, id, &conflict_reason)) {
                    ++failed;
                    continue;
                }
            }
            pending_rows.push_back(std::move(row));
        }
        const auto prep_done_t = std::chrono::steady_clock::now();
        if (!pending_rows.empty()) {
            if (newdb::io::append_rows(eff_data.c_str(), pending_rows).failed()) {
                failed = pending_rows.size();
            } else {
                const auto io_done_t = std::chrono::steady_clock::now();
                for (const auto& row : pending_rows) {
                    tbl.rows.push_back(row);
                    fast_index_insert(tbl, st.session.schema, row, tbl.rows.size() - 1);
                    if (st.txn.inTransaction()) {
                        st.txn.recordOperation("INSERT", current_table, std::to_string(row.id), "", "");
                    }
                    ++ok;
                }
                const auto mem_done_t = std::chrono::steady_clock::now();
                const auto prep_ms = std::chrono::duration_cast<std::chrono::milliseconds>(prep_done_t - begin_t).count();
                const auto io_ms = std::chrono::duration_cast<std::chrono::milliseconds>(io_done_t - prep_done_t).count();
                const auto mem_ms = std::chrono::duration_cast<std::chrono::milliseconds>(mem_done_t - io_done_t).count();
                const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(mem_done_t - begin_t).count();
                if (ok > 0) {
                    invalidate_eq_sidecars_after_write(eff_data);
                }
                log_and_print(log_file,
                              "[%s] table='%s' start_id=%d count=%zu ok=%zu dup=%zu failed=%zu prep_ms=%lld io_ms=%lld mem_ms=%lld elapsed_ms=%lld\n",
                              bulk_name,
                              current_table.c_str(),
                              start_id,
                              count,
                              ok,
                              dup,
                              failed,
                              static_cast<long long>(prep_ms),
                              static_cast<long long>(io_ms),
                              static_cast<long long>(mem_ms),
                              static_cast<long long>(elapsed_ms));
                return true;
            }
        }
        const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - begin_t).count();
        log_and_print(log_file,
                      "[%s] table='%s' start_id=%d count=%zu ok=%zu dup=%zu failed=%zu prep_ms=%lld io_ms=%d mem_ms=%d elapsed_ms=%lld\n",
                      bulk_name,
                      current_table.c_str(),
                      start_id,
                      count,
                      ok,
                      dup,
                      failed,
                      static_cast<long long>(std::chrono::duration_cast<std::chrono::milliseconds>(prep_done_t - begin_t).count()),
                      0,
                      0,
                      static_cast<long long>(elapsed_ms));
        return true;
    }

    if (strncasecmp_ascii(line, "INSERT", 6) != 0) {
        return false;
    }
    if (current_file.empty()) {
        log_and_print(log_file,
                      "[INSERT] no table selected. Use CREATE TABLE or USE first.\n");
        return true;
    }

    newdb::HeapTable* tbl_ptr = get_cached_table(st);
    if (!tbl_ptr) {
        return true;
    }
    refresh_schema_if_missing(st, eff_data);
    newdb::HeapTable& tbl = *tbl_ptr;
    const newdb::Status ist = newdb_materialize_heap_if_lazy(tbl, st.session.schema);
    if (!ist.ok) {
        log_and_print(log_file, "[INSERT] %s\n", ist.message.c_str());
        return true;
    }

    int id = 0;
    newdb::Row row;
    std::vector<std::string> args;
    if (!parse_comma_args(line + 6, args) || args.empty()) {
        log_and_print(log_file,
                      "[INSERT] usage: INSERT(id, v1, v2, ...)  # with DEFATTR\n");
        return true;
    }
    try {
        id = std::stoi(args[0]);
    } catch (...) {
        log_and_print(log_file,
                      "[INSERT] first argument must be integer id.\n");
        return true;
    }
    row.id = id;
    if (st.session.schema.attrs.empty()) {
        if (args.size() != 3) {
            log_and_print(log_file,
                          "[INSERT] usage without DEFATTR: INSERT(id, name, balance)\n");
            return true;
        }
        try {
            (void)std::stoll(args[2]);
        } catch (...) {
            log_and_print(log_file,
                          "[INSERT] attribute 'balance' expects int, got '%s'\n",
                          args[2].c_str());
            return true;
        }
        row.attrs["name"] = args[1];
        row.attrs["balance"] = args[2];
    } else {
        std::vector<std::string> values;
        for (std::size_t i = 1; i < args.size(); ++i) {
            values.push_back(args[i]);
        }
        for (std::size_t i = 0; i < st.session.schema.attrs.size(); ++i) {
            std::string val = "0";
            if (i < values.size()) {
                val = values[i];
            }
            const newdb::AttrMeta& meta = st.session.schema.attrs[i];
            if (meta.type == newdb::AttrType::Date && (val.empty() || val == "0")) {
                val = get_current_date_str();
            } else if ((meta.type == newdb::AttrType::DateTime || meta.type == newdb::AttrType::Timestamp)
                       && (val.empty() || val == "0")) {
                val = get_current_datetime_str();
            }
            if (!validate_typed_attr_value("INSERT", log_file, meta, val)) {
                return true;
            }
            row.attrs[meta.name] = val;
        }
    }

    if (tbl.find_by_id(id) != nullptr) {
        log_and_print(log_file,
                      "[INSERT] duplicate id=%d in table '%s', insert rejected.\n",
                      id, current_table.c_str());
        return true;
    }
    if (st.session.schema.primary_key != "id") {
        auto itpk = row.attrs.find(st.session.schema.primary_key);
        if (itpk == row.attrs.end()) {
            log_and_print(log_file,
                          "[INSERT] primary key '%s' missing for id=%d\n",
                          st.session.schema.primary_key.c_str(), id);
            return true;
        }
        if (tbl.primary_key_value_exists(st.session.schema, st.session.schema.primary_key, itpk->second, 0)) {
            log_and_print(log_file,
                          "[INSERT] duplicate primary key %s=%s, insert rejected.\n",
                          st.session.schema.primary_key.c_str(), itpk->second.c_str());
            return true;
        }
    }
    if (st.txn.inTransaction()) {
        std::string conflict_reason;
        if (!st.txn.tryReserveWriteKey(current_table, id, &conflict_reason)) {
            log_and_print(log_file, "[INSERT] %s\n", conflict_reason.c_str());
            return true;
        }
    }
    if (newdb::io::append_row(eff_data.c_str(), row).failed()) {
        log_and_print(log_file,
                      "[INSERT] failed to append row for table '%s'.\n",
                      current_table.c_str());
        return true;
    }
    invalidate_eq_sidecars_after_write(eff_data);
    if (st.txn.inTransaction()) {
        st.txn.recordOperation("INSERT", current_table, std::to_string(id), "", "");
    }
    tbl.rows.push_back(row);
    fast_index_insert(tbl, st.session.schema, row, tbl.rows.size() - 1);
    log_and_print(log_file,
                  "[INSERT] ok: table='%s' now has %zu rows.\n",
                  current_table.c_str(), tbl.rows.size());
    return true;
}

bool handle_schema_key_command(ShellState& st,
                               const char* line,
                               const char* log_file,
                               const std::string& eff_data,
                               newdb::HeapTable& tbl) {
    if (strncasecmp_ascii(line, "SET PRIMARY KEY", 15) != 0) {
        return false;
    }
    std::vector<std::string> args;
    if (!parse_comma_args(line + 15, args) || args.size() != 1) {
        log_and_print(log_file, "[KEY] usage: SET PRIMARY KEY(key)\n");
        return true;
    }
    std::string key = args[0];
    if (key.empty()) {
        log_and_print(log_file, "[KEY] key cannot be empty.\n");
        return true;
    }
    if (key == "id") {
        st.session.schema.primary_key = "id";
        newdb::save_schema_text(newdb::schema_sidecar_path_for_data_file(eff_data), st.session.schema);
        log_and_print(log_file, "[KEY] primary key set to id\n");
        return true;
    }
    if (find_attr_meta(st.session.schema, key) == nullptr) {
        log_and_print(log_file,
                      "[KEY] unknown attribute '%s'. Use DEFATTR first.\n",
                      key.c_str());
        return true;
    }
    std::set<std::string> seen;
    newdb::Row r;
    for (std::size_t ri = 0; ri < tbl.logical_row_count(); ++ri) {
        if (tbl.is_heap_storage_backed()) {
            if (!tbl.decode_heap_slot(ri, r)) {
                log_and_print(log_file, "[KEY] failed to read row at slot %zu\n", ri);
                return true;
            }
        } else {
            r = tbl.rows[ri];
        }
        auto it = r.attrs.find(key);
        if (it == r.attrs.end()) {
            log_and_print(log_file,
                          "[KEY] cannot set primary key to '%s': row(id=%d) missing this attribute\n",
                          key.c_str(), r.id);
            return true;
        }
        const std::string& v = it->second;
        if (seen.find(v) != seen.end()) {
            log_and_print(log_file,
                          "[KEY] cannot set primary key to '%s': duplicate value '%s'\n",
                          key.c_str(), v.c_str());
            return true;
        }
        seen.insert(v);
    }
    st.session.schema.primary_key = key;
    newdb::save_schema_text(newdb::schema_sidecar_path_for_data_file(eff_data), st.session.schema);
    log_and_print(log_file, "[KEY] primary key set to %s\n", st.session.schema.primary_key.c_str());
    return true;
}

bool handle_dml_update_delete_commands(ShellState& st,
                                       const char* line,
                                       const char* log_file,
                                       const std::string& eff_data,
                                       const std::string& current_table,
                                       newdb::HeapTable& tbl) {
    if (strncasecmp_ascii(line, "UPDATE", 6) == 0) {
        std::vector<std::string> args;
        if (!parse_comma_args(line + 6, args) || args.empty()) {
            log_and_print(log_file,
                          "[UPDATE] usage: UPDATE(id, name, balance) or UPDATE(id, v1, v2, ...) with DEFATTR\n");
            return true;
        }
        int id = 0;
        try {
            id = std::stoi(args[0]);
        } catch (...) {
            log_and_print(log_file, "[UPDATE] first argument must be integer id.\n");
            return true;
        }
        refresh_schema_if_missing(st, eff_data);
        const newdb::Status ust = newdb_materialize_heap_if_lazy(tbl, st.session.schema);
        if (!ust.ok) {
            log_and_print(log_file, "[UPDATE] %s\n", ust.message.c_str());
            return true;
        }
        newdb::Row* target = nullptr;
        std::size_t target_index = 0;
        for (std::size_t i = 0; i < tbl.rows.size(); ++i) {
            if (tbl.rows[i].id == id) {
                target = &tbl.rows[i];
                target_index = i;
                break;
            }
        }
        if (!target) {
            log_and_print(log_file, "[UPDATE] id=%d not found in table '%s'\n", id, current_table.c_str());
            return true;
        }
        newdb::Row new_row = *target;
        if (st.session.schema.attrs.empty()) {
            if (args.size() != 3) {
                log_and_print(log_file, "[UPDATE] usage without DEFATTR: UPDATE(id, name, balance)\n");
                return true;
            }
            try {
                (void)std::stoll(args[2]);
            } catch (...) {
                log_and_print(log_file,
                              "[UPDATE] attribute 'balance' expects int, got '%s'\n",
                              args[2].c_str());
                return true;
            }
            new_row.attrs["name"] = args[1];
            new_row.attrs["balance"] = args[2];
        } else {
            if (args.size() < 1 + st.session.schema.attrs.size()) {
                log_and_print(log_file, "[UPDATE] with DEFATTR need id + %zu values, got %zu\n",
                              st.session.schema.attrs.size(), args.size());
                return true;
            }
            for (std::size_t i = 0; i < st.session.schema.attrs.size(); ++i) {
                std::string val = args[1 + i];
                const newdb::AttrMeta& meta = st.session.schema.attrs[i];
                if (meta.type == newdb::AttrType::Date && (val == "now" || val == "NOW")) {
                    val = get_current_date_str();
                } else if ((meta.type == newdb::AttrType::DateTime || meta.type == newdb::AttrType::Timestamp)
                           && (val == "now" || val == "NOW")) {
                    val = get_current_datetime_str();
                }
                if (!validate_typed_attr_value("UPDATE", log_file, meta, val)) {
                    return true;
                }
                new_row.attrs[meta.name] = val;
            }
        }
        if (st.session.schema.primary_key != "id") {
            auto itpk = new_row.attrs.find(st.session.schema.primary_key);
            if (itpk == new_row.attrs.end()) {
                log_and_print(log_file, "[UPDATE] primary key '%s' missing for id=%d\n",
                              st.session.schema.primary_key.c_str(), target->id);
                return true;
            }
            if (tbl.primary_key_value_exists(st.session.schema, st.session.schema.primary_key, itpk->second, new_row.id)) {
                log_and_print(log_file, "[UPDATE] duplicate primary key %s=%s, update rejected.\n",
                              st.session.schema.primary_key.c_str(), itpk->second.c_str());
                return true;
            }
        }
        if (st.txn.inTransaction()) {
            std::string conflict_reason;
            if (!st.txn.tryReserveWriteKey(current_table, id, &conflict_reason)) {
                log_and_print(log_file, "[UPDATE] %s\n", conflict_reason.c_str());
                return true;
            }
        }
        if (newdb::io::append_row(eff_data.c_str(), new_row).failed()) {
            log_and_print(log_file, "[UPDATE] failed to append row for table '%s'.\n", current_table.c_str());
            return true;
        }
        std::set<std::string> changed_attrs;
        for (const auto& meta : st.session.schema.attrs) {
            const std::string& key = meta.name;
            const auto old_it = target->attrs.find(key);
            const auto new_it = new_row.attrs.find(key);
            const std::string old_v = (old_it == target->attrs.end()) ? std::string{} : old_it->second;
            const std::string new_v = (new_it == new_row.attrs.end()) ? std::string{} : new_it->second;
            if (old_v != new_v) {
                changed_attrs.insert(key);
            }
        }
        if (changed_attrs.empty()) {
            // Schema may be empty (legacy name/balance layout), keep correctness.
            invalidate_eq_sidecars_after_write(eff_data);
        } else {
            invalidate_eq_sidecars_after_write(eff_data, changed_attrs);
        }
        if (st.txn.inTransaction()) {
            std::string old_value;
            for (const auto& kv : target->attrs) old_value += kv.first + "=" + kv.second + ";";
            std::string new_value;
            for (const auto& kv : new_row.attrs) new_value += kv.first + "=" + kv.second + ";";
            st.txn.recordOperation("UPDATE", current_table, std::to_string(id), old_value, new_value);
        }
        const newdb::Row old_row = *target;
        tbl.rows[target_index] = std::move(new_row);
        fast_index_update_slot(tbl, st.session.schema, old_row, tbl.rows[target_index], target_index);
        log_and_print(log_file, "[UPDATE] ok: id=%d updated (table='%s').\n", id, current_table.c_str());
        return true;
    }

    if (strncasecmp_ascii(line, "DELETE(", 7) == 0) {
        std::vector<std::string> args;
        if (!parse_comma_args(line + 6, args) || args.size() != 1) {
            log_and_print(log_file, "[DELETE] usage: DELETE(id)\n");
            return true;
        }
        int id = 0;
        try { id = std::stoi(args[0]); } catch (...) {
            log_and_print(log_file, "[DELETE] argument must be integer id.\n");
            return true;
        }
        const newdb::Status dst = newdb_materialize_heap_if_lazy(tbl, st.session.schema);
        if (!dst.ok) {
            log_and_print(log_file, "[DELETE] %s\n", dst.message.c_str());
            return true;
        }
        std::size_t idx = 0;
        bool found = false;
        for (std::size_t i = 0; i < tbl.rows.size(); ++i) {
            if (tbl.rows[i].id == id) { idx = i; found = true; break; }
        }
        if (!found) {
            log_and_print(log_file, "[DELETE] id=%d not found in table '%s'\n", id, current_table.c_str());
            return true;
        }
        newdb::Row tomb;
        tomb.id = id;
        tomb.attrs["__deleted"] = "1";
        if (st.txn.inTransaction()) {
            std::string conflict_reason;
            if (!st.txn.tryReserveWriteKey(current_table, id, &conflict_reason)) {
                log_and_print(log_file, "[DELETE] %s\n", conflict_reason.c_str());
                return true;
            }
        }
        if (newdb::io::append_row(eff_data.c_str(), tomb).failed()) {
            log_and_print(log_file, "[DELETE] failed to append tombstone for table '%s'.\n", current_table.c_str());
            return true;
        }
        invalidate_eq_sidecars_after_write(eff_data);
        if (st.txn.inTransaction()) {
            std::string old_value;
            for (const auto& kv : tbl.rows[idx].attrs) old_value += kv.first + "=" + kv.second + ";";
            st.txn.recordOperation("DELETE", current_table, std::to_string(id), old_value, "");
        }
        const newdb::Row removed_row = tbl.rows[idx];
        const std::size_t last_idx = tbl.rows.size() - 1;
        std::optional<newdb::Row> moved_row;
        if (idx != last_idx) {
            moved_row = tbl.rows[last_idx];
            tbl.rows[idx] = std::move(tbl.rows[last_idx]);
        }
        tbl.rows.pop_back();
        fast_index_remove_slot(tbl, st.session.schema, removed_row, idx, moved_row);
        log_and_print(log_file, "[DELETE] ok: id=%d removed, rows=%zu (table='%s').\n",
                      id, tbl.rows.size(), current_table.c_str());
        return true;
    }

    if (strncasecmp_ascii(line, "DELETEPK", 8) == 0) {
        std::vector<std::string> args;
        if (!parse_comma_args(line + 8, args) || args.size() != 1) {
            log_and_print(log_file, "[DELETEPK] usage: DELETEPK(value)\n");
            return true;
        }
        std::string pk = st.session.schema.primary_key.empty() ? "id" : st.session.schema.primary_key;
        const std::string& val = args[0];
        const newdb::Status dpst = newdb_materialize_heap_if_lazy(tbl, st.session.schema);
        if (!dpst.ok) {
            log_and_print(log_file, "[DELETEPK] %s\n", dpst.message.c_str());
            return true;
        }
        int id_to_delete = 0;
        std::size_t idx = 0;
        bool found = false;
        if (pk == "id") {
            try { id_to_delete = std::stoi(val); } catch (...) {
                log_and_print(log_file, "[DELETEPK] primary key id expects integer\n");
                return true;
            }
            for (std::size_t i = 0; i < tbl.rows.size(); ++i) {
                if (tbl.rows[i].id == id_to_delete) { idx = i; found = true; break; }
            }
        } else {
            for (std::size_t i = 0; i < tbl.rows.size(); ++i) {
                auto it = tbl.rows[i].attrs.find(pk);
                if (it != tbl.rows[i].attrs.end() && it->second == val) {
                    idx = i;
                    id_to_delete = tbl.rows[i].id;
                    found = true;
                    break;
                }
            }
        }
        if (!found) {
            log_and_print(log_file, "[DELETEPK] not found: %s=%s\n", pk.c_str(), val.c_str());
            return true;
        }
        newdb::Row tomb;
        tomb.id = id_to_delete;
        tomb.attrs["__deleted"] = "1";
        if (st.txn.inTransaction()) {
            std::string conflict_reason;
            if (!st.txn.tryReserveWriteKey(current_table, id_to_delete, &conflict_reason)) {
                log_and_print(log_file, "[DELETEPK] %s\n", conflict_reason.c_str());
                return true;
            }
        }
        if (newdb::io::append_row(eff_data.c_str(), tomb).failed()) {
            log_and_print(log_file, "[DELETEPK] failed to append tombstone for table '%s'.\n", current_table.c_str());
            return true;
        }
        invalidate_eq_sidecars_after_write(eff_data);
        const newdb::Row removed_row = tbl.rows[idx];
        const std::size_t last_idx = tbl.rows.size() - 1;
        std::optional<newdb::Row> moved_row;
        if (idx != last_idx) {
            moved_row = tbl.rows[last_idx];
            tbl.rows[idx] = std::move(tbl.rows[last_idx]);
        }
        tbl.rows.pop_back();
        fast_index_remove_slot(tbl, st.session.schema, removed_row, idx, moved_row);
        log_and_print(log_file, "[DELETEPK] ok: %s=%s removed, rows=%zu (table='%s').\n",
                      pk.c_str(), val.c_str(), tbl.rows.size(), current_table.c_str());
        return true;
    }
    return false;
}

bool handle_export_command(ShellState& st,
                           const char* line,
                           const char* log_file,
                           const std::string& current_table,
                           const std::string& current_file,
                           newdb::HeapTable& tbl) {
    if (strncasecmp_ascii(line, "EXPORT", 6) != 0) {
        return false;
    }
    std::istringstream iss(line);
    std::string cmd;
    std::string fmt;
    iss >> cmd >> fmt;
    std::string rest;
    std::getline(iss, rest);
    while (!rest.empty() && std::isspace((unsigned char)rest.front())) rest.erase(rest.begin());
    std::string out_file = rest;
    if (out_file.empty() && !current_file.empty()) {
        if (fmt == "CSV") out_file = current_table + ".csv";
        else if (fmt == "JSON") out_file = current_table + ".json";
    }
    if (out_file.empty()) {
        log_and_print(log_file, "[EXPORT] error: no output file specified\n");
        return true;
    }
    {
        std::error_code ec;
        std::filesystem::path p(out_file);
        if (!p.is_absolute()) {
            std::filesystem::path base;
            if (!st.data_dir.empty()) {
                base = std::filesystem::absolute(st.data_dir, ec);
            }
            if (base.empty()) {
                base = workspace_directory(st);
            }
            p = base / p;
            out_file = p.lexically_normal().string();
        }
    }
    if (fmt == "CSV") {
        export_table_csv(st.session.schema, tbl, out_file, log_file);
    } else if (fmt == "JSON") {
        export_table_json(st.session.schema, tbl, out_file, log_file);
    } else {
        log_and_print(log_file, "[EXPORT] unknown format '%s'\n", fmt.c_str());
    }
    return true;
}

bool handle_dml_attr_commands(ShellState& st,
                              const char* line,
                              const char* log_file,
                              const std::string& eff_data,
                              const std::string& current_table,
                              newdb::HeapTable& tbl) {
    if (strncasecmp_ascii(line, "SETATTR", 7) == 0) {
        std::vector<std::string> args;
        if (!parse_comma_args(line + 7, args) || args.size() != 3) {
            log_and_print(log_file, "[SETATTR] usage: SETATTR(id, key, value)\n");
            return true;
        }
        int id = 0;
        try { id = std::stoi(args[0]); } catch (...) {
            log_and_print(log_file, "[SETATTR] first argument must be integer id.\n");
            return true;
        }
        const newdb::Status sat = newdb_materialize_heap_if_lazy(tbl, st.session.schema);
        if (!sat.ok) {
            log_and_print(log_file, "[SETATTR] %s\n", sat.message.c_str());
            return true;
        }
        const std::string& key_str = args[1];
        const std::string& value_str = args[2];
        refresh_schema_if_missing(st, eff_data);
        if (key_str == "id") {
            log_and_print(log_file, "[SETATTR] cannot modify primary key 'id'\n");
            return true;
        }
        std::string value_to_set = value_str;
        const newdb::AttrMeta* attr_meta = st.session.schema.find_attr(key_str);
        if (attr_meta != nullptr) {
            if (!validate_typed_attr_value("SETATTR", log_file, *attr_meta, value_to_set)) {
                return true;
            }
            if (attr_meta->type == newdb::AttrType::Date && (value_to_set == "now" || value_to_set == "NOW")) {
                value_to_set = get_current_date_str();
            } else if ((attr_meta->type == newdb::AttrType::DateTime || attr_meta->type == newdb::AttrType::Timestamp)
                       && (value_to_set == "now" || value_to_set == "NOW")) {
                value_to_set = get_current_datetime_str();
            }
        }
        for (std::size_t i = 0; i < tbl.rows.size(); ++i) {
            if (tbl.rows[i].id != id) continue;
            if (st.session.schema.primary_key != "id" && key_str == st.session.schema.primary_key) {
                if (tbl.primary_key_value_exists(st.session.schema, st.session.schema.primary_key, value_to_set, id)) {
                    log_and_print(log_file, "[SETATTR] duplicate primary key %s=%s, update rejected.\n",
                                  st.session.schema.primary_key.c_str(), value_to_set.c_str());
                    return true;
                }
            }
            newdb::Row new_row = tbl.rows[i];
            new_row.attrs[key_str] = value_to_set;
            if (st.txn.inTransaction()) {
                std::string conflict_reason;
                if (!st.txn.tryReserveWriteKey(current_table, id, &conflict_reason)) {
                    log_and_print(log_file, "[SETATTR] %s\n", conflict_reason.c_str());
                    return true;
                }
            }
            if (newdb::io::append_row(eff_data.c_str(), new_row).failed()) {
                log_and_print(log_file, "[SETATTR] failed to append row for table '%s'.\n", current_table.c_str());
                return true;
            }
            invalidate_eq_sidecars_after_write(eff_data, std::set<std::string>{key_str});
            const newdb::Row old_row = tbl.rows[i];
            tbl.rows[i] = std::move(new_row);
            fast_index_update_slot(tbl, st.session.schema, old_row, tbl.rows[i], i);
            log_and_print(log_file, "[SETATTR] ok: id=%d key=%s updated (table='%s').\n",
                          id, key_str.c_str(), current_table.c_str());
            return true;
        }
        log_and_print(log_file, "[SETATTR] id=%d not found in table '%s'\n", id, current_table.c_str());
        return true;
    }

    if (strncasecmp_ascii(line, "DELATTR", 7) == 0) {
        std::vector<std::string> args;
        if (!parse_comma_args(line + 7, args) || args.size() != 1) {
            log_and_print(log_file, "[DELATTR] usage: DELATTR(key)\n");
            return true;
        }
        const std::string& key = args[0];
        if (key == "id") {
            log_and_print(log_file, "[DELATTR] cannot delete primary key 'id'\n");
            return true;
        }
        const newdb::Status dat = newdb_materialize_heap_if_lazy(tbl, st.session.schema);
        if (!dat.ok) {
            log_and_print(log_file, "[DELATTR] %s\n", dat.message.c_str());
            return true;
        }
        std::size_t affected = 0;
        for (auto& r : tbl.rows) {
            auto it = r.attrs.find(key);
            if (it != r.attrs.end()) {
                r.attrs.erase(it);
                ++affected;
            }
        }
        if (affected == 0) {
            log_and_print(log_file, "[DELATTR] key=%s not found in any row\n", key.c_str());
            return true;
        }
        if (!st.session.schema.attrs.empty()) {
            auto a = st.session.schema.attrs;
            a.erase(std::remove_if(a.begin(), a.end(),
                                   [&key](const newdb::AttrMeta& m) { return m.name == key; }),
                    a.end());
            st.session.schema.attrs = std::move(a);
        }
        // Persist schema sidecar so GUI (reads <table>.attr) stays in sync.
        newdb::save_schema_text(newdb::schema_sidecar_path_for_data_file(eff_data), st.session.schema);
        invalidate_eq_sidecars_after_write(eff_data, std::set<std::string>{key});
        log_and_print(log_file, "[DELATTR] ok: key=%s removed from %zu rows (table='%s').\n",
                      key.c_str(), tbl.rows.size(), current_table.c_str());
        return true;
    }

    if (strncasecmp_ascii(line, "RENATTR", 7) == 0) {
        std::vector<std::string> args;
        if (!parse_comma_args(line + 7, args) || args.size() != 2) {
            log_and_print(log_file, "[RENATTR] usage: RENATTR(old, new)\n");
            return true;
        }
        const std::string& oldk = args[0];
        const std::string& newk = args[1];
        if (oldk == "id" || newk == "id") {
            log_and_print(log_file, "[RENATTR] cannot rename primary key 'id'\n");
            return true;
        }
        if (oldk == newk) {
            log_and_print(log_file, "[RENATTR] old and new names are the same, ignored.\n");
            return true;
        }
        const newdb::Status rat = newdb_materialize_heap_if_lazy(tbl, st.session.schema);
        if (!rat.ok) {
            log_and_print(log_file, "[RENATTR] %s\n", rat.message.c_str());
            return true;
        }
        std::size_t affected = 0;
        for (auto& r : tbl.rows) {
            auto it = r.attrs.find(oldk);
            if (it != r.attrs.end()) {
                std::string val = it->second;
                r.attrs.erase(it);
                r.attrs[newk] = val;
                ++affected;
            }
        }
        if (affected == 0) {
            log_and_print(log_file, "[RENATTR] key=%s not found in any row\n", oldk.c_str());
            return true;
        }
        if (!st.session.schema.attrs.empty()) {
            auto a = st.session.schema.attrs;
            for (auto& m : a) if (m.name == oldk) m.name = newk;
            st.session.schema.attrs = std::move(a);
        }
        if (!st.session.schema.primary_key.empty() && st.session.schema.primary_key == oldk) {
            st.session.schema.primary_key = newk;
        }
        newdb::save_schema_text(newdb::schema_sidecar_path_for_data_file(eff_data), st.session.schema);
        invalidate_eq_sidecars_after_write(eff_data, std::set<std::string>{oldk, newk});
        log_and_print(log_file, "[RENATTR] ok: key=%s renamed to %s in %zu rows (table='%s').\n",
                      oldk.c_str(), newk.c_str(), affected, current_table.c_str());
        return true;
    }

    return false;
}

bool handle_query_find_commands(ShellState& st,
                                const char* line,
                                const char* log_file,
                                const std::string& eff_data,
                                newdb::HeapTable& tbl) {
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
        const std::string pk = st.session.schema.primary_key.empty() ? "id" : st.session.schema.primary_key;
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
                                     st.session.schema.default_int_predicate_attr().c_str(),
                                     min_bal);
        return true;
    }
    return false;
}

bool handle_schema_catalog_commands(ShellState& st, const char* line, const char* log_file) {
    if (strncasecmp_ascii(line, "CREATE SCHEMA", 13) == 0) {
        std::vector<std::string> args;
        if (!parse_comma_args(line + 13, args) || args.size() != 1) {
            log_and_print(log_file, "[CREATE SCHEMA] usage: CREATE SCHEMA(schema_name)\n");
            return true;
        }
        const std::string schema_name = args[0];
        if (schema_name.empty()) {
            log_and_print(log_file, "[CREATE SCHEMA] schema name cannot be empty\n");
            return true;
        }
        bool ok = create_schema(st.data_dir, schema_name);
        if (ok) log_and_print(log_file, "[CREATE SCHEMA] schema '%s' created\n", schema_name.c_str());
        else log_and_print(log_file, "[CREATE SCHEMA] failed to create schema '%s' (may already exist)\n", schema_name.c_str());
        return true;
    }
    if (strncasecmp_ascii(line, "DROP SCHEMA", 11) == 0) {
        std::vector<std::string> args;
        if (!parse_comma_args(line + 11, args) || args.size() != 1) {
            log_and_print(log_file, "[DROP SCHEMA] usage: DROP SCHEMA(schema_name)\n");
            return true;
        }
        const std::string schema_name = args[0];
        if (schema_name.empty()) {
            log_and_print(log_file, "[DROP SCHEMA] schema name cannot be empty\n");
            return true;
        }
        std::vector<std::string> tables_in_schema;
        get_tables_in_schema(st.data_dir, schema_name, tables_in_schema);
        if (!tables_in_schema.empty()) {
            log_and_print(log_file, "[DROP SCHEMA] cannot drop schema '%s' - it contains %zu tables\n",
                          schema_name.c_str(), tables_in_schema.size());
            return true;
        }
        bool ok = delete_schema(st.data_dir, schema_name);
        if (ok) log_and_print(log_file, "[DROP SCHEMA] schema '%s' deleted\n", schema_name.c_str());
        else log_and_print(log_file, "[DROP SCHEMA] failed to delete schema '%s'\n", schema_name.c_str());
        return true;
    }
    if (strcasecmp_ascii(line, "LIST SCHEMAS") == 0 || strcasecmp_ascii(line, "SHOW SCHEMAS") == 0) {
        std::vector<std::string> schemas;
        list_schemas(st.data_dir, schemas);
        if (schemas.empty()) {
            log_and_print(log_file, "[LIST SCHEMAS] no schemas found\n");
            return true;
        }
        log_and_print(log_file, "[LIST SCHEMAS] schemas (%zu):\n", schemas.size());
        for (const auto& s : schemas) {
            std::vector<std::string> tables;
            get_tables_in_schema(st.data_dir, s, tables);
            log_and_print(log_file, "  %s  (%zu tables)\n", s.c_str(), tables.size());
        }
        return true;
    }
    return false;
}

bool handle_query_sum_avg_commands(ShellState& st,
                                   const char* line,
                                   const char* log_file,
                                   const std::string& eff_data,
                                   newdb::HeapTable& tbl) {
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
        newdb::AttrType tp = st.session.schema.type_of(attr);
        if (!(tp == newdb::AttrType::Int || tp == newdb::AttrType::Float || tp == newdb::AttrType::Double)) {
            log_and_print(log_file, "[%s] attribute '%s' is not numeric (int/float/double)\n", tag, attr.c_str());
            return true;
        }
        if (conds.size() == 1 && conds[0].op == CondOp::Eq && conds[0].attr != "id") {
            const std::uint64_t before_calls = tbl.decode_heap_slot_calls;
            const CoveringAggLookup cov = lookup_or_build_covering_agg_sidecar(
                eff_data, conds[0].attr, attr, conds[0].value, st.session.schema, tbl);
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
                if (!is_index_friendly_single(conds[i], st.session.schema)) {
                    continue;
                }
                const std::size_t cost = seed_cost_simple(conds[i], st.session.schema);
                if (cost < best_cost) {
                    best_cost = cost;
                    seed_idx = i;
                }
            }
            if (seed_idx < conds.size()) {
                std::vector<WhereCond> seed_cond{conds[seed_idx]};
                const std::vector<std::size_t> seed_slots = build_candidate_slots(tbl, st.session.schema, seed_cond, &st.where_ctx);
                if (where_policy_last_blocked(&st.where_ctx)) {
                    log_and_print(log_file, "[%s] blocked: %s\n", tag, where_policy_last_message(&st.where_ctx).c_str());
                    return true;
                }
                for (const std::size_t slot : seed_slots) {
                    if (!row_at_slot_read(tbl, slot, r)) continue;
                    if (!row_match_multi_conditions(r, st.session.schema, conds)) continue;
                    accumulate_row(r);
                }
                fused_done = true;
            }
        }

        if (!fused_done) {
            const std::vector<std::size_t> candidates = build_candidate_slots(tbl, st.session.schema, conds, &st.where_ctx);
            if (where_policy_last_blocked(&st.where_ctx)) {
                log_and_print(log_file, "[%s] blocked: %s\n", tag, where_policy_last_message(&st.where_ctx).c_str());
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

bool handle_ddl_alter_rename_commands(ShellState& st,
                                      const char* line,
                                      const char* log_file,
                                      const std::string& eff_data,
                                      std::string& current_table,
                                      std::string& current_file) {
    if (strncasecmp_ascii(line, "ALTER TABLE", 11) == 0) {
        std::string full = trim(line);
        std::istringstream iss(full);
        std::string kw1, kw2, tableName, op1, op2;
        iss >> kw1 >> kw2 >> tableName >> op1;
        if (tableName.empty() || op1.empty()) {
            log_and_print(log_file, "[ALTER] usage: ALTER TABLE name SET SCHEMA(schema_name)\n");
            return true;
        }
        if (op1 == "SET") {
            std::string rest;
            std::getline(iss, rest);
            rest = trim(rest);
            // support both "SCHEMA(x)" and "SCHEMA (x)"
            if (rest.rfind("SCHEMA", 0) == 0) {
                std::string schemaPart = trim(rest.substr(6));
                if (!schemaPart.empty() && schemaPart.front() == '(' && schemaPart.back() == ')') {
                    schemaPart = schemaPart.substr(1, schemaPart.size() - 2);
                    const size_t commaPos = schemaPart.find(',');
                    std::string schemaName = (commaPos != std::string::npos) ? schemaPart.substr(commaPos + 1) : schemaPart;
                    schemaName = trim(schemaName);
                    if (schemaName.empty()) {
                        log_and_print(log_file, "[ALTER] usage: ALTER TABLE name SET SCHEMA(schema_name)\n");
                        return true;
                    }
                    reload_schema_from_data_path(st, tableName + ".bin");
                    st.session.schema.table_label = schemaName;
                    newdb::save_schema_text(newdb::schema_sidecar_path_for_data_file(st.session.data_path), st.session.schema);
                    log_and_print(log_file, "[ALTER] table '%s' schema set to '%s'\n", tableName.c_str(), schemaName.c_str());
                    shell_invalidate_session_table(st);
                    return true;
                }
            }
            log_and_print(log_file, "[ALTER] usage: ALTER TABLE name SET SCHEMA(schema_name)\n");
            return true;
        }
        if (op1 == "REMOVE") {
            iss >> op2;
            if (op2 == "SCHEMA") {
                reload_schema_from_data_path(st, tableName + ".bin");
                st.session.schema.table_label = "";
                newdb::save_schema_text(newdb::schema_sidecar_path_for_data_file(st.session.data_path), st.session.schema);
                log_and_print(log_file, "[ALTER] table '%s' schema removed\n", tableName.c_str());
                shell_invalidate_session_table(st);
                return true;
            }
            log_and_print(log_file, "[ALTER] usage: ALTER TABLE name REMOVE SCHEMA\n");
            return true;
        }
        log_and_print(log_file, "[ALTER] unknown subcommand. Use: ALTER TABLE name SET SCHEMA(schema_name) or ALTER TABLE name REMOVE SCHEMA\n");
        return true;
    }

    if (strncasecmp_ascii(line, "RENAME TABLE", 12) == 0) {
        std::vector<std::string> args;
        if (!parse_comma_args(line + 12, args) || args.size() != 1) {
            log_and_print(log_file, "[RENAME] usage: RENAME TABLE(new_name)\n");
            return true;
        }
        const std::string new_table = args[0];
        const std::string new_file = new_table + ".bin";
        const std::string old_abs = eff_data;
        const std::string new_abs = resolve_table_file(st, new_file);
        if (new_abs == old_abs) {
            log_and_print(log_file, "[RENAME] new name is same as current, ignored.\n");
            return true;
        }
        const std::string old_attr = newdb::schema_sidecar_path_for_data_file(old_abs);
        const std::string new_attr = newdb::schema_sidecar_path_for_data_file(new_abs);
        if (std::rename(old_abs.c_str(), new_abs.c_str()) != 0) {
            log_and_print(log_file, "[RENAME] failed to rename file '%s' to '%s'\n", old_abs.c_str(), new_abs.c_str());
            return true;
        }
        if (old_attr != new_attr) {
            (void)std::rename(old_attr.c_str(), new_attr.c_str());
        }
        current_table = new_table;
        current_file = new_file;
        reload_schema_from_data_path(st, new_file);
        shell_invalidate_session_table(st);
        log_and_print(log_file, "[RENAME] table now '%s' (file=%s)\n", current_table.c_str(), st.session.data_path.c_str());
        return true;
    }
    return false;
}

bool handle_schema_show_commands(ShellState& st,
                                 const char* line,
                                 const char* log_file,
                                 const std::string& current_table,
                                 const std::string& current_file) {
    if (strcasecmp_ascii(line, "SHOW ATTR") == 0 || strcasecmp_ascii(line, "DESCRIBE") == 0) {
        if (current_file.empty()) {
            log_and_print(log_file, "[ATTR] no table selected. Use CREATE TABLE or USE first.\n");
            return true;
        }
        if (st.session.schema.attrs.empty()) {
            reload_schema_from_data_path(st, current_file);
        }
        log_and_print(log_file, "[ATTR] table='%s' file=%s\n", current_table.c_str(), current_file.c_str());
        log_and_print(log_file, "  id:int%s\n", (st.session.schema.primary_key == "id" ? "  [PK]" : ""));
        if (st.session.schema.attrs.empty()) {
            log_and_print(log_file, "  (no DEFATTR)\n");
            return true;
        }
        for (const auto& m : st.session.schema.attrs) {
            log_and_print(log_file, "  %s:%s%s\n", m.name.c_str(),
                          newdb::TableSchema::type_name(m.type),
                          (m.name == st.session.schema.primary_key ? "  [PK]" : ""));
        }
        return true;
    }
    if (strcasecmp_ascii(line, "SHOW KEY") == 0 || strcasecmp_ascii(line, "SHOW PRIMARY KEY") == 0) {
        if (current_file.empty()) {
            log_and_print(log_file, "[KEY] no table selected. Use CREATE TABLE or USE first.\n");
            return true;
        }
        log_and_print(log_file, "[KEY] table='%s' primary_key=%s\n", current_table.c_str(),
                      (st.session.schema.primary_key.empty() ? "id" : st.session.schema.primary_key.c_str()));
        return true;
    }
    return false;
}

bool handle_workspace_admin_commands(ShellState& st,
                                     const char* line,
                                     const char* log_file,
                                     const std::string& eff_data,
                                     std::string& current_table,
                                     std::string& current_file) {
    if (strcasecmp_ascii(line, "LIST TABLES") == 0 || strcasecmp_ascii(line, "SHOW TABLES") == 0) {
        namespace fs = std::filesystem;
        std::error_code ec;
        const fs::path cwd = workspace_directory(st);
        std::vector<std::string> tables;
        for (fs::directory_iterator it(cwd, ec), end; !ec && it != end; it.increment(ec)) {
            const fs::directory_entry& ent = *it;
            if (!ent.is_regular_file(ec)) continue;
            fs::path p = ent.path();
            if (p.extension() != ".bin") continue;
            std::string filename = p.filename().string();
            std::string stem = p.stem().string();
            bool looks_like_log = (filename == std::filesystem::path(st.log_file_path).filename().string());
            if (!looks_like_log) {
                const std::string suf = "_log";
                if (stem.size() >= suf.size() && stem.compare(stem.size() - suf.size(), suf.size(), suf) == 0) {
                    looks_like_log = true;
                }
            }
            if (!looks_like_log) tables.push_back(p.stem().string());
        }
        std::sort(tables.begin(), tables.end());
        if (tables.empty()) {
            log_and_print(log_file, "[LIST] no tables (*.bin) in workspace.\n");
            return true;
        }
        log_and_print(log_file, "[LIST] tables (%zu):\n", tables.size());
        for (const auto& t : tables) {
            const std::string df = resolve_table_file(st, t + ".bin");
            const std::string af = newdb::schema_sidecar_path_for_data_file(df);
            std::error_code ec1, ec2;
            bool has_attr = fs::exists(af, ec1);
            std::uintmax_t sz = fs::file_size(df, ec2);
            log_and_print(log_file, "  %s%s  (%s, %zu bytes)%s\n",
                          (t == current_table ? "* " : "  "), t.c_str(), df.c_str(),
                          static_cast<std::size_t>(ec2 ? 0 : sz), (has_attr ? "  [+attr]" : ""));
        }
        return true;
    }
    if (strncasecmp_ascii(line, "DROP TABLE", 10) == 0) {
        std::vector<std::string> args;
        if (!parse_comma_args(line + 10, args) || args.size() != 1) {
            log_and_print(log_file, "[DROP] usage: DROP TABLE(name)\n");
            return true;
        }
        std::string tname = args[0];
        const std::string df = resolve_table_file(st, tname + ".bin");
        const std::string af = newdb::schema_sidecar_path_for_data_file(df);
        namespace fs = std::filesystem;
        std::error_code ec1, ec2;
        bool removed_df = fs::remove(df, ec1);
        bool removed_af = fs::remove(af, ec2);
        if (!removed_df) {
            log_and_print(log_file, "[DROP] table '%s' not found or cannot delete (file=%s)\n", tname.c_str(), df.c_str());
            return true;
        }
        if (tname == current_table) {
            current_table.clear();
            current_file.clear();
            st.session.schema = newdb::TableSchema{};
        }
        log_and_print(log_file, "[DROP] ok: table '%s' dropped (data=%s%s)\n",
                      tname.c_str(), df.c_str(), (removed_af ? ", attr deleted" : ", attr not found"));
        shell_invalidate_session_table(st);
        return true;
    }
    if (strcasecmp_ascii(line, "SHOWLOG") == 0) {
        dump_log_file(log_file);
        return true;
    }
    if (strcasecmp_ascii(line, "RESET") == 0) {
        if (current_file.empty()) {
            log_and_print(log_file, "[RESET] no table selected. Use CREATE TABLE or USE first.\n");
            return true;
        }
        std::vector<newdb::Row> rows;
        if (newdb::io::create_heap_file(eff_data.c_str(), rows)) {
            newdb::save_schema_text(newdb::schema_sidecar_path_for_data_file(eff_data), st.session.schema);
            log_and_print(log_file, "[RESET] empty table '%s' recreated (file=%s).\n", current_table.c_str(), current_file.c_str());
        } else {
            log_and_print(log_file, "[RESET] failed.\n");
        }
        shell_invalidate_session_table(st);
        return true;
    }
    if (strcasecmp_ascii(line, "VACUUM") == 0) {
        if (current_file.empty()) {
            log_and_print(log_file, "[VACUUM] no table selected. Use CREATE TABLE or USE first.\n");
            return true;
        }
        std::size_t rows_after = 0;
        const newdb::Status vst =
            newdb::io::compact_heap_file(eff_data.c_str(), current_table, st.session.schema, &rows_after);
        if (vst.ok) {
            (void)newdb::save_schema_text(newdb::schema_sidecar_path_for_data_file(eff_data), st.session.schema);
            log_and_print(log_file, "[VACUUM] compacted table '%s' (file=%s), rows=%zu.\n",
                          current_table.c_str(), current_file.c_str(), rows_after);
        } else {
            log_and_print(log_file, "[VACUUM] failed to compact table '%s': %s\n",
                          current_table.c_str(),
                          vst.message.c_str());
        }
        shell_invalidate_session_table(st);
        return true;
    }
    if (strcasecmp_ascii(line, "SCAN") == 0) {
        if (current_file.empty()) {
            log_and_print(log_file, "[SCAN] no table selected. Use CREATE TABLE or USE first.\n");
            return true;
        }
        newdb::io::scan_heap_file(eff_data.c_str());
        return true;
    }
    return false;
}

bool handle_import_defattr_commands(ShellState& st,
                                    const char* line,
                                    const char* log_file,
                                    const std::string& eff_data,
                                    const std::string& current_file) {
    if (strncasecmp_ascii(line, "IMPORTDIR", 9) == 0) {
        std::vector<std::string> args;
        if (!parse_comma_args(line + 9, args) || args.size() != 1) {
            log_and_print(log_file, "[IMPORTDIR] usage: IMPORTDIR(path)\n");
            return true;
        }
        if (import_tables_from_directory(args[0].c_str(),
                                         st.data_dir.empty() ? nullptr : st.data_dir.c_str(),
                                         log_file)) {
            log_and_print(log_file, "[IMPORTDIR] done.\n");
        } else {
            log_and_print(log_file, "[IMPORTDIR] failed.\n");
        }
        return true;
    }
    if (strncasecmp_ascii(line, "DEFATTR", 7) == 0) {
        std::vector<newdb::AttrMeta> attrs;
        std::set<std::string> seen_names;
        std::vector<std::string> args;
        if (!parse_comma_args(line + 7, args) || args.empty()) {
            log_and_print(log_file, "[DEFATTR] usage: DEFATTR(name:type, name:type, ...) (excluding 'id')\n");
            return true;
        }
        for (const auto& a : args) {
            auto pos = a.find(':');
            if (pos == std::string::npos || pos == 0 || pos + 1 >= a.size()) {
                log_and_print(log_file, "[DEFATTR] each item must be name:type, e.g. age:int, nickname:string\n");
                attrs.clear();
                break;
            }
            std::string name = a.substr(0, pos);
            std::string type_str = a.substr(pos + 1);
            if (name == "id") continue;
            newdb::AttrType tp = newdb::TableSchema::parse_type(type_str);
            if (tp == newdb::AttrType::Unknown || seen_names.find(name) != seen_names.end()) {
                attrs.clear();
                break;
            }
            seen_names.insert(name);
            attrs.push_back(newdb::AttrMeta{name, tp});
        }
        if (attrs.empty()) {
            log_and_print(log_file, "[DEFATTR] usage: DEFATTR <name:type> [name:type] ... (excluding 'id')\n");
            return true;
        }
        st.session.schema.attrs = std::move(attrs);
        if (!st.session.schema.valid_primary_key()) st.session.schema.primary_key = "id";
        log_and_print(log_file, "[DEFATTR] attributes set to:");
        for (const auto& m : st.session.schema.attrs) {
            log_and_print(log_file, " %s(%s)", m.name.c_str(), newdb::TableSchema::type_name(m.type));
        }
        log_and_print(log_file, "\n");
        if (!current_file.empty()) {
            newdb::save_schema_text(newdb::schema_sidecar_path_for_data_file(eff_data), st.session.schema);
        }
        return true;
    }
    return false;
}

bool handle_session_commands(const char* line, const char* log_file, bool& handled) {
    handled = false;
    if (strcasecmp_ascii(line, "EXIT") == 0) {
        log_and_print(log_file, "bye.\n");
        handled = true;
        return false;
    }
    if (strcasecmp_ascii(line, "HELP") == 0) {
        log_and_print(log_file,
                      "Commands: DEFATTR/CREATE/USE/RENAME/INSERT/BULKINSERT/BULKINSERTFAST/UPDATE/SETATTR/RENATTR/DELATTR/DELETE/DELETEPK/FIND/FINDPK/QBAL/WHERE/COUNT/PAGE/EXPORT/LIST TABLES/SHOW TABLES/DROP TABLE/SHOW ATTR/DESCRIBE/SHOW KEY/SHOW PRIMARY KEY/SET PRIMARY KEY/SHOWLOG/SHOW TUNING/SHOW STORAGE/RESET/SCAN/IMPORTDIR/BEGIN/COMMIT/ROLLBACK/AUTOVACUUM/WALSYNC/HELP/EXIT\n");
        log_and_print(log_file,
                      "CLI: run `newdb_demo --help` for --data-dir, --table, --log-file, --verbose, etc.\n");
        handled = true;
        return true;
    }
    return true;
}

} // namespace

bool process_command_line(ShellState& st, const char* input_line) {
    std::string& current_table = st.session.table_name;
    std::string& current_file = st.session.data_path;
    const char* log_file = st.log_file_path.c_str();
    char line[512];
    std::strncpy(line, input_line, sizeof(line) - 1);
    line[sizeof(line) - 1] = '\0';

    // 去掉尾部换行
    std::size_t len = std::strlen(line);
    while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r')) {
        line[--len] = '\0';
    }
    if (len == 0) return true;

    logging_bind_shell(&st);
    const std::string eff_data = effective_data_path(st);
    append_session_log_line(log_file, line, st.encrypt_log);

    struct ShellHeapGuardClear {
        ShellState* st;
        ~ShellHeapGuardClear() {
            if (st != nullptr) {
                st->session_heap_guard.reset();
            }
        }
    } shell_heap_clear{&st};

    bool session_handled = false;
    const bool keep_going = handle_session_commands(line, log_file, session_handled);
    if (!keep_going) {
        return false;
    }
    if (session_handled) {
        return true;
    }

    // Phase-1 dispatch chain: commands that don't require loaded heap table.
    const std::array<std::function<bool()>, 8> phase1_handlers = {
        [&]() { return handle_txn_commands(st, line, log_file, current_table); },
        [&]() { return handle_workspace_admin_commands(st, line, log_file, eff_data, current_table, current_file); },
        [&]() { return handle_import_defattr_commands(st, line, log_file, eff_data, current_file); },
        [&]() { return handle_schema_catalog_commands(st, line, log_file); },
        [&]() { return handle_ddl_create_use_commands(st, line, log_file, eff_data, current_table, current_file); },
        [&]() { return handle_ddl_alter_rename_commands(st, line, log_file, eff_data, current_table, current_file); },
        [&]() { return handle_schema_show_commands(st, line, log_file, current_table, current_file); },
        [&]() { return handle_dml_insert_command(st, line, log_file, eff_data, current_table, current_file); },
    };
    for (const auto& h : phase1_handlers) {
        if (h()) {
            return true;
        }
    }

    // 其他命令优先使用缓存的表（按当前表文件名区分）；按需装载一次
    newdb::HeapTable* tbl_ptr = get_cached_table(st);
    if (!tbl_ptr) {
        return true;
    }
    newdb::HeapTable& tbl = *tbl_ptr;

    // Phase-2 dispatch chain: commands requiring loaded table cache.
    const std::array<std::function<bool()>, 9> phase2_handlers = {
        [&]() { return handle_schema_key_command(st, line, log_file, eff_data, tbl); },
        [&]() { return handle_query_where_count_commands(st, line, log_file, eff_data, current_table, tbl); },
        [&]() { return handle_dml_update_delete_commands(st, line, log_file, eff_data, current_table, tbl); },
        [&]() { return handle_dml_attr_commands(st, line, log_file, eff_data, current_table, tbl); },
        [&]() { return handle_query_find_commands(st, line, log_file, eff_data, tbl); },
        [&]() { return handle_query_sum_avg_commands(st, line, log_file, eff_data, tbl); },
        [&]() { return handle_query_min_max_commands(st, line, log_file, tbl); },
        [&]() { return handle_query_page_command(st, line, log_file, eff_data, tbl); },
        [&]() { return handle_export_command(st, line, log_file, current_table, current_file, tbl); },
    };
    for (const auto& h : phase2_handlers) {
        if (h()) {
            return true;
        }
    }

    log_and_print(log_file, "[ERR] unknown command. Type HELP.\n");
    return true;
}
