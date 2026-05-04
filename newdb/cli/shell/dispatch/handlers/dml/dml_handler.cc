#include <waterfall/config.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include <newdb/page_io.h>

#include "cli/modules/sidecar/covering/covering_index_sidecar.h"
#include "cli/shell/dispatch/shared/dispatch_internal.h"
#include "cli/modules/import_export/demo_export.h"
#include "cli/modules/import_export/import.h"
#include "cli/modules/common/logging/logging.h"
#include "cli/modules/sidecar/page/page_index_sidecar.h"
#include "cli/modules/catalog/schema_catalog.h"
#include "cli/shell/state/shell_state.h"
#include "cli/shell/dispatch/services/lsm/lsm_lite_service.h"
#include "cli/modules/common/view/table_view.h"
#include "cli/modules/txn/coordinator/txn_manager.h"
#include "cli/modules/common/util/utils.h"
#include "cli/modules/where/executor/where.h"

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
        const newdb::Status ist = newdb_materialize_heap_if_lazy(tbl, st.session.schema, &st);
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
                    lsm_lite_record_writes(st, eff_data, pending_rows, false);
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
    const newdb::Status ist = newdb_materialize_heap_if_lazy(tbl, st.session.schema, &st);
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
    lsm_lite_record_writes(st, eff_data, std::vector<newdb::Row>{row}, false);
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
        const newdb::Status ust = newdb_materialize_heap_if_lazy(tbl, st.session.schema, &st);
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
        lsm_lite_record_writes(st, eff_data, std::vector<newdb::Row>{tbl.rows[target_index]}, false);
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
        const newdb::Status dst = newdb_materialize_heap_if_lazy(tbl, st.session.schema, &st);
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
        lsm_lite_record_writes(st, eff_data, std::vector<newdb::Row>{tomb}, true);
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
        const newdb::Status dpst = newdb_materialize_heap_if_lazy(tbl, st.session.schema, &st);
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
        lsm_lite_record_writes(st, eff_data, std::vector<newdb::Row>{tomb}, true);
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
        const newdb::Status sat = newdb_materialize_heap_if_lazy(tbl, st.session.schema, &st);
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
        const newdb::Status dat = newdb_materialize_heap_if_lazy(tbl, st.session.schema, &st);
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
        const newdb::Status rat = newdb_materialize_heap_if_lazy(tbl, st.session.schema, &st);
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




