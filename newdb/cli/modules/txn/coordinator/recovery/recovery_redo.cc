#include "cli/modules/txn/coordinator/recovery/recovery_redo.h"

#include <newdb/page_io.h>

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <string>
#include <unordered_map>
#include <unordered_set>

bool recovery_strict_redo_from_env() {
    if (const char* mode = std::getenv("NEWDB_REDO_IDEMPOTENT_MODE")) {
        std::string m = mode;
        for (auto& ch : m) {
            ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
        }
        if (m == "strict") {
            return true;
        }
    }
    if (const char* prof = std::getenv("NEWDB_BENCHMARK_PROFILE")) {
        std::string p = prof;
        for (auto& ch : p) {
            ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
        }
        if (p.find("innodb") != std::string::npos) {
            return true;
        }
    }
    return false;
}

RecoveryRedoResult recovery_apply_committed_redo(
    const std::map<std::string, std::vector<TxnWalOp>>& redo_by_table,
    const std::function<std::string(const std::string& table_name)>& resolve_data_file,
    bool strict_redo) {
    RecoveryRedoResult out{};
    auto upsert_row = [&](const std::string& data_file, const newdb::Row& row) {
        (void)newdb::io::append_row(data_file.c_str(), row);
    };
    std::unordered_set<std::string> redo_guard;
    std::unordered_map<std::uint64_t, std::uint64_t> applied_max_lsn_by_object;

    for (auto& table_kv : redo_by_table) {
        const std::string data_file = resolve_data_file(table_kv.first);
        auto ops = table_kv.second;
        std::sort(ops.begin(), ops.end(), [](const TxnWalOp& a, const TxnWalOp& b) {
            if (a.record_lsn != b.record_lsn) {
                return a.record_lsn < b.record_lsn;
            }
            return a.op_seq < b.op_seq;
        });
        for (const auto& op : ops) {
            if (strict_redo) {
                const auto it = applied_max_lsn_by_object.find(op.db_object_id);
                if (it != applied_max_lsn_by_object.end() && op.record_lsn <= it->second) {
                    ++out.apply_conflicts;
                    continue;
                }
                applied_max_lsn_by_object[op.db_object_id] =
                    std::max(applied_max_lsn_by_object[op.db_object_id], op.record_lsn);
            } else {
                const std::string key =
                    std::to_string(op.db_object_id) + ":" + op.rec.key + ":" + std::to_string(op.record_lsn);
                if (!redo_guard.insert(key).second) {
                    ++out.apply_conflicts;
                    continue;
                }
            }
            if (op.wal_op == newdb::WalOp::DELETE) {
                try {
                    newdb::Row tomb;
                    tomb.id = std::stoi(op.rec.key);
                    tomb.attrs["__deleted"] = "1";
                    upsert_row(data_file, tomb);
                } catch (...) {
                    ++out.apply_conflicts;
                }
            } else if (op.has_after) {
                upsert_row(data_file, op.after_row);
            }
        }
    }
    return out;
}
