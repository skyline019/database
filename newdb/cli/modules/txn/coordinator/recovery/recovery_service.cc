#include <waterfall/config.h>

#include "cli/modules/txn/coordinator/txn_manager.h"
#include "cli/modules/txn/coordinator/txn_coordinator_state.h"
#include "cli/modules/txn/coordinator/internal/txn_internal.h"
#include "cli/modules/common/logging/logging.h"
#include "cli/modules/common/util/constants.h"
#include "cli/modules/sidecar/common/sidecar_wal_lsn.h"

#include <newdb/heap_table.h>
#include <newdb/page_io.h>
#include <newdb/schema_io.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

bool TxnCoordinator::recoverFromWAL() {
    const auto recovery_started_at = std::chrono::steady_clock::now();
    newdb::WalManager* wm = ensureWal();
    if (!wm) {
        return true;
    }
    const auto analyze_started_at = std::chrono::steady_clock::now();
    std::uint64_t recover_target_lsn = 0;
    std::uint64_t recover_target_ts_ms = 0;
    if (const char* raw = std::getenv("NEWDB_RECOVER_TARGET_LSN")) {
        try {
            recover_target_lsn = static_cast<std::uint64_t>(std::stoull(raw));
        } catch (...) {
            recover_target_lsn = 0;
        }
    }
    if (const char* raw = std::getenv("NEWDB_RECOVER_TARGET_TS_MS")) {
        try {
            recover_target_ts_ms = static_cast<std::uint64_t>(std::stoull(raw));
        } catch (...) {
            recover_target_ts_ms = 0;
        }
    }
    newdb::TableSchema schema;
    std::vector<newdb::WalDecodedRecord> recs;
    const newdb::Status rd = wm->read_all_records(schema, recs);
    if (!rd.ok) {
        {
            std::lock_guard<std::mutex> lk(st_->m_wal_recovery_policy_mu);
            st_->m_wal_recovery_policy = std::string("cli_txn_reconcile|read_all_records_failed:") + rd.message;
        }
        return false;
    }
    if (recover_target_lsn == 0 && recover_target_ts_ms != 0) {
        std::uint64_t cutoff = 0;
        for (const auto& wr : recs) {
            if (!wr.has_record_ts_ms) continue;
            if (wr.record_ts_ms <= recover_target_ts_ms) {
                cutoff = std::max(cutoff, wr.lsn);
            }
        }
        recover_target_lsn = cutoff;
    }
    const auto analyze_elapsed_ms = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - analyze_started_at)
            .count());
    st_->m_wal_recovery_records_scanned.store(static_cast<std::uint64_t>(recs.size()), std::memory_order_relaxed);
    st_->m_wal_recovery_analyze_ms.store(analyze_elapsed_ms, std::memory_order_relaxed);
    {
        newdb::WalRecoveryStats scan{};
        std::uint64_t records_after_cp = 0;
        if (wm->capture_recovery_scan_stats(&scan).ok) {
            st_->m_wal_recovery_segments_after_checkpoint.store(scan.indexed_segments, std::memory_order_relaxed);
            if (scan.last_complete_checkpoint_lsn > 0) {
                for (const auto& wr : recs) {
                    if (wr.lsn > scan.last_complete_checkpoint_lsn) {
                        ++records_after_cp;
                    }
                }
            }
            st_->m_wal_recovery_records_after_checkpoint.store(records_after_cp, std::memory_order_relaxed);
        }
    }
    struct TxnWalOp {
        TxnRecord rec;
        std::uint64_t op_seq{0};
        std::uint64_t record_lsn{0};
        std::uint64_t db_object_id{0};
        bool has_before{false};
        newdb::Row before_row;
        bool has_after{false};
        newdb::Row after_row;
        newdb::WalOp wal_op{newdb::WalOp::UPDATE};
    };
    std::unordered_map<uint64_t, std::vector<TxnWalOp>> txn_records;
    std::unordered_set<uint64_t> committed;
    std::unordered_set<uint64_t> rolled_back;
    std::unordered_map<uint64_t, std::uint64_t> partial_cutoff_by_txn;
    std::uint64_t cp_begin = 0;
    std::uint64_t cp_end = 0;
    for (const auto& wr : recs) {
        if (recover_target_lsn > 0 && wr.lsn > recover_target_lsn) {
            continue;
        }
        if (wr.op == newdb::WalOp::COMMIT) {
            committed.insert(wr.txn_id);
            continue;
        }
        if (wr.op == newdb::WalOp::ROLLBACK) {
            rolled_back.insert(wr.txn_id);
            continue;
        }
        if (!wr.has_row) {
            if (wr.op == newdb::WalOp::CHECKPOINT_BEGIN) {
                ++cp_begin;
            } else if (wr.op == newdb::WalOp::CHECKPOINT_END) {
                ++cp_end;
            } else if (wr.op == newdb::WalOp::TXN_ABORT_PARTIAL && wr.has_pitr_target_lsn) {
                partial_cutoff_by_txn[wr.txn_id] = wr.pitr_target_lsn;
            }
            continue;
        }
        // Redo/undo chain: only heap table DML (INSERT/UPDATE/DELETE). Skip SESSION_SNAPSHOT,
        // SAVEPOINT_*, PITR_MARK rows, and any other non-DML logical WAL entries.
        if (wr.op != newdb::WalOp::INSERT && wr.op != newdb::WalOp::UPDATE &&
            wr.op != newdb::WalOp::DELETE) {
            continue;
        }
        TxnWalOp oprec;
        TxnRecord& rec = oprec.rec;
        rec.txn_id = static_cast<int64_t>(wr.txn_id);
        rec.table_name = wr.table;
        rec.key = std::to_string(wr.row.id);
        const auto it_old = wr.row.attrs.find("__wal_old");
        const auto it_new = wr.row.attrs.find("__wal_new");
        const auto it_op = wr.row.attrs.find("__wal_op");
        rec.old_value = (it_old != wr.row.attrs.end()) ? it_old->second : std::string{};
        rec.new_value = (it_new != wr.row.attrs.end()) ? it_new->second : std::string{};
        if (it_op != wr.row.attrs.end()) {
            rec.operation = it_op->second;
        } else if (wr.op == newdb::WalOp::INSERT) {
            rec.operation = "INSERT";
        } else if (wr.op == newdb::WalOp::DELETE) {
            rec.operation = "DELETE";
        } else {
            rec.operation = "UPDATE";
        }
        oprec.op_seq = wr.op_seq_in_txn;
        oprec.record_lsn = wr.lsn;
        if (wr.has_db_object_id) {
            oprec.db_object_id = wr.db_object_id;
        } else {
            oprec.db_object_id = static_cast<std::uint64_t>(std::hash<std::string>{}(wr.table));
        }
        oprec.has_before = wr.has_before_row;
        oprec.before_row = wr.before_row;
        oprec.has_after = wr.has_after_row;
        oprec.after_row = wr.after_row;
        oprec.wal_op = wr.op;
        txn_records[wr.txn_id].push_back(std::move(oprec));
    }

    std::unordered_map<uint64_t, std::vector<TxnWalOp>> dangling_by_txn;
    std::unordered_map<uint64_t, std::vector<TxnWalOp>> committed_by_txn;
    for (auto& kv : txn_records) {
        if (committed.find(kv.first) != committed.end()) {
            committed_by_txn.emplace(kv.first, kv.second);
            continue;
        }
        if (rolled_back.find(kv.first) != rolled_back.end()) {
            continue;
        }
        dangling_by_txn.emplace(kv.first, kv.second);
    }
    for (auto& kv : committed_by_txn) {
        const auto it = partial_cutoff_by_txn.find(kv.first);
        if (it == partial_cutoff_by_txn.end()) continue;
        const std::uint64_t cutoff = it->second;
        auto& vec = kv.second;
        vec.erase(std::remove_if(vec.begin(), vec.end(), [&](const TxnWalOp& x) {
            return x.record_lsn > cutoff;
        }), vec.end());
    }
    for (auto& kv : dangling_by_txn) {
        const auto it = partial_cutoff_by_txn.find(kv.first);
        if (it == partial_cutoff_by_txn.end()) continue;
        const std::uint64_t cutoff = it->second;
        auto& vec = kv.second;
        vec.erase(std::remove_if(vec.begin(), vec.end(), [&](const TxnWalOp& x) {
            return x.record_lsn <= cutoff;
        }), vec.end());
    }

    std::size_t dangling_count = 0;
    for (const auto& kv : dangling_by_txn) {
        dangling_count += kv.second.size();
    }
    st_->m_wal_recovery_dangling_txns.store(static_cast<std::uint64_t>(dangling_by_txn.size()),
                                       std::memory_order_relaxed);
    st_->m_wal_recovery_redo_plan_pending_txn_count.store(static_cast<std::uint64_t>(dangling_by_txn.size()),
                                                     std::memory_order_relaxed);

    if (dangling_count > 0) {
        logging_console_printf("[WAL] Found %zu uncommitted operations, starting recovery...\n", dangling_count);
        st_->m_wal_recovery_undo_ops.fetch_add(static_cast<std::uint64_t>(dangling_count),
                                          std::memory_order_relaxed);
    }

    const auto undo_started_at = std::chrono::steady_clock::now();
    const auto redo_started_at = std::chrono::steady_clock::now();
    std::map<std::string, std::vector<TxnWalOp>> redo_by_table;
    for (const auto& kv : committed_by_txn) {
        for (const auto& rec : kv.second) {
            redo_by_table[rec.rec.table_name].push_back(rec);
        }
    }
    auto upsert_row = [&](const std::string& data_file, const newdb::Row& row) {
        (void)newdb::io::append_row(data_file.c_str(), row);
    };
    const bool strict_redo = [&]() -> bool {
        if (const char* mode = std::getenv("NEWDB_REDO_IDEMPOTENT_MODE")) {
            std::string m = mode;
            for (auto& ch : m) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
            return (m == "strict");
        }
        if (const char* prof = std::getenv("NEWDB_BENCHMARK_PROFILE")) {
            std::string p = prof;
            for (auto& ch : p) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
            if (p.find("innodb") != std::string::npos) return true;
        }
        return false;
    }();
    std::unordered_set<std::string> redo_guard; // lite: full key set
    std::unordered_map<std::uint64_t, std::uint64_t> applied_max_lsn_by_object; // strict: per-object max lsn
    std::uint64_t redo_apply_conflicts = 0;
    for (auto& table_kv : redo_by_table) {
        const std::string data_file = resolveDataFilePath(table_kv.first);
        auto& ops = table_kv.second;
        std::sort(ops.begin(), ops.end(), [](const TxnWalOp& a, const TxnWalOp& b) {
            if (a.record_lsn != b.record_lsn) return a.record_lsn < b.record_lsn;
            return a.op_seq < b.op_seq;
        });
        for (const auto& op : ops) {
            if (strict_redo) {
                const auto it = applied_max_lsn_by_object.find(op.db_object_id);
                if (it != applied_max_lsn_by_object.end() && op.record_lsn <= it->second) {
                    ++redo_apply_conflicts;
                    continue;
                }
                applied_max_lsn_by_object[op.db_object_id] = std::max(applied_max_lsn_by_object[op.db_object_id], op.record_lsn);
            } else {
                const std::string key = std::to_string(op.db_object_id) + ":" + op.rec.key + ":" + std::to_string(op.record_lsn);
                if (!redo_guard.insert(key).second) {
                    ++redo_apply_conflicts;
                    continue;
                }
            }
            if (op.wal_op == newdb::WalOp::DELETE) {
                newdb::Row tomb;
                tomb.id = std::stoi(op.rec.key);
                tomb.attrs["__deleted"] = "1";
                upsert_row(data_file, tomb);
            } else if (op.has_after) {
                upsert_row(data_file, op.after_row);
            }
        }
    }
    const auto redo_elapsed_ms = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - redo_started_at)
            .count());
    st_->m_wal_recovery_redo_ms.store(redo_elapsed_ms, std::memory_order_relaxed);
    st_->m_wal_recovery_apply_conflict_count.store(redo_apply_conflicts, std::memory_order_relaxed);
    st_->m_wal_recovery_checkpoint_begin_count.store(static_cast<std::uint64_t>(cp_begin), std::memory_order_relaxed);
    st_->m_wal_recovery_checkpoint_end_count.store(static_cast<std::uint64_t>(cp_end), std::memory_order_relaxed);

    std::map<std::string, std::vector<TxnWalOp>> ops_by_table;
    for (const auto& kv : dangling_by_txn) {
        for (const auto& rec : kv.second) {
            ops_by_table[rec.rec.table_name].push_back(rec);
        }
    }

    auto parse_attrs = [](const std::string& packed) {
        std::map<std::string, std::string> attrs;
        std::size_t i = 0;
        while (i < packed.size()) {
            const std::size_t sep = packed.find('=', i);
            if (sep == std::string::npos) {
                break;
            }
            const std::size_t end = packed.find(';', sep + 1);
            const std::string key = packed.substr(i, sep - i);
            const std::string val =
                (end == std::string::npos) ? packed.substr(sep + 1) : packed.substr(sep + 1, end - (sep + 1));
            if (!key.empty()) {
                attrs[key] = val;
            }
            if (end == std::string::npos) {
                break;
            }
            i = end + 1;
        }
        return attrs;
    };

    for (auto& table_kv : ops_by_table) {
        const std::string& table_name = table_kv.first;
        std::vector<TxnWalOp>& table_ops = table_kv.second;
        const std::string data_file = resolveDataFilePath(table_name);
        std::sort(table_ops.begin(), table_ops.end(), [](const TxnWalOp& a, const TxnWalOp& b) {
            if (a.record_lsn != b.record_lsn) return a.record_lsn > b.record_lsn;
            return a.op_seq > b.op_seq;
        });

        for (auto it = table_ops.begin(); it != table_ops.end(); ++it) {
            const auto& rec = it->rec;
            if (rec.operation == "TXN_BEGIN") {
                continue;
            }
            try {
                const int id = std::stoi(rec.key);
                if (rec.operation == "INSERT") {
                    newdb::Row tombstone;
                    tombstone.id = id;
                    tombstone.attrs["__deleted"] = "1";
                    (void)newdb::io::append_row(data_file.c_str(), tombstone);
                    logging_console_printf("[WAL] Undo INSERT id=%d on %s\n", id, table_name.c_str());
                } else if (rec.operation == "UPDATE" || rec.operation == "DELETE") {
                    if (it->has_before) {
                        (void)newdb::io::append_row(data_file.c_str(), it->before_row);
                    } else {
                        newdb::Row recovered;
                        recovered.id = id;
                        const auto attrs = parse_attrs(rec.old_value);
                        for (const auto& kv : attrs) {
                            if (kv.first != "__deleted") {
                                recovered.attrs[kv.first] = kv.second;
                            }
                        }
                        (void)newdb::io::append_row(data_file.c_str(), recovered);
                    }
                    logging_console_printf("[WAL] Undo %s id=%d on %s\n",
                                           rec.operation.c_str(),
                                           id,
                                           table_name.c_str());
                }
            } catch (...) {
                logging_console_printf("[WAL] Skip malformed WAL record key=%s\n", rec.key.c_str());
            }
        }
    }
    const auto undo_elapsed_ms = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - undo_started_at)
            .count());
    st_->m_wal_recovery_undo_ms.store(undo_elapsed_ms, std::memory_order_relaxed);

    const auto finalize_started_at = std::chrono::steady_clock::now();
    // ??dangling ???????????????????????????
    for (const auto& kv : dangling_by_txn) {
        const uint64_t txn = kv.first;
        st_->m_txn_id.store(static_cast<int64_t>(txn));
        writeWAL("TXN_ROLLBACK", "", "", "", "recovered");
    }
    // ????????WAL??????????????
    writeWAL("RECOVERY_DONE", "", "", "", "");
    flushWAL();
    const auto finalize_elapsed_ms = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - finalize_started_at)
            .count());
    st_->m_wal_recovery_finalize_ms.store(finalize_elapsed_ms, std::memory_order_relaxed);
    st_->m_wal_recovery_runs.fetch_add(1, std::memory_order_relaxed);
    const auto elapsed_ms = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - recovery_started_at)
            .count());
    st_->m_wal_recovery_last_elapsed_ms.store(elapsed_ms, std::memory_order_relaxed);
    {
        std::ostringstream pol;
        pol << "cli_txn_reconcile";
        if (recover_target_lsn != 0) {
            pol << ";recover_target_lsn=" << recover_target_lsn;
        }
        if (recover_target_ts_ms != 0) {
            pol << ";recover_target_ts_ms=" << recover_target_ts_ms;
        }
        if (const auto ls = wm->last_recovery_stats()) {
            pol << "|heap_policy=" << ls->recovery_policy;
            pol << ";heap_uncommitted_discarded=" << ls->uncommitted_txn_discarded_count;
            pol << ";heap_partial_tail_stops=" << ls->segment_index_partial_tail_stops;
            pol << ";heap_bad_header_stops=" << ls->segment_index_bad_header_stops;
        }
        std::lock_guard<std::mutex> lk(st_->m_wal_recovery_policy_mu);
        st_->m_wal_recovery_policy = pol.str();
    }
    logging_console_printf("[WAL] Recovery complete\n");

    return true;
}

Result<bool> TxnCoordinator::recoverToLsn(const std::uint64_t target_lsn) {
    if (target_lsn == 0) {
        return Result<bool>::Err("invalid target lsn");
    }
#if defined(_WIN32)
    _putenv_s("NEWDB_RECOVER_TARGET_LSN", std::to_string(target_lsn).c_str());
#else
    setenv("NEWDB_RECOVER_TARGET_LSN", std::to_string(target_lsn).c_str(), 1);
#endif
    const auto begin = std::chrono::steady_clock::now();
    const bool ok = recoverFromWAL();
    const auto elapsed_ms = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin).count());
#if defined(_WIN32)
    _putenv_s("NEWDB_RECOVER_TARGET_LSN", "");
#else
    unsetenv("NEWDB_RECOVER_TARGET_LSN");
#endif
    if (!ok) {
        return Result<bool>::Err("recover to lsn failed");
    }
    st_->m_pitr_runs.fetch_add(1, std::memory_order_relaxed);
    st_->m_pitr_target_lsn.store(target_lsn, std::memory_order_relaxed);
    st_->m_pitr_elapsed_ms.store(elapsed_ms, std::memory_order_relaxed);
    writeWAL("PITR_MARK", "", std::to_string(target_lsn), "", std::to_string(newdb::WalManager::wall_clock_ms()));
    return Result<bool>::Ok(true);
}

Result<bool> TxnCoordinator::recoverToTime(const std::uint64_t target_ts_ms) {
    if (target_ts_ms == 0) {
        return Result<bool>::Err("invalid target time");
    }
    std::uint64_t cutoff_lsn = 0;
    {
        std::lock_guard<std::mutex> lk(st_->m_wal_mutex);
        newdb::WalManager* wm = ensureWal();
        if (wm != nullptr) {
            newdb::TableSchema schema;
            std::vector<newdb::WalDecodedRecord> recs;
            const newdb::Status rd = wm->read_all_records(schema, recs);
            if (rd.ok) {
                for (const auto& wr : recs) {
                    if (!wr.has_record_ts_ms) continue;
                    if (wr.record_ts_ms <= target_ts_ms) {
                        cutoff_lsn = std::max(cutoff_lsn, wr.lsn);
                    }
                }
            }
        }
    }
    if (cutoff_lsn == 0) {
        return Result<bool>::Err("no WAL anchor at or before target time");
    }
#if defined(_WIN32)
    _putenv_s("NEWDB_RECOVER_TARGET_LSN", std::to_string(cutoff_lsn).c_str());
#else
    setenv("NEWDB_RECOVER_TARGET_LSN", std::to_string(cutoff_lsn).c_str(), 1);
#endif
    const auto begin = std::chrono::steady_clock::now();
    const bool ok = recoverFromWAL();
    const auto elapsed_ms = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin).count());
#if defined(_WIN32)
    _putenv_s("NEWDB_RECOVER_TARGET_LSN", "");
#else
    unsetenv("NEWDB_RECOVER_TARGET_LSN");
#endif
    if (!ok) {
        return Result<bool>::Err("recover to time failed");
    }
    st_->m_pitr_runs.fetch_add(1, std::memory_order_relaxed);
    st_->m_pitr_target_lsn.store(cutoff_lsn, std::memory_order_relaxed);
    st_->m_pitr_elapsed_ms.store(elapsed_ms, std::memory_order_relaxed);
    writeWAL("PITR_MARK", "", std::to_string(cutoff_lsn), "", std::to_string(target_ts_ms));
    return Result<bool>::Ok(true);
}

// ========== ?? VACUUM ==========



