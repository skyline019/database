#include <waterfall/config.h>

#include "cli/modules/txn/coordinator/txn_manager.h"
#include "cli/modules/txn/coordinator/txn_coordinator_state.h"
#include "cli/modules/txn/coordinator/internal/txn_internal.h"
#include "cli/modules/txn/coordinator/recovery/recovery_analyze.h"
#include "cli/modules/txn/coordinator/recovery/recovery_redo.h"
#include "cli/modules/txn/coordinator/recovery/recovery_undo.h"
#include "cli/modules/txn/coordinator/recovery/recovery_finalize.h"
#include "cli/modules/common/logging/logging.h"
#include "cli/modules/common/util/constants.h"
#include "cli/modules/sidecar/common/sidecar_wal_lsn.h"

#include <newdb/heap_table.h>
#include <newdb/page_io.h>
#include <newdb/schema_io.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
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
            if (!wr.has_record_ts_ms) {
                continue;
            }
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

    RecoveryAnalyzeOutput analyzed;
    recovery_analyze_wal_records(recs, recover_target_lsn, analyzed);
    const std::size_t dangling_count = recovery_count_dangling_ops(analyzed);

    st_->m_wal_recovery_dangling_txns.store(static_cast<std::uint64_t>(analyzed.dangling_by_txn.size()),
                                           std::memory_order_relaxed);
    st_->m_wal_recovery_redo_plan_pending_txn_count.store(
        static_cast<std::uint64_t>(analyzed.dangling_by_txn.size()), std::memory_order_relaxed);

    if (dangling_count > 0) {
        logging_console_printf("[WAL] Found %zu uncommitted operations, starting recovery...\n", dangling_count);
        st_->m_wal_recovery_undo_ops.fetch_add(static_cast<std::uint64_t>(dangling_count),
                                               std::memory_order_relaxed);
    }

    const auto undo_started_at = std::chrono::steady_clock::now();
    const auto redo_started_at = std::chrono::steady_clock::now();
    std::map<std::string, std::vector<TxnWalOp>> redo_by_table;
    for (const auto& kv : analyzed.committed_by_txn) {
        for (const auto& rec : kv.second) {
            redo_by_table[rec.rec.table_name].push_back(rec);
        }
    }
    const bool strict_redo = recovery_strict_redo_from_env();
    const RecoveryRedoResult redo_stats = recovery_apply_committed_redo(
        redo_by_table,
        [this](const std::string& table_name) { return resolveDataFilePath(table_name); },
        strict_redo);
    const auto redo_elapsed_ms = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - redo_started_at)
            .count());
    st_->m_wal_recovery_redo_ms.store(redo_elapsed_ms, std::memory_order_relaxed);
    st_->m_wal_recovery_apply_conflict_count.store(redo_stats.apply_conflicts, std::memory_order_relaxed);
    st_->m_wal_recovery_checkpoint_begin_count.store(static_cast<std::uint64_t>(analyzed.checkpoint_begin),
                                                     std::memory_order_relaxed);
    st_->m_wal_recovery_checkpoint_end_count.store(static_cast<std::uint64_t>(analyzed.checkpoint_end),
                                                    std::memory_order_relaxed);

    const RecoveryUndoResult undo_stats = recovery_apply_dangling_undo(
        analyzed.dangling_by_txn,
        [this](const std::string& table_name) { return resolveDataFilePath(table_name); });
    st_->m_wal_recovery_undo_chain_fallback_txns.store(undo_stats.chain_fallback_txns, std::memory_order_relaxed);

    const auto undo_elapsed_ms = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - undo_started_at)
            .count());
    st_->m_wal_recovery_undo_ms.store(undo_elapsed_ms, std::memory_order_relaxed);

    const auto finalize_started_at = std::chrono::steady_clock::now();
    std::vector<std::uint64_t> dangling_txn_ids;
    dangling_txn_ids.reserve(analyzed.dangling_by_txn.size());
    for (const auto& kv : analyzed.dangling_by_txn) {
        dangling_txn_ids.push_back(kv.first);
    }
    RecoveryFinalizeCallbacks fin{};
    fin.set_active_txn_id_for_wal = [this](std::uint64_t txn) {
        st_->m_txn_id.store(static_cast<int64_t>(txn));
    };
    fin.write_txn_rollback_recovered = [this]() {
        writeWAL("TXN_ROLLBACK", "", "", "", "recovered");
    };
    fin.write_recovery_done = [this]() { writeWAL("RECOVERY_DONE", "", "", "", ""); };
    fin.flush_wal = [this]() { flushWAL(); };
    recovery_finalize_dangling_txns(dangling_txn_ids, fin);
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
                    if (!wr.has_record_ts_ms) {
                        continue;
                    }
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
