#include <waterfall/config.h>

#include "cli/modules/txn/coordinator/txn_manager.h"
#include "cli/modules/txn/coordinator/txn_coordinator_state.h"
#include "cli/modules/txn/coordinator/internal/txn_internal.h"
#include "cli/modules/common/logging/logging.h"
#include "cli/modules/common/util/constants.h"
#include "cli/modules/sidecar/common/sidecar_wal_lsn.h"

#include <newdb/heap_table.h>
#include <newdb/mvcc.h>
#include <newdb/page_io.h>
#include <newdb/schema_io.h>

#include <cctype>
#include <cstdio>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace {
bool read_u64_env(const char* key, std::uint64_t* out) {
    const char* raw = std::getenv(key);
    if (raw == nullptr || raw[0] == '\0') {
        return false;
    }
    try {
        *out = static_cast<std::uint64_t>(std::stoull(raw));
        return true;
    } catch (...) {
        return false;
    }
}
} // namespace

std::string TxnCoordinator::getWALPath(const std::string& table_name) const {
    namespace fs = std::filesystem;
    const fs::path base = st_->m_workspace_root.empty() ? fs::current_path() : fs::path(st_->m_workspace_root);
    if (table_name.empty()) {
        return (base / (std::string("demodb") + Constants::WAL_FILE_EXT)).lexically_normal().string();
    }
    return (base / (table_name + Constants::WAL_FILE_EXT)).lexically_normal().string();
}


std::string TxnCoordinator::walSyncConfigPath() const {
    namespace fs = std::filesystem;
    const fs::path base = st_->m_workspace_root.empty() ? fs::current_path() : fs::path(st_->m_workspace_root);
    return (base / "demodb.walsync.conf").lexically_normal().string();
}


std::string TxnCoordinator::vacuumConfigPath() const {
    namespace fs = std::filesystem;
    const fs::path base = st_->m_workspace_root.empty() ? fs::current_path() : fs::path(st_->m_workspace_root);
    return (base / "demodb.vacuum.conf").lexically_normal().string();
}


void TxnCoordinator::loadVacuumConfig() {
    std::ifstream in(vacuumConfigPath());
    if (!in) {
        return;
    }
    std::size_t threshold = 0;
    std::size_t min_interval = 0;
    in >> threshold >> min_interval;
    if (threshold > 0) {
        st_->m_vacuum_ops_threshold.store(threshold);
    }
    if (min_interval > 0) {
        st_->m_vacuum_min_interval_sec.store(min_interval);
    }
}


void TxnCoordinator::persistVacuumConfig() const {
    std::ofstream out(vacuumConfigPath(), std::ios::out | std::ios::trunc);
    if (!out) {
        return;
    }
    out << st_->m_vacuum_ops_threshold.load() << " " << st_->m_vacuum_min_interval_sec.load() << "\n";
}


bool TxnCoordinator::loadWalSyncConfig(newdb::WalManager& wm) const {
    std::ifstream in(walSyncConfigPath());
    if (!in) {
        return false;
    }
    std::string mode;
    std::uint64_t interval = wm.normal_sync_interval_ms();
    in >> mode >> interval;
    for (auto& ch : mode) {
        ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    }
    if (mode == "off") {
        wm.set_sync_mode(newdb::WalSyncMode::Off);
    } else if (mode == "normal") {
        wm.set_sync_mode(newdb::WalSyncMode::Normal);
    } else if (mode == "full") {
        wm.set_sync_mode(newdb::WalSyncMode::Full);
    }
    wm.set_normal_sync_interval_ms(interval);
    return true;
}


void TxnCoordinator::persistWalSyncConfig(const newdb::WalManager& wm) const {
    std::ofstream out(walSyncConfigPath(), std::ios::out | std::ios::trunc);
    if (!out) {
        return;
    }
    const char* mode = (wm.sync_mode() == newdb::WalSyncMode::Off) ? "off" :
                       (wm.sync_mode() == newdb::WalSyncMode::Normal) ? "normal" : "full";
    out << mode << " " << wm.normal_sync_interval_ms() << "\n";
}


std::string TxnCoordinator::resolveDataFilePath(const std::string& table_name) const {
    namespace fs = std::filesystem;
    std::error_code ec;
    fs::path p = table_name + Constants::DATA_FILE_EXT;
    if (!st_->m_workspace_root.empty()) {
        p = fs::absolute(st_->m_workspace_root, ec) / p;
    } else {
        p = fs::absolute(p, ec);
    }
    return p.lexically_normal().string();
}


newdb::WalManager* TxnCoordinator::ensureWal() {
    if (!st_->wal_) {
        const std::string ws = st_->m_workspace_root.empty() ? "." : st_->m_workspace_root;
        st_->wal_ = std::make_unique<newdb::WalManager>("demodb", ws);
        const bool loaded = loadWalSyncConfig(*st_->wal_);
        if (!loaded) {
            // Default startup profile for balanced throughput/durability.
            st_->wal_->set_sync_mode(newdb::WalSyncMode::Normal);
            st_->wal_->set_normal_sync_interval_ms(20);
            persistWalSyncConfig(*st_->wal_);
        }
        if (const char* mode = std::getenv("NEWDB_WAL_SYNC")) {
            std::string m = mode;
            for (auto& ch : m) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
            if (m == "off") st_->wal_->set_sync_mode(newdb::WalSyncMode::Off);
            else if (m == "normal") st_->wal_->set_sync_mode(newdb::WalSyncMode::Normal);
            else st_->wal_->set_sync_mode(newdb::WalSyncMode::Full);
        }
        (void)st_->wal_->open();
    }
    return st_->wal_.get();
}


void TxnCoordinator::writeWAL(const std::string& operation, const std::string& table,
                          const std::string& key, const std::string& old_val, const std::string& new_val) {
    std::lock_guard<std::mutex> lk(st_->m_wal_mutex);
    newdb::WalManager* wm = ensureWal();
    if (!wm) {
        return;
    }
    const uint64_t txn = static_cast<uint64_t>(st_->m_txn_id.load());
    const auto wal_begin_t = std::chrono::steady_clock::now();
    const std::uint64_t db_object_id = static_cast<std::uint64_t>(std::hash<std::string>{}(table));

    if (operation == "TXN_BEGIN") {
        (void)wm->begin_transaction(txn);
        // Advance `current_lsn()` so Snapshot `BEGIN` can pin a non-zero read view on an otherwise
        // empty WAL (engine `begin_transaction` is a no-op for LSN).
        (void)wm->append_record(txn, newdb::WalOp::SESSION_SNAPSHOT, table, nullptr);
        const auto ms = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - wal_begin_t)
                .count());
        onWriteTiming(WriteTimingStage::WalAppend, ms);
        return;
    }
    if (operation == "TXN_COMMIT") {
        (void)wm->commit_transaction(txn);
        const auto ms = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - wal_begin_t)
                .count());
        onWriteTiming(WriteTimingStage::WalAppend, ms);
        return;
    }
    if (operation == "TXN_ROLLBACK") {
        (void)wm->rollback_transaction(txn);
        const auto ms = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - wal_begin_t)
                .count());
        onWriteTiming(WriteTimingStage::WalAppend, ms);
        return;
    }
    if (operation == "RECOVERY_DONE") {
        (void)wm->append_record(txn, newdb::WalOp::SESSION_SNAPSHOT, table, nullptr);
        const auto ms = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - wal_begin_t)
                .count());
        onWriteTiming(WriteTimingStage::WalAppend, ms);
        return;
    }
    if (operation == "SAVEPOINT_SET") {
        // savepoint_id: stable hash of savepoint name
        (void)wm->append_record(txn, newdb::WalOp::SAVEPOINT_SET, table, nullptr, nullptr, nullptr, nullptr,
                                /*db_object_id=*/db_object_id,
                                /*savepoint_id=*/static_cast<std::uint64_t>(std::hash<std::string>{}(key)),
                                /*undo_prev_lsn=*/st_->m_last_undo_lsn,
                                /*pitr_target_lsn=*/0, /*pitr_target_ts_ms=*/0,
                                /*op_seq_in_txn=*/0);
        return;
    }
    if (operation == "SAVEPOINT_ROLLBACK") {
        (void)wm->append_record(txn, newdb::WalOp::SAVEPOINT_ROLLBACK, table, nullptr, nullptr, nullptr, nullptr,
                                db_object_id, static_cast<std::uint64_t>(std::hash<std::string>{}(key)), 0, 0, 0, 0);
        return;
    }
    if (operation == "TXN_ABORT_PARTIAL") {
        std::uint64_t cutoff = 0;
        try { cutoff = static_cast<std::uint64_t>(std::stoull(key)); } catch (...) {}
        (void)wm->append_record(txn, newdb::WalOp::TXN_ABORT_PARTIAL, table, nullptr, nullptr, nullptr, nullptr,
                                db_object_id, 0, 0, cutoff, 0, 0);
        return;
    }
    if (operation == "PITR_MARK") {
        std::uint64_t lsn_target = 0;
        std::uint64_t ts_target = 0;
        try { lsn_target = static_cast<std::uint64_t>(std::stoull(key)); } catch (...) {}
        try { ts_target = static_cast<std::uint64_t>(std::stoull(new_val)); } catch (...) {}
        (void)wm->append_record(txn, newdb::WalOp::PITR_MARK, table, nullptr, nullptr, nullptr, nullptr,
                                db_object_id, 0, 0, lsn_target, ts_target, 0);
        return;
    }

    newdb::WalOp op = newdb::WalOp::UPDATE;
    if (operation == "INSERT") {
        op = newdb::WalOp::INSERT;
    } else if (operation == "DELETE") {
        op = newdb::WalOp::DELETE;
    }

    newdb::Row row;
    try {
        row.id = std::stoi(key);
    } catch (...) {
        row.id = 0;
    }
    auto parse_attrs = [](const std::string& packed) {
        std::map<std::string, std::string> attrs;
        std::size_t i = 0;
        while (i < packed.size()) {
            const std::size_t sep = packed.find('=', i);
            if (sep == std::string::npos) break;
            const std::size_t end = packed.find(';', sep + 1);
            const std::string k = packed.substr(i, sep - i);
            const std::string v = (end == std::string::npos)
                                      ? packed.substr(sep + 1)
                                      : packed.substr(sep + 1, end - (sep + 1));
            if (!k.empty()) attrs[k] = v;
            if (end == std::string::npos) break;
            i = end + 1;
        }
        return attrs;
    };

    newdb::Row before;
    newdb::Row after;
    before.id = row.id;
    after.id = row.id;
    for (const auto& kv : parse_attrs(old_val)) {
        if (kv.first != "__deleted") before.attrs[kv.first] = kv.second;
    }
    for (const auto& kv : parse_attrs(new_val)) {
        if (kv.first != "__deleted") after.attrs[kv.first] = kv.second;
    }
    row.attrs["__wal_old"] = old_val; // legacy recovery compatibility
    row.attrs["__wal_new"] = new_val; // legacy recovery compatibility
    row.attrs["__wal_op"] = operation; // legacy recovery compatibility
    const std::uint64_t op_seq = st_->m_txn_op_seq.load(std::memory_order_relaxed);
    const newdb::Row* before_ptr = nullptr;
    const newdb::Row* after_ptr = nullptr;
    if (op == newdb::WalOp::INSERT) {
        after_ptr = &after;
    } else if (op == newdb::WalOp::DELETE) {
        before_ptr = &before;
    } else {
        before_ptr = &before;
        after_ptr = &after;
    }
    std::uint64_t wal_lsn = 0;
    const std::uint64_t undo_prev = st_->m_last_undo_lsn;
    (void)wm->append_record_with_lsn(txn, op, table, &row, nullptr, before_ptr, after_ptr,
                                    /*db_object_id=*/db_object_id, /*savepoint_id=*/0,
                                    /*undo_prev_lsn=*/undo_prev,
                                    /*pitr_target_lsn=*/0, /*pitr_target_ts_ms=*/0,
                                    op_seq, &wal_lsn);
    if (wal_lsn != 0) {
        st_->m_last_undo_lsn = wal_lsn;
        // Best-effort: also annotate the last in-memory record with its WAL LSN.
        if (!st_->m_txn_records.empty()) {
            st_->m_txn_records.back().wal_lsn = wal_lsn;
        }
    }
    const auto ms = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - wal_begin_t).count());
    onWriteTiming(WriteTimingStage::WalAppend, ms);
}


void TxnCoordinator::persistWalsnHighWaterUnlocked(newdb::WalManager* wm) {
    if (wm == nullptr) {
        return;
    }
    const std::string ws = st_->m_workspace_root.empty() ? std::string(".") : st_->m_workspace_root;
    write_wal_lsn_for_workspace(ws, wm->current_lsn());
}


void TxnCoordinator::maybeCompactWalAfterCommit(const std::string& committed_table) {
    std::lock_guard<std::mutex> lk(st_->m_wal_mutex);
    newdb::WalManager* w2 = ensureWal();
    if (w2 == nullptr) {
        return;
    }
    std::uint64_t cap = st_->wal_compact_max_bytes;
    if (const char* env = std::getenv("NEWDB_WAL_COMPACT_BYTES")) {
        const std::uint64_t v = static_cast<std::uint64_t>(std::strtoull(env, nullptr, 10));
        if (v > 0) {
            cap = v;
        }
    }
    if (w2->wal_file_size_bytes() < cap) {
        return;
    }
    (void)w2->checkpoint_and_truncate(w2->current_lsn());
    st_->m_wal_compact_count.fetch_add(1, std::memory_order_relaxed);
    st_->m_maintenance_checkpoint_trigger_count.fetch_add(1, std::memory_order_relaxed);
    if (!committed_table.empty() && st_->m_vacuum_running.load(std::memory_order_relaxed)) {
        triggerVacuum(committed_table);
        st_->m_maintenance_checkpoint_vacuum_enqueue_count.fetch_add(1, std::memory_order_relaxed);
    }
    persistWalsnHighWaterUnlocked(w2);
}


void TxnCoordinator::flushWAL() {
    std::lock_guard<std::mutex> lk(st_->m_wal_mutex);
    newdb::WalManager* wm = ensureWal();
    if (!wm) {
        return;
    }
    // Track WAL growth as a coarse throughput proxy.
    const std::uint64_t wal_bytes = wm->wal_file_size_bytes();
    const std::uint64_t last = st_->m_last_wal_bytes.exchange(wal_bytes, std::memory_order_relaxed);
    if (wal_bytes >= last) {
        st_->m_wal_bytes_since_start.fetch_add(wal_bytes - last, std::memory_order_relaxed);
    }
    const auto pending = st_->m_wal_group_commit_pending_commits.load(std::memory_order_relaxed);
    const auto window_ms = st_->m_group_commit_window_ms.load(std::memory_order_relaxed);
    const auto max_batch = std::max<std::uint64_t>(1, st_->m_group_commit_max_batch_commits.load(std::memory_order_relaxed));
    const auto now_ms = now_ms_steady();
    if (pending > 0 && window_ms > 0 && pending < max_batch) {
        const auto last_flush_ms = st_->m_last_wal_flush_ms.load(std::memory_order_relaxed);
        if (last_flush_ms > 0 && (now_ms - last_flush_ms) < window_ms) {
            return;
        }
    }
    if (st_->m_hybrid_adaptive_enabled.load(std::memory_order_relaxed)) {
        std::uint64_t queue_depth = st_->m_vacuum_queue_depth.load(std::memory_order_relaxed);
        std::uint64_t recovery_tail = st_->m_wal_recovery_last_elapsed_ms.load(std::memory_order_relaxed);
        std::uint64_t lock_tail = st_->m_lock_wait_max_ms.load(std::memory_order_relaxed);
        (void)read_u64_env("NEWDB_HYBRID_TEST_QUEUE_DEPTH", &queue_depth);
        (void)read_u64_env("NEWDB_HYBRID_TEST_RECOVERY_TAIL_MS", &recovery_tail);
        (void)read_u64_env("NEWDB_HYBRID_TEST_LOCK_TAIL_MS", &lock_tail);
        const auto now_ms = now_ms_steady();
        const auto last_switch = st_->m_hybrid_last_switch_ms.load(std::memory_order_relaxed);
        std::uint64_t min_dwell_ms = 5000;
        (void)read_u64_env("NEWDB_HYBRID_MIN_DWELL_MS", &min_dwell_ms);
        const bool can_switch = (last_switch == 0) || (now_ms >= last_switch && (now_ms - last_switch) >= min_dwell_ms);
        const bool need_durability = (recovery_tail > 600 || lock_tail > 100);
        const bool need_throughput = (queue_depth > 4 && !need_durability);
        const std::uint8_t cur_mode = st_->m_hybrid_mode.load(std::memory_order_relaxed);
        std::uint8_t next_mode = cur_mode;
        std::string reason;
        if (need_durability) {
            next_mode = 1;
            reason = "recovery_or_lock_tail";
        } else if (need_throughput) {
            next_mode = 0;
            reason = "queue_backpressure";
        }
        if (can_switch && next_mode != cur_mode) {
            st_->m_hybrid_mode.store(next_mode, std::memory_order_relaxed);
            st_->m_hybrid_mode_switch_count.fetch_add(1, std::memory_order_relaxed);
            st_->m_hybrid_last_switch_ms.store(now_ms, std::memory_order_relaxed);
            {
                std::lock_guard<std::mutex> lk(st_->m_hybrid_mu);
                st_->m_hybrid_last_switch_reason = reason;
            }
        }
        if (st_->m_hybrid_mode.load(std::memory_order_relaxed) == 1) {
            wm->set_sync_mode(newdb::WalSyncMode::Full);
            st_->m_group_commit_window_ms.store(0, std::memory_order_relaxed);
            st_->m_group_commit_max_batch_commits.store(1, std::memory_order_relaxed);
        } else {
            wm->set_sync_mode(newdb::WalSyncMode::Normal);
            wm->set_normal_sync_interval_ms(20);
        }
    } else if (st_->m_wal_adaptive_enabled.load(std::memory_order_relaxed) &&
               st_->m_vacuum_queue_depth.load(std::memory_order_relaxed) > 4) {
        wm->set_sync_mode(newdb::WalSyncMode::Normal);
        wm->set_normal_sync_interval_ms(20);
    }
    (void)wm->flush();
    persistWalsnHighWaterUnlocked(wm);
    if (pending > 0) {
        st_->m_wal_group_commit_count.fetch_add(1, std::memory_order_relaxed);
        st_->m_wal_group_commit_batch_commits.fetch_add(pending, std::memory_order_relaxed);
        st_->m_wal_group_commit_pending_commits.store(0, std::memory_order_relaxed);
    }
    st_->m_last_wal_flush_ms.store(now_ms, std::memory_order_relaxed);
}


void TxnCoordinator::setWalSyncMode(newdb::WalSyncMode mode) {
    std::lock_guard<std::mutex> lk(st_->m_wal_mutex);
    newdb::WalManager* wm = ensureWal();
    if (wm) {
        wm->set_sync_mode(mode);
        if (mode == newdb::WalSyncMode::Full) {
            st_->m_group_commit_window_ms.store(0, std::memory_order_relaxed);
            st_->m_group_commit_max_batch_commits.store(1, std::memory_order_relaxed);
        }
        persistWalSyncConfig(*wm);
    }
}


newdb::WalSyncMode TxnCoordinator::walSyncMode() {
    std::lock_guard<std::mutex> lk(st_->m_wal_mutex);
    if (!st_->wal_) {
        return newdb::WalSyncMode::Full;
    }
    return st_->wal_->sync_mode();
}


void TxnCoordinator::setWalNormalSyncIntervalMs(const std::uint64_t ms) {
    std::lock_guard<std::mutex> lk(st_->m_wal_mutex);
    newdb::WalManager* wm = ensureWal();
    if (wm) {
        wm->set_normal_sync_interval_ms(ms);
        persistWalSyncConfig(*wm);
    }
}


std::uint64_t TxnCoordinator::walNormalSyncIntervalMs() {
    std::lock_guard<std::mutex> lk(st_->m_wal_mutex);
    if (!st_->wal_) {
        return 50;
    }
    return st_->wal_->normal_sync_interval_ms();
}


void TxnCoordinator::setGroupCommitWindowMs(const std::uint64_t ms) {
    const auto mode = walSyncMode();
    if (mode == newdb::WalSyncMode::Full) {
        st_->m_group_commit_window_ms.store(0, std::memory_order_relaxed);
        return;
    }
    st_->m_group_commit_window_ms.store(ms, std::memory_order_relaxed);
}


std::uint64_t TxnCoordinator::groupCommitWindowMs() const {
    return st_->m_group_commit_window_ms.load(std::memory_order_relaxed);
}


void TxnCoordinator::setGroupCommitMaxBatchCommits(const std::uint64_t n) {
    const auto mode = walSyncMode();
    if (mode == newdb::WalSyncMode::Full) {
        st_->m_group_commit_max_batch_commits.store(1, std::memory_order_relaxed);
        return;
    }
    st_->m_group_commit_max_batch_commits.store((n == 0) ? 1 : n, std::memory_order_relaxed);
}


std::uint64_t TxnCoordinator::groupCommitMaxBatchCommits() const {
    return st_->m_group_commit_max_batch_commits.load(std::memory_order_relaxed);
}


void TxnCoordinator::setWalAdaptiveEnabled(const bool enabled) {
    st_->m_wal_adaptive_enabled.store(enabled, std::memory_order_relaxed);
}


bool TxnCoordinator::walAdaptiveEnabled() const {
    return st_->m_wal_adaptive_enabled.load(std::memory_order_relaxed);
}

void TxnCoordinator::setHybridAdaptiveEnabled(const bool enabled) {
    st_->m_hybrid_adaptive_enabled.store(enabled, std::memory_order_relaxed);
}

bool TxnCoordinator::hybridAdaptiveEnabled() const {
    return st_->m_hybrid_adaptive_enabled.load(std::memory_order_relaxed);
}


void TxnCoordinator::setHotIndexEnabled(const bool enabled) {
    st_->m_hot_index_enabled.store(enabled, std::memory_order_relaxed);
}


bool TxnCoordinator::hotIndexEnabled() const {
    return st_->m_hot_index_enabled.load(std::memory_order_relaxed);
}


void TxnCoordinator::setSegmentTargetBytes(const std::uint64_t bytes) {
    st_->m_segment_target_bytes.store(bytes, std::memory_order_relaxed);
}


std::uint64_t TxnCoordinator::segmentTargetBytes() const {
    return st_->m_segment_target_bytes.load(std::memory_order_relaxed);
}


void TxnCoordinator::setLsmSegmentCount(const std::uint64_t n) {
    st_->m_lsm_segment_count.store(n, std::memory_order_relaxed);
}


void TxnCoordinator::setLsmMemtableBytes(const std::uint64_t n) {
    st_->m_lsm_memtable_bytes.store(n, std::memory_order_relaxed);
}


void TxnCoordinator::syncHeapReadSnapshotForQuery(newdb::HeapTable& table) {
    auto set_source = [this](const std::uint8_t code) {
        st_->m_last_snapshot_source_code.store(code, std::memory_order_relaxed);
    };
    auto publish_snapshot_lsns = [this](std::uint64_t txn_lsn, std::uint64_t stmt_lsn) {
        st_->m_last_transaction_snapshot_lsn.store(txn_lsn, std::memory_order_relaxed);
        st_->m_last_statement_snapshot_lsn.store(stmt_lsn, std::memory_order_relaxed);
    };
    if (const char* opt = std::getenv("NEWDB_TXN_ISOLATION_READPATH")) {
        std::string v = opt;
        for (auto& ch : v) {
            ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
        }
        if (v == "0" || v == "off" || v == "false" || v == "no") {
            table.clear_snapshot();
            st_->m_txn_readpath_disabled_count.fetch_add(1, std::memory_order_relaxed);
            set_source(3);
            publish_snapshot_lsns(0, 0);
            return;
        }
    }
    std::lock_guard<std::mutex> lk(st_->m_wal_mutex);
    newdb::WalManager* wm = st_->wal_.get();
    if (wm == nullptr) {
        table.clear_snapshot();
        set_source(0);
        publish_snapshot_lsns(0, 0);
        return;
    }
    // Outside an active transaction, do not pin an MVCC snapshot on the heap. Compensation / undo rows
    // can carry created_lsn equal to the current WAL tip; a statement snapshot taken slightly earlier
    // would mark them invisible (e.g. COUNT/WHERE after ROLLBACK while readpath is enabled).
    if (getState() != TxnState::Active) {
        table.clear_snapshot();
        set_source(0);
        publish_snapshot_lsns(0, 0);
        return;
    }
    std::uint64_t lsn = wm->current_lsn();
    std::uint64_t txn_lsn = 0;
    std::uint64_t stmt_lsn = 0;
    if (txnIsolationLevel() == TxnIsolationLevel::Snapshot) {
        const std::uint64_t fixed = st_->m_txn_read_view_lsn.load(std::memory_order_relaxed);
        if (getState() == TxnState::Active && fixed != 0) {
            lsn = fixed;
            st_->m_txn_snapshot_pinned_count.fetch_add(1, std::memory_order_relaxed);
            set_source(1);
            txn_lsn = lsn;
        } else {
            st_->m_txn_snapshot_refresh_count.fetch_add(1, std::memory_order_relaxed);
            set_source(2);
            stmt_lsn = lsn;
        }
    } else {
        st_->m_txn_snapshot_refresh_count.fetch_add(1, std::memory_order_relaxed);
        set_source(2);
        stmt_lsn = lsn;
    }
    publish_snapshot_lsns(txn_lsn, stmt_lsn);
    newdb::MVCCSnapshot snap;
    snap.snapshot_lsn = lsn;
    snap.min_visible_lsn = 0;
    table.set_snapshot(snap);
    if (const char* tr = std::getenv("NEWDB_TXN_TRACE")) {
        std::string tv = tr;
        for (auto& ch : tv) {
            ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
        }
        if (tv == "1" || tv == "on" || tv == "true" || tv == "yes") {
            const char* iso =
                txnIsolationLevel() == TxnIsolationLevel::ReadCommitted ? "read_committed" : "snapshot";
            std::fprintf(stderr,
                         "[TXN_TRACE] sync_read_view iso=%s snapshot_lsn=%llu in_txn=%d\n",
                         iso,
                         static_cast<unsigned long long>(lsn),
                         getState() == TxnState::Active ? 1 : 0);
        }
    }
}



