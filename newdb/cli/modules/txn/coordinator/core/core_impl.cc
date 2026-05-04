#include <waterfall/config.h>

#include "cli/modules/txn/coordinator/txn_manager.h"
#include "cli/modules/txn/coordinator/internal/txn_internal.h"
#include "cli/modules/common/logging/logging.h"
#include "cli/modules/common/util/constants.h"
#include "cli/modules/sidecar/common/sidecar_wal_lsn.h"
#include "cli/modules/where/executor/stats/table_stats.h"

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

std::mutex g_write_intent_mu;
std::unordered_map<std::string, std::uint64_t> g_write_intent_owner;
std::unordered_map<std::uint64_t, std::uint64_t> g_txn_wait_for_owner;
std::atomic<std::int64_t> g_txn_id_seed{
    static_cast<std::int64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count())};

newdb::Status compact_table_file_default(const std::string& data_file, const std::string& table_name) {
    if (data_file.empty() || table_name.empty()) {
        return newdb::Status::Fail("empty compact target");
    }
    newdb::TableSchema schema;
    (void)newdb::load_schema_text(newdb::schema_sidecar_path_for_data_file(data_file), schema);
    const newdb::Status st = newdb::io::compact_heap_file(data_file.c_str(), table_name, schema);
    if (st.ok) {
        invalidate_table_stats_for_data_file(data_file);
    }
    return st;
}

std::uint64_t file_size_or_zero(const std::string& path) {
    std::error_code ec;
    const auto sz = std::filesystem::file_size(path, ec);
    if (ec) {
        return 0;
    }
    return static_cast<std::uint64_t>(sz);
}

std::uint64_t now_ms_steady() {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

bool detect_wait_cycle(const std::uint64_t start, std::uint64_t& cycle_owner) {
    std::unordered_set<std::uint64_t> visited;
    std::uint64_t cur = start;
    while (true) {
        const auto it = g_txn_wait_for_owner.find(cur);
        if (it == g_txn_wait_for_owner.end()) {
            return false;
        }
        cur = it->second;
        if (cur == start) {
            cycle_owner = cur;
            return true;
        }
        if (!visited.insert(cur).second) {
            return false;
        }
    }
}

TxnCoordinator::TxnCoordinator() {
    // Staged write timing sampling knob (lower => more detail, higher => less overhead).
    // Default to 8 so pressure tests will have enough samples for p95 attribution.
    std::uint64_t every_n = 8;
    if (const char* raw = std::getenv("NEWDB_WRITE_TIMING_EVERY_N")) {
        try {
            const std::uint64_t v = static_cast<std::uint64_t>(std::stoull(raw));
            if (v > 0) every_n = v;
        } catch (...) {
        }
    }
    m_write_timing_sample_every_n.store(every_n, std::memory_order_relaxed);
}


TxnCoordinator::~TxnCoordinator() {
    stopVacuumThread();
    clearWriteIntents();
    std::vector<std::string> locked_copy;
    {
        std::lock_guard<std::mutex> lk(m_lock_mutex);
        locked_copy = m_locked_files;
    }
    for (const auto& f : locked_copy) {
        (void)releaseLock(f);
    }
}

// ========== ???? ==========


Result<bool> TxnCoordinator::begin(const std::string& table_name) {
    std::lock_guard<std::mutex> lk(m_txn_mutex);
    
    if (m_state.load() == TxnState::Active) {
        return Result<bool>::Err("??????");
    }
    
    // ?????????ID????????????begin ??????
    const std::int64_t next_txn_id = g_txn_id_seed.fetch_add(1, std::memory_order_relaxed) + 1;
    m_txn_id.store(next_txn_id, std::memory_order_relaxed);
    
    m_state.store(TxnState::Active);
    m_txn_op_seq.store(0, std::memory_order_relaxed);
    m_txn_records.clear();
    m_savepoints.clear();
    m_savepoints_lsn.clear();
    m_last_undo_lsn = 0;
    m_reserved_write_keys.clear();
    {
        std::lock_guard<std::mutex> wait_lk(g_write_intent_mu);
        g_txn_wait_for_owner.erase(static_cast<std::uint64_t>(next_txn_id));
    }
    
    m_active_table = table_name;
    const std::string bin_file = resolveDataFilePath(table_name);
    auto lockResult = acquireLock(bin_file);
    if (lockResult.isErr()) {
        m_txn_begin_lock_conflict_count.fetch_add(1, std::memory_order_relaxed);
        m_state.store(TxnState::None);
        return lockResult;
    }
    writeWAL("TXN_BEGIN", table_name, "", "", "");
    flushWAL();
    {
        std::lock_guard<std::mutex> wlk(m_wal_mutex);
        if (wal_) {
            if (txnIsolationLevel() == TxnIsolationLevel::Snapshot) {
                m_txn_read_view_lsn.store(wal_->current_lsn(), std::memory_order_relaxed);
            } else {
                m_txn_read_view_lsn.store(0, std::memory_order_relaxed);
            }
        } else {
            m_txn_read_view_lsn.store(0, std::memory_order_relaxed);
        }
    }

    return Result<bool>::Ok(true);
}


Result<bool> TxnCoordinator::commit() {
    const auto commit_start = std::chrono::steady_clock::now();
    std::string committed_table;
    bool should_trigger_vacuum = false;
    {
        std::lock_guard<std::mutex> lk(m_txn_mutex);
        
        if (m_state.load() != TxnState::Active) {
            return Result<bool>::Err("??????");
        }
        
        writeWAL("TXN_COMMIT", m_active_table, "", "", "");
        m_wal_group_commit_pending_commits.fetch_add(1, std::memory_order_relaxed);
        flushWAL();
        m_txn_commit_count.fetch_add(1, std::memory_order_relaxed);
        
        std::vector<std::string> locked_copy;
        {
            std::lock_guard<std::mutex> lk2(m_lock_mutex);
            locked_copy = m_locked_files;
        }
        for (const auto& f : locked_copy) {
            (void)releaseLock(f);
        }
        
        committed_table = m_active_table;
        m_state.store(TxnState::Committed);
        m_txn_op_seq.store(0, std::memory_order_relaxed);
        clearWriteIntents();
        m_txn_records.clear();
        m_locked_files.clear();
        m_active_table.clear();
        m_txn_read_view_lsn.store(0, std::memory_order_relaxed);

        if (m_vacuum_running.load()) {
            const std::size_t threshold = m_vacuum_ops_threshold.load();
            const std::size_t count = m_vacuum_op_counter.load();
            if (threshold > 0 && count >= threshold && !committed_table.empty()) {
                m_vacuum_op_counter.store(0);
                should_trigger_vacuum = true;
            }
        }
    }
    maybeCompactWalAfterCommit(committed_table);
    if (should_trigger_vacuum) {
        triggerVacuum(committed_table);
    }
    const auto elapsed_ms = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - commit_start)
            .count());
    {
        std::lock_guard<std::mutex> lk(m_samples_mu);
        m_commit_latency_ms_samples.push_back(elapsed_ms);
        if (m_commit_latency_ms_samples.size() > 256) {
            m_commit_latency_ms_samples.erase(m_commit_latency_ms_samples.begin(),
                                              m_commit_latency_ms_samples.begin() + 64);
        }
    }
    std::uint64_t old_max = m_txn_commit_max_ms.load(std::memory_order_relaxed);
    while (elapsed_ms > old_max &&
           !m_txn_commit_max_ms.compare_exchange_weak(
               old_max, elapsed_ms, std::memory_order_relaxed, std::memory_order_relaxed)) {
    }
    return Result<bool>::Ok(true);
}


Result<bool> TxnCoordinator::rollback() {
    std::lock_guard<std::mutex> lk(m_txn_mutex);
    
    if (m_state.load() != TxnState::Active) {
        return Result<bool>::Err("??????");
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

    auto append_undo_record = [&](const TxnRecord& rec) {
        const std::string data_file = resolveDataFilePath(rec.table_name);
        try {
            const int id = std::stoi(rec.key);
            if (rec.operation == "INSERT") {
                newdb::Row tomb;
                tomb.id = id;
                tomb.attrs["__deleted"] = "1";
                (void)newdb::io::append_row(data_file.c_str(), tomb);
                return;
            }
            newdb::Row row;
            row.id = id;
            const auto attrs = parse_attrs(rec.old_value);
            for (const auto& kv : attrs) {
                if (kv.first != "__deleted") {
                    row.attrs[kv.first] = kv.second;
                }
            }
            (void)newdb::io::append_row(data_file.c_str(), row);
        } catch (...) {
            // ignore malformed undo records
        }
    };

    // ??????????????
    for (auto it = m_txn_records.rbegin(); it != m_txn_records.rend(); ++it) {
        append_undo_record(*it);
    }
    
    writeWAL("TXN_ROLLBACK", m_active_table, "", "", "");
    flushWAL();
    
    std::vector<std::string> locked_copy;
    {
        std::lock_guard<std::mutex> lk2(m_lock_mutex);
        locked_copy = m_locked_files;
    }
    for (const auto& f : locked_copy) {
        (void)releaseLock(f);
    }
    
    m_state.store(TxnState::RolledBack);
    m_txn_op_seq.store(0, std::memory_order_relaxed);
    m_savepoints.clear();
    m_savepoints_lsn.clear();
    m_last_undo_lsn = 0;
    clearWriteIntents();
    m_txn_records.clear();
    m_locked_files.clear();
    m_active_table.clear();
    m_txn_read_view_lsn.store(0, std::memory_order_relaxed);

    return Result<bool>::Ok(true);
}

Result<bool> TxnCoordinator::savepoint(const std::string& name) {
    std::lock_guard<std::mutex> lk(m_txn_mutex);
    if (m_state.load() != TxnState::Active) {
        return Result<bool>::Err("no active transaction");
    }
    if (name.empty()) {
        return Result<bool>::Err("empty savepoint");
    }
    m_savepoints[name] = m_txn_op_seq.load(std::memory_order_relaxed);
    m_savepoints_lsn[name] = m_last_undo_lsn;
    writeWAL("SAVEPOINT_SET", m_active_table, name, "", "");
    return Result<bool>::Ok(true);
}

Result<bool> TxnCoordinator::releaseSavepoint(const std::string& name) {
    std::lock_guard<std::mutex> lk(m_txn_mutex);
    if (m_state.load() != TxnState::Active) {
        return Result<bool>::Err("no active transaction");
    }
    const auto it = m_savepoints.find(name);
    if (it == m_savepoints.end()) {
        return Result<bool>::Err("savepoint not found");
    }
    m_savepoints.erase(it);
    m_savepoints_lsn.erase(name);
    return Result<bool>::Ok(true);
}

Result<bool> TxnCoordinator::rollbackToSavepoint(const std::string& name) {
    std::lock_guard<std::mutex> lk(m_txn_mutex);
    if (m_state.load() != TxnState::Active) {
        return Result<bool>::Err("no active transaction");
    }
    const auto it_sp = m_savepoints.find(name);
    if (it_sp == m_savepoints.end()) {
        return Result<bool>::Err("savepoint not found");
    }
    const std::uint64_t target_op_seq = it_sp->second;
    const std::uint64_t target_lsn = [&]() -> std::uint64_t {
        const auto it2 = m_savepoints_lsn.find(name);
        if (it2 == m_savepoints_lsn.end()) return 0;
        return it2->second;
    }();
    auto parse_attrs = [](const std::string& packed) {
        std::map<std::string, std::string> attrs;
        std::size_t i = 0;
        while (i < packed.size()) {
            const std::size_t sep = packed.find('=', i);
            if (sep == std::string::npos) break;
            const std::size_t end = packed.find(';', sep + 1);
            const std::string key = packed.substr(i, sep - i);
            const std::string val =
                (end == std::string::npos) ? packed.substr(sep + 1) : packed.substr(sep + 1, end - (sep + 1));
            if (!key.empty()) attrs[key] = val;
            if (end == std::string::npos) break;
            i = end + 1;
        }
        return attrs;
    };
    std::uint64_t undone = 0;
    for (auto it = m_txn_records.rbegin(); it != m_txn_records.rend(); ++it) {
        if (it->op_seq <= target_op_seq) break;
        const std::string data_file = resolveDataFilePath(it->table_name);
        try {
            const int id = std::stoi(it->key);
            if (it->operation == "INSERT") {
                newdb::Row tomb;
                tomb.id = id;
                tomb.attrs["__deleted"] = "1";
                (void)newdb::io::append_row(data_file.c_str(), tomb);
            } else {
                newdb::Row row;
                row.id = id;
                const auto attrs = parse_attrs(it->old_value);
                for (const auto& kv : attrs) {
                    if (kv.first != "__deleted") row.attrs[kv.first] = kv.second;
                }
                (void)newdb::io::append_row(data_file.c_str(), row);
            }
            ++undone;
        } catch (...) {
            m_undo_chain_fallback_count.fetch_add(1, std::memory_order_relaxed);
        }
    }
    m_rollback_partial_ops.fetch_add(undone, std::memory_order_relaxed);
    m_rollback_savepoint_count.fetch_add(1, std::memory_order_relaxed);
    while (!m_txn_records.empty() && m_txn_records.back().op_seq > target_op_seq) {
        m_txn_records.pop_back();
    }
    m_txn_op_seq.store(target_op_seq, std::memory_order_relaxed);
    m_last_undo_lsn = target_lsn;
    writeWAL("TXN_ABORT_PARTIAL", m_active_table, std::to_string(target_lsn), "", "");
    writeWAL("SAVEPOINT_ROLLBACK", m_active_table, name, "", "");
    flushWAL();
    return Result<bool>::Ok(true);
}

// ========== ????==========


void TxnCoordinator::recordOperation(const std::string& operation, const std::string& table,
                                   const std::string& key, const std::string& old_val, const std::string& new_val) {
    if (m_state.load() != TxnState::Active) {
        return;  // ????????
    }
    
    TxnRecord rec;
    rec.txn_id = m_txn_id.load();
    rec.state = m_state.load();
    rec.table_name = table;
    rec.operation = operation;
    rec.key = key;
    rec.old_value = old_val;
    rec.new_value = new_val;
    rec.timestamp = std::time(nullptr);
    rec.op_seq = m_txn_op_seq.fetch_add(1, std::memory_order_relaxed) + 1;
    
    m_txn_records.push_back(rec);
    
    // ???? WAL
    writeWAL(operation, table, key, old_val, new_val);

    if (m_vacuum_running.load()) {
        (void)m_vacuum_op_counter.fetch_add(1);
    }
}



