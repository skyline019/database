#include <waterfall/config.h>

#include "cli/modules/txn/coordinator/txn_manager.h"
#include "cli/modules/txn/coordinator/write_conflict/lock_key.h"
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
#include <thread>
#include <algorithm>

void TxnCoordinator::recordWriteConflictSample(const std::string& table_name,
                                               const int row_id,
                                               const std::uint64_t holder_txn,
                                               const char* tag) {
    std::ostringstream oss;
    oss << "table=" << table_name << ";row=" << row_id << ";holder=" << holder_txn << ";tag=" << (tag ? tag : "");
    std::lock_guard<std::mutex> lk(m_write_conflict_sample_mu);
    m_write_conflict_last_sample = oss.str();
}

bool TxnCoordinator::tryReserveWriteKey(const std::string& table_name, const int id, std::string* reason) {
    if (m_state.load() != TxnState::Active) {
        return true;
    }
    if (table_name.empty()) {
        return true;
    }
    const std::uint64_t txn = static_cast<std::uint64_t>(m_txn_id.load());
    const std::string key = LockKey::row_pk_write_intent(table_name, id).to_storage_key();
    const WriteConflictPolicy policy = m_write_conflict_policy.load(std::memory_order_relaxed);
    const std::uint64_t wait_timeout_ms = m_write_conflict_wait_timeout_ms.load(std::memory_order_relaxed);
    const auto wait_start = std::chrono::steady_clock::now();
    bool deadlock_reported = false;
    unsigned wait_backoff_round = 0;

    for (;;) {
        {
            std::lock_guard<std::mutex> lk(g_write_intent_mu);
            const auto it = g_write_intent_owner.find(key);
            if (it == g_write_intent_owner.end() || it->second == txn) {
                g_write_intent_owner[key] = txn;
                g_txn_wait_for_owner.erase(txn);
                m_reserved_write_keys.insert(key);
                const auto waited_ms = static_cast<std::uint64_t>(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - wait_start)
                        .count());
                if (waited_ms > 0) {
                    m_lock_wait_ms_total.fetch_add(waited_ms, std::memory_order_relaxed);
                    {
                        std::lock_guard<std::mutex> lk(m_samples_mu);
                        m_lock_wait_ms_samples.push_back(waited_ms);
                        if (m_lock_wait_ms_samples.size() > 256) {
                            m_lock_wait_ms_samples.erase(m_lock_wait_ms_samples.begin(),
                                                         m_lock_wait_ms_samples.begin() + 64);
                        }
                    }
                    std::uint64_t old_max = m_lock_wait_max_ms.load(std::memory_order_relaxed);
                    while (waited_ms > old_max &&
                           !m_lock_wait_max_ms.compare_exchange_weak(
                               old_max, waited_ms, std::memory_order_relaxed, std::memory_order_relaxed)) {
                    }
                }
                return true;
            }
            g_txn_wait_for_owner[txn] = it->second;
            if (!deadlock_reported) {
                std::uint64_t cycle_owner = 0;
                if (detect_wait_cycle(txn, cycle_owner)) {
                    m_lock_deadlock_detect_count.fetch_add(1, std::memory_order_relaxed);
                    m_lock_deadlock_victim_count.fetch_add(1, std::memory_order_relaxed);
                    deadlock_reported = true;
                    m_write_conflict_count.fetch_add(1, std::memory_order_relaxed);
                    recordWriteConflictSample(table_name, id, cycle_owner, "deadlock_victim");
                    g_txn_wait_for_owner.erase(txn);
                    if (reason != nullptr) {
                        *reason = "deadlock detected on " + key + ", current txn chosen as victim";
                    }
                    return false;
                }
            }
        }
        if (policy != WriteConflictPolicy::Wait || wait_timeout_ms == 0) {
            m_write_conflict_count.fetch_add(1, std::memory_order_relaxed);
            std::uint64_t holder = 0;
            {
                std::lock_guard<std::mutex> lk(g_write_intent_mu);
                const auto hi = g_write_intent_owner.find(key);
                if (hi != g_write_intent_owner.end()) {
                    holder = hi->second;
                }
                g_txn_wait_for_owner.erase(txn);
            }
            recordWriteConflictSample(table_name, id, holder, "reject");
            if (reason != nullptr) {
                *reason = "write conflict on " + key + " (held by another active transaction)";
            }
            return false;
        }
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - wait_start);
        if (elapsed.count() >= static_cast<long long>(wait_timeout_ms)) {
            m_write_conflict_count.fetch_add(1, std::memory_order_relaxed);
            m_write_conflict_wait_timeout_count.fetch_add(1, std::memory_order_relaxed);
            m_lock_wait_ms_total.fetch_add(static_cast<std::uint64_t>(elapsed.count()), std::memory_order_relaxed);
            std::uint64_t wait_ms = static_cast<std::uint64_t>(elapsed.count());
            {
                std::lock_guard<std::mutex> lk(m_samples_mu);
                m_lock_wait_ms_samples.push_back(wait_ms);
                if (m_lock_wait_ms_samples.size() > 256) {
                    m_lock_wait_ms_samples.erase(m_lock_wait_ms_samples.begin(),
                                                 m_lock_wait_ms_samples.begin() + 64);
                }
            }
            std::uint64_t old_max = m_lock_wait_max_ms.load(std::memory_order_relaxed);
            while (wait_ms > old_max &&
                   !m_lock_wait_max_ms.compare_exchange_weak(
                       old_max, wait_ms, std::memory_order_relaxed, std::memory_order_relaxed)) {
            }
            std::uint64_t holder = 0;
            {
                std::lock_guard<std::mutex> lk(g_write_intent_mu);
                const auto hi = g_write_intent_owner.find(key);
                if (hi != g_write_intent_owner.end()) {
                    holder = hi->second;
                }
                g_txn_wait_for_owner.erase(txn);
            }
            recordWriteConflictSample(table_name, id, holder, "wait_timeout");
            if (reason != nullptr) {
                *reason = "write conflict wait timeout on " + key;
            }
            return false;
        }
        m_write_conflict_wait_count.fetch_add(1, std::memory_order_relaxed);
        const unsigned ms =
            (std::min)(128u, 1u << (std::min)(wait_backoff_round, static_cast<unsigned>(7)));
        wait_backoff_round = (std::min)(wait_backoff_round + 1u, 24u);
        std::this_thread::sleep_for(std::chrono::milliseconds(ms == 0u ? 1u : ms));
    }
}


void TxnCoordinator::setWriteConflictPolicy(const WriteConflictPolicy policy) {
    m_write_conflict_policy.store(policy, std::memory_order_relaxed);
}


WriteConflictPolicy TxnCoordinator::writeConflictPolicy() const {
    return m_write_conflict_policy.load(std::memory_order_relaxed);
}


void TxnCoordinator::setWriteConflictWaitTimeoutMs(const std::uint64_t ms) {
    m_write_conflict_wait_timeout_ms.store(ms, std::memory_order_relaxed);
}


std::uint64_t TxnCoordinator::writeConflictWaitTimeoutMs() const {
    return m_write_conflict_wait_timeout_ms.load(std::memory_order_relaxed);
}


void TxnCoordinator::setTxnIsolationLevel(const TxnIsolationLevel level) {
    m_txn_isolation_level.store(level, std::memory_order_relaxed);
}


TxnIsolationLevel TxnCoordinator::txnIsolationLevel() const {
    return m_txn_isolation_level.load(std::memory_order_relaxed);
}


void TxnCoordinator::clearWriteIntents() {
    if (m_reserved_write_keys.empty()) {
        return;
    }
    const std::uint64_t txn = static_cast<std::uint64_t>(m_txn_id.load());
    std::lock_guard<std::mutex> lk(g_write_intent_mu);
    g_txn_wait_for_owner.erase(txn);
    for (const auto& key : m_reserved_write_keys) {
        const auto it = g_write_intent_owner.find(key);
        if (it != g_write_intent_owner.end() && it->second == txn) {
            g_write_intent_owner.erase(it);
        }
    }
    m_reserved_write_keys.clear();
}



