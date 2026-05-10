#include <waterfall/config.h>

#include "cli/modules/txn/coordinator/txn_manager.h"
#include "cli/modules/txn/coordinator/txn_coordinator_state.h"
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

// Write-intent reservation: all LockKey kinds share `g_write_intent_owner`; conflict is storage-key equality only.
// Future unique secondary index: reserve `LockKey::index_eq_write_intent(table, index_name, encoded_key)` *before*
// uniqueness checks; recommended acquisition order with PK row intent is **index_eq first, then row_pk** so that
// bulk PK reservation (`tryReserveWriteKeysBatchSorted`) and single-row updates stay consistent.

void TxnCoordinator::recordWriteConflictSampleLockKey(const LockKey& lk,
                                                        const std::uint64_t holder_txn,
                                                        const char* tag) {
    std::ostringstream oss;
    oss << "table=" << lk.table << ";lock=" << lk.to_storage_key() << ";holder=" << holder_txn
        << ";tag=" << (tag ? tag : "");
    std::lock_guard<std::mutex> lk_mu(st_->m_write_conflict_sample_mu);
    st_->m_write_conflict_last_sample = oss.str();
}

void TxnCoordinator::recordWriteConflictSample(const std::string& table_name,
                                               const int row_id,
                                               const std::uint64_t holder_txn,
                                               const char* tag) {
    recordWriteConflictSampleLockKey(LockKey::row_pk_write_intent(table_name, row_id), holder_txn, tag);
}

bool TxnCoordinator::tryReserveWriteKey(const std::string& table_name, const int id, std::string* reason) {
    if (table_name.empty()) {
        return true;
    }
    return tryReserveWriteLockKey(LockKey::row_pk_write_intent(table_name, id), reason);
}

bool TxnCoordinator::tryReserveWriteKeysBatchSorted(const std::string& table_name,
                                                    std::vector<int> row_ids,
                                                    std::string* reason) {
    if (table_name.empty()) {
        return true;
    }
    std::sort(row_ids.begin(), row_ids.end());
    row_ids.erase(std::unique(row_ids.begin(), row_ids.end()), row_ids.end());
    std::vector<std::string> acquired;
    acquired.reserve(row_ids.size());
    for (const int id : row_ids) {
        LockKey lk = LockKey::row_pk_write_intent(table_name, id);
        if (!tryReserveWriteLockKey(lk, reason)) {
            releaseWriteIntentStorageKeysForCurrentTxn(acquired);
            return false;
        }
        acquired.push_back(lk.to_storage_key());
    }
    return true;
}

void TxnCoordinator::releaseWriteIntentStorageKeysForCurrentTxn(const std::vector<std::string>& storage_keys) {
    if (storage_keys.empty()) {
        return;
    }
    if (st_->m_state.load() != TxnState::Active) {
        return;
    }
    const std::uint64_t txn = static_cast<std::uint64_t>(st_->m_txn_id.load());
    std::lock_guard<std::mutex> lk_mu(g_write_intent_mu);
    g_txn_wait_for_owner.erase(txn);
    for (const std::string& key : storage_keys) {
        const auto it = g_write_intent_owner.find(key);
        if (it != g_write_intent_owner.end() && it->second == txn) {
            g_write_intent_owner.erase(it);
        }
        st_->m_reserved_write_keys.erase(key);
    }
}

bool TxnCoordinator::tryReserveWriteLockKey(const LockKey& lk, std::string* reason) {
    if (st_->m_state.load() != TxnState::Active) {
        return true;
    }
    if (lk.table.empty()) {
        return true;
    }
    const std::uint64_t txn = static_cast<std::uint64_t>(st_->m_txn_id.load());
    const std::string key = lk.to_storage_key();
    const WriteConflictPolicy policy = st_->m_write_conflict_policy.load(std::memory_order_relaxed);
    const std::uint64_t wait_timeout_ms = st_->m_write_conflict_wait_timeout_ms.load(std::memory_order_relaxed);
    const auto wait_start = std::chrono::steady_clock::now();
    bool deadlock_reported = false;
    unsigned wait_backoff_round = 0;

    for (;;) {
        {
            std::lock_guard<std::mutex> lk_mu(g_write_intent_mu);
            const auto it = g_write_intent_owner.find(key);
            if (it == g_write_intent_owner.end() || it->second == txn) {
                g_write_intent_owner[key] = txn;
                g_txn_wait_for_owner.erase(txn);
                const auto ins = st_->m_reserved_write_keys.insert(key);
                if (ins.second) {
                    if (lk.kind == LockKeyKind::RangeWriteIntent) {
                        st_->m_lock_key_range_count.fetch_add(1, std::memory_order_relaxed);
                    } else if (lk.kind == LockKeyKind::PredicateWriteIntent) {
                        st_->m_lock_key_predicate_count.fetch_add(1, std::memory_order_relaxed);
                    }
                }
                const auto waited_ms = static_cast<std::uint64_t>(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - wait_start)
                        .count());
                if (waited_ms > 0) {
                    st_->m_lock_wait_ms_total.fetch_add(waited_ms, std::memory_order_relaxed);
                    {
                        std::lock_guard<std::mutex> lk_samples(st_->m_samples_mu);
                        st_->m_lock_wait_ms_samples.push_back(waited_ms);
                        if (st_->m_lock_wait_ms_samples.size() > 256) {
                            st_->m_lock_wait_ms_samples.erase(st_->m_lock_wait_ms_samples.begin(),
                                                         st_->m_lock_wait_ms_samples.begin() + 64);
                        }
                    }
                    std::uint64_t old_max = st_->m_lock_wait_max_ms.load(std::memory_order_relaxed);
                    while (waited_ms > old_max &&
                           !st_->m_lock_wait_max_ms.compare_exchange_weak(
                               old_max, waited_ms, std::memory_order_relaxed, std::memory_order_relaxed)) {
                    }
                }
                return true;
            }
            g_txn_wait_for_owner[txn] = it->second;
            if (!deadlock_reported) {
                std::uint64_t cycle_owner = 0;
                if (detect_wait_cycle(txn, cycle_owner)) {
                    st_->m_lock_deadlock_detect_count.fetch_add(1, std::memory_order_relaxed);
                    st_->m_lock_deadlock_victim_count.fetch_add(1, std::memory_order_relaxed);
                    deadlock_reported = true;
                    st_->m_write_conflict_count.fetch_add(1, std::memory_order_relaxed);
                    recordWriteConflictSampleLockKey(lk, cycle_owner, "deadlock_victim");
                    g_txn_wait_for_owner.erase(txn);
                    if (reason != nullptr) {
                        *reason = "deadlock detected on " + key + ", current txn chosen as victim";
                    }
                    return false;
                }
            }
        }
        if (policy != WriteConflictPolicy::Wait || wait_timeout_ms == 0) {
            st_->m_write_conflict_count.fetch_add(1, std::memory_order_relaxed);
            std::uint64_t holder = 0;
            {
                std::lock_guard<std::mutex> lk_mu(g_write_intent_mu);
                const auto hi = g_write_intent_owner.find(key);
                if (hi != g_write_intent_owner.end()) {
                    holder = hi->second;
                }
                g_txn_wait_for_owner.erase(txn);
            }
            recordWriteConflictSampleLockKey(lk, holder, "reject");
            if (reason != nullptr) {
                *reason = "write conflict on " + key + " (held by another active transaction)";
            }
            return false;
        }
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - wait_start);
        if (elapsed.count() >= static_cast<long long>(wait_timeout_ms)) {
            st_->m_write_conflict_count.fetch_add(1, std::memory_order_relaxed);
            st_->m_write_conflict_wait_timeout_count.fetch_add(1, std::memory_order_relaxed);
            st_->m_lock_wait_ms_total.fetch_add(static_cast<std::uint64_t>(elapsed.count()), std::memory_order_relaxed);
            std::uint64_t wait_ms = static_cast<std::uint64_t>(elapsed.count());
            {
                std::lock_guard<std::mutex> lk_samples(st_->m_samples_mu);
                st_->m_lock_wait_ms_samples.push_back(wait_ms);
                if (st_->m_lock_wait_ms_samples.size() > 256) {
                    st_->m_lock_wait_ms_samples.erase(st_->m_lock_wait_ms_samples.begin(),
                                                 st_->m_lock_wait_ms_samples.begin() + 64);
                }
            }
            std::uint64_t old_max = st_->m_lock_wait_max_ms.load(std::memory_order_relaxed);
            while (wait_ms > old_max &&
                   !st_->m_lock_wait_max_ms.compare_exchange_weak(
                       old_max, wait_ms, std::memory_order_relaxed, std::memory_order_relaxed)) {
            }
            std::uint64_t holder = 0;
            {
                std::lock_guard<std::mutex> lk_mu(g_write_intent_mu);
                const auto hi = g_write_intent_owner.find(key);
                if (hi != g_write_intent_owner.end()) {
                    holder = hi->second;
                }
                g_txn_wait_for_owner.erase(txn);
            }
            recordWriteConflictSampleLockKey(lk, holder, "wait_timeout");
            if (reason != nullptr) {
                *reason = "write conflict wait timeout on " + key;
            }
            return false;
        }
        st_->m_write_conflict_wait_count.fetch_add(1, std::memory_order_relaxed);
        const unsigned ms =
            (std::min)(128u, 1u << (std::min)(wait_backoff_round, static_cast<unsigned>(7)));
        wait_backoff_round = (std::min)(wait_backoff_round + 1u, 24u);
        std::this_thread::sleep_for(std::chrono::milliseconds(ms == 0u ? 1u : ms));
    }
}


void TxnCoordinator::setWriteConflictPolicy(const WriteConflictPolicy policy) {
    st_->m_write_conflict_policy.store(policy, std::memory_order_relaxed);
}


WriteConflictPolicy TxnCoordinator::writeConflictPolicy() const {
    return st_->m_write_conflict_policy.load(std::memory_order_relaxed);
}


void TxnCoordinator::setWriteConflictWaitTimeoutMs(const std::uint64_t ms) {
    st_->m_write_conflict_wait_timeout_ms.store(ms, std::memory_order_relaxed);
}


std::uint64_t TxnCoordinator::writeConflictWaitTimeoutMs() const {
    return st_->m_write_conflict_wait_timeout_ms.load(std::memory_order_relaxed);
}


void TxnCoordinator::setTxnIsolationLevel(const TxnIsolationLevel level) {
    st_->m_txn_isolation_level.store(level, std::memory_order_relaxed);
}


TxnIsolationLevel TxnCoordinator::txnIsolationLevel() const {
    return st_->m_txn_isolation_level.load(std::memory_order_relaxed);
}


void TxnCoordinator::clearWriteIntents() {
    if (st_->m_reserved_write_keys.empty()) {
        return;
    }
    const std::uint64_t txn = static_cast<std::uint64_t>(st_->m_txn_id.load());
    std::lock_guard<std::mutex> lk(g_write_intent_mu);
    g_txn_wait_for_owner.erase(txn);
    for (const auto& key : st_->m_reserved_write_keys) {
        const auto it = g_write_intent_owner.find(key);
        if (it != g_write_intent_owner.end() && it->second == txn) {
            g_write_intent_owner.erase(it);
        }
    }
    st_->m_reserved_write_keys.clear();
}



