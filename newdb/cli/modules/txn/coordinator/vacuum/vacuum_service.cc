#include <waterfall/config.h>

#include "cli/modules/txn/coordinator/txn_manager.h"
#include "cli/modules/txn/coordinator/internal/txn_internal.h"
#include "cli/modules/logging/logging.h"
#include "cli/modules/util/constants.h"
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

void TxnCoordinator::startVacuumThread() {
    if (m_vacuum_running.load()) {
        return;
    }
    
    m_vacuum_running.store(true);
    m_vacuum_op_counter.store(0);
    m_vacuum_trigger_count.store(0);
    m_vacuum_execute_count.store(0);
    m_vacuum_cooldown_skip_count.store(0);
    m_vacuum_compact_success_count.store(0);
    m_vacuum_compact_failure_count.store(0);
    m_vacuum_compact_bytes_reclaimed.store(0);
    m_vacuum_compact_last_elapsed_ms.store(0);
    m_vacuum_queue_depth.store(0);
    m_vacuum_queue_depth_peak.store(0);
    
    m_vacuum_thread = std::thread([this]() {
        while (m_vacuum_running.load()) {
            std::unique_lock<std::mutex> lk(m_vacuum_mutex);
            
            // 等待信号或定期检�?(�?60 �?
            m_vacuum_cv.wait_for(lk, std::chrono::seconds(60), [this]() {
                return !m_vacuum_running.load() || !m_vacuum_queue.empty();
            });
            
            if (!m_vacuum_running.load()) {
                break;
            }
            
            // 处理 VACUUM 队列
            while (!m_vacuum_queue.empty()) {
                if (m_vacuum_queue.size() > 16) {
                    m_scheduler_throttle_count.fetch_add(1, std::memory_order_relaxed);
                    lk.unlock();
                    std::this_thread::sleep_for(std::chrono::milliseconds(2));
                    lk.lock();
                }
                std::string table = m_vacuum_queue.back();
                m_vacuum_queue.pop_back();
                m_vacuum_queue_depth.store(
                    static_cast<std::uint64_t>(m_vacuum_queue.size()), std::memory_order_relaxed);
                m_vacuum_pending.erase(table);
                
                lk.unlock();
                const auto t0 = std::chrono::steady_clock::now();
                bool compact_success = true;
                std::uint64_t bytes_reclaimed = 0;
                const std::string data_file = resolveDataFilePath(table);
                const std::uint64_t before_bytes = file_size_or_zero(data_file);

                // Execute caller-provided callback when available, otherwise
                // use default heap compaction to keep long-run storage bounded.
                if (m_vacuum_callback) {
                    m_vacuum_callback(table);
                } else {
                    const newdb::Status st = compact_table_file_default(data_file, table);
                    compact_success = st.ok;
                }
                const std::uint64_t after_bytes = file_size_or_zero(data_file);
                if (before_bytes > after_bytes) {
                    bytes_reclaimed = before_bytes - after_bytes;
                }

                const auto t1 = std::chrono::steady_clock::now();
                const auto elapsed_ms =
                    static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count());
                if (compact_success) {
                    m_vacuum_compact_success_count.fetch_add(1, std::memory_order_relaxed);
                    if (bytes_reclaimed > 0) {
                        m_vacuum_compact_bytes_reclaimed.fetch_add(bytes_reclaimed, std::memory_order_relaxed);
                    }
                } else {
                    m_vacuum_compact_failure_count.fetch_add(1, std::memory_order_relaxed);
                }
                m_vacuum_compact_last_elapsed_ms.store(elapsed_ms, std::memory_order_relaxed);

                lk.lock();
                m_vacuum_last_run[table] = std::chrono::steady_clock::now();
                m_vacuum_execute_count.fetch_add(1, std::memory_order_relaxed);
            }
        }
    });
}


void TxnCoordinator::stopVacuumThread() {
    if (!m_vacuum_running.load()) {
        return;
    }
    
    m_vacuum_running.store(false);
    m_vacuum_cv.notify_all();
    
    if (m_vacuum_thread.joinable()) {
        m_vacuum_thread.join();
    }
    m_vacuum_op_counter.store(0);
}


void TxnCoordinator::triggerVacuum(const std::string& table_name) {
    std::lock_guard<std::mutex> lk(m_vacuum_mutex);
    if (table_name.empty()) {
        return;
    }
    const auto now = std::chrono::steady_clock::now();
    const std::size_t min_interval = m_vacuum_min_interval_sec.load();
    const auto it_last = m_vacuum_last_run.find(table_name);
    if (it_last != m_vacuum_last_run.end() && min_interval > 0) {
        const auto elapsed =
            std::chrono::duration_cast<std::chrono::seconds>(now - it_last->second).count();
        if (elapsed >= 0 && static_cast<std::size_t>(elapsed) < min_interval) {
            m_vacuum_cooldown_skip_count.fetch_add(1, std::memory_order_relaxed);
            return;
        }
    }
    if (!m_vacuum_pending.insert(table_name).second) {
        return;
    }
    std::size_t queue_cap = 256;
    if (const char* env = std::getenv("NEWDB_MAINTENANCE_QUEUE_MAX")) {
        const std::uint64_t v = static_cast<std::uint64_t>(std::strtoull(env, nullptr, 10));
        if (v > 0) {
            queue_cap = static_cast<std::size_t>(v);
        }
    }
    if (m_vacuum_queue.size() >= queue_cap) {
        m_scheduler_throttle_count.fetch_add(1, std::memory_order_relaxed);
        m_vacuum_pending.erase(table_name);
        return;
    }
    m_vacuum_last_run[table_name] = now;
    m_vacuum_trigger_count.fetch_add(1, std::memory_order_relaxed);
    m_vacuum_queue.push_back(table_name);
    const auto depth = static_cast<std::uint64_t>(m_vacuum_queue.size());
    m_vacuum_queue_depth.store(depth, std::memory_order_relaxed);
    std::uint64_t old_peak = m_vacuum_queue_depth_peak.load(std::memory_order_relaxed);
    while (depth > old_peak &&
           !m_vacuum_queue_depth_peak.compare_exchange_weak(
               old_peak, depth, std::memory_order_relaxed, std::memory_order_relaxed)) {
    }
    m_vacuum_cv.notify_one();
}


void TxnCoordinator::setVacuumCallback(std::function<void(const std::string&)> cb) {
    m_vacuum_callback = cb;
}


void TxnCoordinator::setVacuumOpsThreshold(const std::size_t threshold) {
    m_vacuum_ops_threshold.store(threshold == 0 ? 1 : threshold);
    persistVacuumConfig();
}


void TxnCoordinator::setVacuumMinIntervalSec(const std::size_t sec) {
    m_vacuum_min_interval_sec.store(sec);
    persistVacuumConfig();
}


void TxnCoordinator::set_workspace_root(std::string path) {
    m_workspace_root = std::move(path);
    loadVacuumConfig();
}



