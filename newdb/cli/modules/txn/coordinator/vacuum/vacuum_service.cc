#include <waterfall/config.h>

#include "cli/modules/txn/coordinator/txn_manager.h"
#include "cli/modules/txn/coordinator/internal/txn_internal.h"
#include "cli/modules/common/logging/logging.h"
#include "cli/modules/common/util/constants.h"
#include "cli/modules/sidecar/common/sidecar_wal_lsn.h"

#include "cli/modules/storage/table_storage_health.h"

#include <newdb/heap_table.h>
#include <newdb/page_io.h>
#include <newdb/schema_io.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <algorithm>

namespace {

bool vacuum_queue_use_health_env() {
    const char* e = std::getenv("NEWDB_VACUUM_QUEUE_USE_HEALTH");
    return e != nullptr && e[0] == '1' && e[1] == '\0';
}

/// When set to `1`, add a coarse `current_lsn - last_vacuum_lsn` term to the enqueue score (opt-in; not v1 default).
bool vacuum_score_wal_since_env() {
    const char* e = std::getenv("NEWDB_VACUUM_SCORE_WAL_SINCE");
    return e != nullptr && e[0] == '1' && e[1] == '\0';
}

std::uint64_t vacuum_health_slot_weight() {
    std::uint64_t w = 65536;
    if (const char* ew = std::getenv("NEWDB_VACUUM_HEALTH_SLOT_WEIGHT")) {
        const std::uint64_t v = static_cast<std::uint64_t>(std::strtoull(ew, nullptr, 10));
        if (v > 0) {
            w = v;
        }
    }
    return w;
}

std::uint64_t saturating_add_u64(std::uint64_t a, std::uint64_t b) {
    if (b > std::numeric_limits<std::uint64_t>::max() - a) {
        return std::numeric_limits<std::uint64_t>::max();
    }
    return a + b;
}

std::uint64_t saturating_mul_u64(std::uint64_t a, std::uint64_t b) {
    if (a == 0 || b == 0) {
        return 0;
    }
    if (a > std::numeric_limits<std::uint64_t>::max() / b) {
        return std::numeric_limits<std::uint64_t>::max();
    }
    return a * b;
}

struct VacuumScoreBreakdown {
    std::uint64_t file_bytes_term{0};
    std::uint64_t health_bonus_term{0};
    std::uint64_t wal_since_term{0};
    std::uint64_t total{0};
};

/// Single scoring function for vacuum queue ordering and `compact_debt_*` runtime fields.
/// When `current_wal_lsn > 0` and **`NEWDB_VACUUM_SCORE_WAL_SINCE=1`**, adds a capped `(wal_lsn - last_vacuum_lsn)` term after vacuum metadata exists.
VacuumScoreBreakdown compute_vacuum_score_breakdown(std::uint64_t file_bytes,
                                                   bool health_measured,
                                                   const newdb::TableStorageHealth& h,
                                                   std::uint64_t current_wal_lsn) {
    VacuumScoreBreakdown out{};
    out.file_bytes_term = file_bytes;
    out.total = file_bytes;
    if (!health_measured) {
        return out;
    }
    std::uint64_t bonus = 0;
    bonus = saturating_add_u64(bonus, h.dead_bytes);
    const std::uint64_t w = vacuum_health_slot_weight();
    bonus = saturating_add_u64(bonus, saturating_mul_u64(h.tombstone_slots, w));
    const std::uint64_t ratio_u = static_cast<std::uint64_t>(h.tombstone_ratio * 1000000.0 + 0.5);
    bonus = saturating_add_u64(bonus, ratio_u);
    out.health_bonus_term = bonus;
    out.total = saturating_add_u64(file_bytes, bonus);
    if (vacuum_score_wal_since_env() && current_wal_lsn > 0 && h.last_vacuum_lsn > 0 &&
        current_wal_lsn > h.last_vacuum_lsn) {
        const std::uint64_t gap = current_wal_lsn - h.last_vacuum_lsn;
        const std::uint64_t cap = 1ull << 30;
        const std::uint64_t g = (gap > cap) ? cap : gap;
        out.wal_since_term = g;
        out.total = saturating_add_u64(out.total, g);
    }
    return out;
}

bool vacuum_measure_health_for_enqueue(const std::string& table_name,
                                       const std::string& data_file,
                                       newdb::TableStorageHealth* out_h,
                                       bool* out_measured_ok) {
    if (out_measured_ok != nullptr) {
        *out_measured_ok = false;
    }
    if (!vacuum_queue_use_health_env()) {
        return false;
    }
    newdb::TableSchema schema;
    if (!newdb::load_schema_text(newdb::schema_sidecar_path_for_data_file(data_file), schema)) {
        return false;
    }
    newdb::HeapTable tbl;
    newdb::HeapLoadOptions opts{};
    opts.lazy_decode = true;
    if (!newdb::io::load_heap_file(data_file.c_str(), table_name, schema, tbl, opts).ok) {
        return false;
    }
    const newdb::TableStorageHealth h = newdb::measure_table_storage_health(tbl);
    if (out_measured_ok != nullptr) {
        *out_measured_ok = true;
    }
    if (out_h != nullptr) {
        *out_h = h;
    }
    return true;
}

}  // namespace

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
    m_vacuum_health_bonus_last.store(0);
    
    m_vacuum_thread = std::thread([this]() {
        while (m_vacuum_running.load()) {
            std::unique_lock<std::mutex> lk(m_vacuum_mutex);
            
            // Wait for work or wake periodically (up to ~60s per wait_for cycle).
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
                const auto best = std::max_element(
                    m_vacuum_queue.begin(), m_vacuum_queue.end(),
                    [](const std::pair<std::string, std::uint64_t>& a,
                       const std::pair<std::string, std::uint64_t>& b) { return a.second < b.second; });
                const std::string table = best->first;
                m_vacuum_queue.erase(best);
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

                if (compact_success) {
                    newdb::WalManager* wm = ensureWal();
                    const std::uint64_t vac_lsn = (wm != nullptr) ? wm->current_lsn() : 0ull;
                    newdb::TableStorageHealth h{};
                    bool measured_ok = false;
                    if (vacuum_measure_health_for_enqueue(table, data_file, &h, &measured_ok) && measured_ok) {
                        if (vac_lsn != 0ull) {
                            h.last_vacuum_lsn = vac_lsn;
                        }
                        h.last_vacuum_elapsed_ms = elapsed_ms;
                        recordLastStorageHealthSnapshot(h);
                    } else if (vac_lsn != 0ull || elapsed_ms != 0ull) {
                        mergeLastVacuumIntoStorageHealth(vac_lsn, elapsed_ms);
                    }
                }

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
    const std::string data_file = resolveDataFilePath(table_name);
    const std::uint64_t file_bytes = file_size_or_zero(data_file);
    bool health_measured = false;
    newdb::TableStorageHealth health{};
    std::uint64_t wal_now = 0;
    {
        newdb::WalManager* w = ensureWal();
        if (w != nullptr) {
            wal_now = w->current_lsn();
        }
    }
    if (vacuum_measure_health_for_enqueue(table_name, data_file, &health, &health_measured)) {
        recordLastStorageHealthSnapshot(health);
    }
    const VacuumScoreBreakdown debt_metrics =
        compute_vacuum_score_breakdown(file_bytes, health_measured, health, wal_now);
    const std::uint64_t queue_score = debt_metrics.total;
    m_vacuum_health_bonus_last.store(debt_metrics.health_bonus_term, std::memory_order_relaxed);
    m_vacuum_score_file_term_last.store(debt_metrics.file_bytes_term, std::memory_order_relaxed);
    m_vacuum_score_health_bonus_term_last.store(debt_metrics.health_bonus_term, std::memory_order_relaxed);
    m_vacuum_score_wal_since_term_last.store(debt_metrics.wal_since_term, std::memory_order_relaxed);
    m_vacuum_queue.push_back(std::make_pair(table_name, queue_score));
    const auto depth = static_cast<std::uint64_t>(m_vacuum_queue.size());
    m_vacuum_queue_depth.store(depth, std::memory_order_relaxed);
    {
        m_vacuum_priority_score_last.store(queue_score, std::memory_order_relaxed);
        m_compact_debt_bytes_last.store(queue_score, std::memory_order_relaxed);
        m_compact_debt_rows_last.store(health_measured ? health.tombstone_rows : 0ull, std::memory_order_relaxed);
        const std::uint64_t ratio_micro = health_measured
                                              ? static_cast<std::uint64_t>(health.fragmentation_ratio * 1000000.0 + 0.5)
                                              : 0ull;
        m_compact_debt_ratio_micro_last.store(ratio_micro, std::memory_order_relaxed);
        m_compact_debt_priority_last.store(queue_score, std::memory_order_relaxed);
    }
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



