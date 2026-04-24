#pragma once

#include <string>
#include <atomic>
#include <mutex>
#include <thread>
#include <condition_variable>
#include <vector>
#include <unordered_set>
#include <functional>
#include <memory>
#include <cstddef>
#include <unordered_map>

#include <newdb/wal_manager.h>

#include "result.h"
#include "constants.h"

// 事务状态
enum class TxnState {
    None,       // 无事务
    Active,     // 事务活跃中
    Committed,  // 已提交
    RolledBack  // 已回滚
};

// 事务记录
struct TxnRecord {
    int64_t txn_id;
    TxnState state;
    std::string table_name;
    std::string operation;  // INSERT/UPDATE/DELETE
    std::string key;       // 主键值
    std::string old_value; // 修改前的值 (用于回滚)
    std::string new_value; // 修改后的值
    int64_t timestamp;
};

// 事务管理器（嵌入 ShellState，无全局单例）
class TxnCoordinator {
public:
    TxnCoordinator();
    ~TxnCoordinator();
    TxnCoordinator(const TxnCoordinator&) = delete;
    TxnCoordinator& operator=(const TxnCoordinator&) = delete;

    // 事务控制
    Result<bool> begin(const std::string& table_name);
    Result<bool> commit();
    Result<bool> rollback();
    TxnState getState() const { return m_state.load(); }
    int64_t getTxnId() const { return m_txn_id.load(); }
    bool inTransaction() const { return m_state.load() == TxnState::Active; }
    
    // 文件锁
    Result<bool> acquireLock(const std::string& file_path);
    Result<bool> releaseLock(const std::string& file_path);
    bool isLocked(const std::string& file_path) const;
    
    // WAL (Write-Ahead Log)
    void writeWAL(const std::string& operation, const std::string& table,
                   const std::string& key, const std::string& old_val, const std::string& new_val);
    void flushWAL();
    bool recoverFromWAL();
    void setWalSyncMode(newdb::WalSyncMode mode);
    newdb::WalSyncMode walSyncMode();
    void setWalNormalSyncIntervalMs(std::uint64_t ms);
    std::uint64_t walNormalSyncIntervalMs();
    
    // 记录事务操作 (用于回滚)
    void recordOperation(const std::string& operation, const std::string& table,
                         const std::string& key, const std::string& old_val, const std::string& new_val);
    
    // 后台 VACUUM
    void startVacuumThread();
    void stopVacuumThread();
    void triggerVacuum(const std::string& table_name);
    void setVacuumCallback(std::function<void(const std::string&)> cb);
    void setVacuumOpsThreshold(std::size_t threshold);
    std::size_t vacuumOpsThreshold() const { return m_vacuum_ops_threshold.load(); }
    bool vacuumRunning() const { return m_vacuum_running.load(); }

    // Prefix for <table>.bin lock paths (same as ShellState::data_dir). Empty => cwd-relative names.
    void set_workspace_root(std::string path);
    const std::string& workspace_root() const { return m_workspace_root; }

private:
    struct LockHandleState {
        std::string lock_file_path;
#if defined(_WIN32)
        void* handle{nullptr};
#else
        int fd{-1};
#endif
    };
    // WAL 文件路径
    std::string getWALPath(const std::string& table_name) const;
    std::string walSyncConfigPath() const;
    bool loadWalSyncConfig(newdb::WalManager& wm) const;
    void persistWalSyncConfig(const newdb::WalManager& wm) const;
    std::string vacuumConfigPath() const;
    void loadVacuumConfig();
    void persistVacuumConfig() const;
    std::string resolveDataFilePath(const std::string& table_name) const;
    newdb::WalManager* ensureWal();
    void maybeCompactWalAfterCommit();
    void persistWalsnHighWaterUnlocked(newdb::WalManager* wm);
    std::uint64_t wal_compact_max_bytes{4ull * 1024ull * 1024ull};
    
    // 事务状态
    std::atomic<TxnState> m_state{TxnState::None};
    std::atomic<int64_t> m_txn_id{0};
    std::mutex m_txn_mutex;
    
    // 事务记录缓冲 (用于回滚)
    std::vector<TxnRecord> m_txn_records;
    
    // 文件锁
    std::mutex m_lock_mutex;
    std::vector<std::string> m_locked_files;
    std::unordered_map<std::string, LockHandleState> m_lock_handles;
    
    // WAL
    std::mutex m_wal_mutex;
    std::unique_ptr<newdb::WalManager> wal_;
    
    // VACUUM 线程
    std::thread m_vacuum_thread;
    std::atomic<bool> m_vacuum_running{false};
    std::mutex m_vacuum_mutex;
    std::condition_variable m_vacuum_cv;
    std::vector<std::string> m_vacuum_queue;
    std::unordered_set<std::string> m_vacuum_pending;
    std::function<void(const std::string&)> m_vacuum_callback;
    std::atomic<std::size_t> m_vacuum_ops_threshold{300};
    std::atomic<std::size_t> m_vacuum_op_counter{0};

    std::string m_workspace_root;
    std::string m_active_table;
};
