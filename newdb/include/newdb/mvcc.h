#pragma once

#include <cstdint>
#include <unordered_set>
#include <unordered_map>
#include <memory>
#include <mutex>

namespace newdb {

class WalManager;
class HeapTable;

// Record metadata stored alongside each row version
struct RecordMetadata {
    uint64_t created_lsn = 0;
    uint64_t deleted_lsn = 0;
    uint64_t txn_id = 0;
    bool is_tombstone = false;
};

// MVCC Snapshot: represents a consistent view of the database at a point in time
struct MVCCSnapshot {
    uint64_t snapshot_lsn = 0;          // LSN at which snapshot was taken
    uint64_t min_visible_lsn = 0;       // Minimum LSN of committed transactions
    std::unordered_set<uint64_t> active_txns;  // Transactions active at snapshot time
    
    MVCCSnapshot() = default;
    MVCCSnapshot(uint64_t lsn, std::unordered_set<uint64_t>&& active)
        : snapshot_lsn(lsn), active_txns(std::move(active)) {}
    
    // Check if a record version is visible in this snapshot
    bool is_visible(uint64_t created_lsn, uint64_t deleted_lsn, uint64_t creator_txn) const;
    
    // Check if a record version is visible (with RecordMetadata)
    bool is_visible(const RecordMetadata& meta) const;
};

// MVCC Manager: tracks active transactions and creates snapshots
class MVCCManager {
public:
    explicit MVCCManager(WalManager& wal) : wal_(wal) {}
    
    // Begin a transaction: returns transaction ID
    uint64_t begin_transaction();
    
    // Commit a transaction: record commit LSN
    void commit_transaction(uint64_t txn_id, uint64_t commit_lsn);
    
    // Rollback a transaction
    void rollback_transaction(uint64_t txn_id);
    
    // Create a snapshot for current read operation
    MVCCSnapshot create_snapshot() const;
    
    // Get the current "high watermark" of committed LSNs
    uint64_t min_visible_lsn() const noexcept { return min_visible_lsn_; }
    
    // Clean up old transaction metadata (called after checkpoint)
    void purge_txns_before(uint64_t lsn);
    
private:
    WalManager& wal_;
    uint64_t next_txn_id_{1};
    std::unordered_set<uint64_t> active_txns_;
    std::unordered_map<uint64_t, uint64_t> committed_txn_lsn_;
    uint64_t min_visible_lsn_{0};  // All txns with LSN <= this are visible
    mutable std::mutex mut_;
};

// Scoped snapshot: automatically releases snapshot on destruction
class ScopedSnapshot {
public:
    explicit ScopedSnapshot(const MVCCManager& mgr) 
        : snapshot_(mgr.create_snapshot()) {}
    const MVCCSnapshot& get() const { return snapshot_; }
private:
    MVCCSnapshot snapshot_;
};

} // namespace newdb