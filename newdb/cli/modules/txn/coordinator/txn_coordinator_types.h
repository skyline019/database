#pragma once

#include <cstdint>
#include <string>

// Shared txn enums/records used by TxnCoordinator and TxnCoordinatorState.

enum class TxnState {
    None,
    Active,
    Committed,
    RolledBack,
};

struct TxnRecord {
    int64_t txn_id;
    TxnState state;
    std::string table_name;
    std::string operation;
    std::string key;
    std::string old_value;
    std::string new_value;
    int64_t timestamp;
    std::uint64_t op_seq{0};
    std::uint64_t wal_lsn{0};
};

enum class WriteConflictPolicy {
    Reject,
    Wait,
};

enum class TxnIsolationLevel {
    ReadCommitted,
    Snapshot,
};

enum class WriteTimingStage : std::uint8_t {
    HeapAppend = 0,
    HotIndex = 1,
    SidecarInvalidate = 2,
    WalAppend = 3,
    LsmTrack = 4,
    LsmRotateCompact = 5,
    LsmFlush = 6,
    LsmCompaction = 7,
};
