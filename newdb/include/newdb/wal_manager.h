#pragma once

#include <newdb/error.h>
#include <newdb/row.h>
#include <newdb/schema.h>
#include <newdb/heap_table.h>
#include <newdb/tuple_codec.h>
#include <newdb/mvcc.h>

#include <cstdint>
#include <string>
#include <vector>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>
#include <condition_variable>
#include <deque>
#include <future>
#include <thread>
#include <atomic>

namespace newdb {

enum class WalSyncMode : uint8_t {
    Full = 0,   // fflush + fsync/_commit each flush
    Normal = 1, // fflush always, fsync/_commit periodically
    Off = 2     // fflush only
};

enum class WalOp : uint8_t {
    INSERT = 1,
    UPDATE = 2,
    DELETE = 3,
    COMMIT = 4,
    ROLLBACK = 5,
    CHECKPOINT = 6,
    SESSION_SNAPSHOT = 7
};

struct WalRecordHeader {
    uint32_t magic = 0x57414C30;      // "WAL0"
    uint64_t lsn = 0;                  // Log Sequence Number
    uint64_t txn_id = 0;               // Transaction ID
    uint32_t payload_len = 0;          // Payload length
    uint16_t checksum = 0;             // Header CRC16
    uint8_t  type = 0;                 // WalOp
    uint8_t  flags = 0;                // Reserved
#if defined(__GNUC__) || defined(__clang__)
} __attribute__((packed));
#else
};
#endif

class WalManager;

struct WalDecodedRecord {
    uint64_t lsn{0};
    uint64_t txn_id{0};
    WalOp op{WalOp::CHECKPOINT};
    std::string table;
    Row row;
    bool has_row{false};
};

struct WalRecoveryStats {
    std::uint64_t records_read{0};
    std::uint64_t checksum_failures{0};
    std::uint64_t decode_failures{0};
    std::uint64_t apply_count{0};
    std::uint64_t scanned_segments{0};
    std::uint64_t skipped_segments{0};
    std::uint64_t indexed_records{0};
    std::uint64_t indexed_segments{0};
    std::uint64_t indexed_offsets{0};
    std::uint64_t seek_skipped_records{0};
    std::uint64_t begin_ms{0};
    std::uint64_t end_ms{0};
    std::uint64_t elapsed_ms() const { return end_ms >= begin_ms ? (end_ms - begin_ms) : 0; }
};

// RAII wrapper for transaction scope
class WalTxn {
public:
    WalTxn(WalManager& wal, uint64_t txn_id);
    ~WalTxn();
    void commit();
    void rollback();
    uint64_t txn_id() const { return txn_id_; }

private:
    WalManager* wal_;
    uint64_t txn_id_;
    bool active_;
};

class WalManager {
public:
    explicit WalManager(std::string db_name, std::string wal_dir = "");
    ~WalManager();

    WalManager(const WalManager&) = delete;
    WalManager& operator=(const WalManager&) = delete;

    Status open();
    void close();

    Status append_record(uint64_t txn_id, WalOp op,
                         const std::string& table,
                         const Row* row = nullptr,
                         const std::vector<uint8_t>* raw_payload = nullptr);

    Status begin_transaction(uint64_t txn_id);
    Status commit_transaction(uint64_t txn_id);
    Status rollback_transaction(uint64_t txn_id);

    Status flush();
    void set_sync_mode(WalSyncMode mode) { sync_mode_ = mode; }
    WalSyncMode sync_mode() const { return sync_mode_; }
    void set_normal_sync_interval_ms(std::uint64_t ms) { normal_sync_interval_ms_ = (ms == 0 ? 1 : ms); }
    std::uint64_t normal_sync_interval_ms() const { return normal_sync_interval_ms_; }
    void set_segment_max_bytes(std::uint64_t bytes) { segment_max_bytes_ = bytes; }
    std::uint64_t segment_max_bytes() const { return segment_max_bytes_; }

    uint64_t current_lsn() const noexcept { return current_lsn_; }

    // Resync `current_lsn_` from on-disk records (call after `open` if WAL had history).
    Status resync_lsn_from_disk();

    // Size of the WAL file in bytes (0 if missing/closed).
    std::uint64_t wal_file_size_bytes() const;

    Status recover(HeapTable* out_table, const TableSchema& schema);
    Status recover(HeapTable* out_table, const TableSchema& schema, WalRecoveryStats* stats);
    Status read_all_records(const TableSchema& schema, std::vector<WalDecodedRecord>& out);
    std::optional<WalRecoveryStats> last_recovery_stats() const;

    static std::uint64_t wall_clock_ms();

    Status checkpoint(uint64_t snapshot_lsn);
    Status checkpoint_and_truncate(uint64_t snapshot_lsn);

    static bool wal_exists(const std::string& db_name, const std::string& wal_dir = "");

    const std::string& wal_path() const { return wal_path_; }

    Status delete_wal();

private:
    struct QueuedWalOp {
        WalRecordHeader hdr{};
        std::vector<uint8_t> payload;
        bool flush_only{false};
        std::shared_ptr<std::promise<Status>> done;
    };

    std::string db_name_;
    std::string wal_dir_;
    std::string wal_path_;
    FILE* fp_{nullptr};
    uint64_t current_lsn_{0};
    WalSyncMode sync_mode_{WalSyncMode::Full};
    std::uint64_t last_durable_flush_ms_{0};
    std::uint64_t normal_sync_interval_ms_{50};
    std::uint64_t segment_max_bytes_{0};
    std::uint32_t segment_index_{0};
    mutable std::mutex mut_;
    mutable std::optional<WalRecoveryStats> last_recovery_stats_;
    std::mutex queue_mu_;
    std::condition_variable queue_cv_;
    std::deque<QueuedWalOp> queue_;
    std::thread writer_thread_;
    std::atomic<bool> writer_stopping_{false};
    std::atomic<bool> writer_started_{false};
    std::atomic<std::uint64_t> next_lsn_{0};

    uint16_t compute_checksum(const WalRecordHeader* hdr, const uint8_t* payload, uint32_t paylen);
    bool verify_checksum(const WalRecordHeader* hdr, const uint8_t* payload);
    Status write_record(const WalRecordHeader* hdr, const uint8_t* payload, uint32_t paylen);
    Status read_record(FILE* fp, WalRecordHeader* hdr, std::vector<uint8_t>& payload);
    Status append_control_record_nolock(uint64_t txn_id, WalOp op);
    Status enqueue_and_wait(QueuedWalOp&& op);
    void writer_loop();
    void resync_lsn_from_open_file_nolock();
    Status flush_durable_nolock();
    std::string current_wal_segment_path_nolock() const;
    bool open_current_segment_nolock(const char* mode);
    Status maybe_rotate_segment_nolock();
    std::vector<std::string> wal_read_paths_nolock() const;
    void detect_segment_index_nolock();

    Status serialize_row(const Row& row, const TableSchema& schema, std::vector<uint8_t>& out) const;
    Status deserialize_row(const uint8_t* data, uint32_t len, Row& out, const TableSchema& schema) const;

    Status encode_row_payload(const Row& row, const TableSchema& schema, std::vector<uint8_t>& out) const;
    Status decode_row_payload(const uint8_t* data, uint32_t len, Row& out, const TableSchema& schema) const;
};

} // namespace newdb