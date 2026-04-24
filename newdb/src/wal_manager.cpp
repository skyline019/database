#include <newdb/wal_manager.h>
#include <newdb/heap_page.h>
#include <newdb/wal_codec.h>

#include <cstdio>
#include <cstring>
#include <cstdint>
#include <vector>
#include <string>
#include <mutex>
#include <chrono>
#include <algorithm>
#include <unordered_map>
#include <unordered_set>
#include <filesystem>
#if defined(_WIN32)
#include <io.h>
#else
#include <unistd.h>
#endif

namespace newdb {

namespace {

uint16_t crc16_ccitt(const uint8_t* data, size_t len, uint16_t seed = 0xFFFF) {
    for (size_t i = 0; i < len; ++i) {
        seed ^= static_cast<uint16_t>(data[i] << 8);
        for (int j = 0; j < 8; ++j) {
            if (seed & 0x8000) {
                seed = static_cast<uint16_t>((seed << 1) ^ 0x1021);
            } else {
                seed <<= 1;
            }
        }
    }
    return seed;
}

std::uint64_t monotonic_ms() {
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
}

std::uint64_t wall_clock_ms_impl() {
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
}

bool parse_u64_env(const char* name, std::uint64_t& out) {
    const char* raw = std::getenv(name);
    if (raw == nullptr || raw[0] == '\0') return false;
    char* end = nullptr;
    const unsigned long long v = std::strtoull(raw, &end, 10);
    if (end == raw || (end != nullptr && *end != '\0')) return false;
    out = static_cast<std::uint64_t>(v);
    return true;
}

} // namespace

std::uint64_t WalManager::wall_clock_ms() {
    return wall_clock_ms_impl();
}

WalManager::WalManager(std::string db_name, std::string wal_dir)
    : db_name_(std::move(db_name)), wal_dir_(std::move(wal_dir)) {
    wal_path_ = wal_dir_.empty() ? (db_name_ + ".wal") : (wal_dir_ + "/" + db_name_ + ".wal");
}

WalManager::~WalManager() { close(); }

WalTxn::WalTxn(WalManager& wal, uint64_t txn_id) : wal_(&wal), txn_id_(txn_id), active_(true) {
    wal_->begin_transaction(txn_id);
}
WalTxn::~WalTxn() { if (active_) wal_->rollback_transaction(txn_id_); }
void WalTxn::commit() { if (active_) { wal_->commit_transaction(txn_id_); active_ = false; } }
void WalTxn::rollback() { if (active_) { wal_->rollback_transaction(txn_id_); active_ = false; } }

Status WalManager::open() {
    std::lock_guard<std::mutex> lg(mut_);
    if (fp_ != nullptr) return Status::Fail("WAL already open");
    if (segment_max_bytes_ == 0) {
        std::uint64_t seg = 0;
        if (parse_u64_env("NEWDB_WAL_SEGMENT_BYTES", seg) && seg > 0) {
            segment_max_bytes_ = seg;
        }
    }
    detect_segment_index_nolock();
    if (!open_current_segment_nolock("ab+")) {
        return Status::Fail("cannot open WAL file: " + current_wal_segment_path_nolock());
    }
    resync_lsn_from_open_file_nolock();
    next_lsn_.store(current_lsn_, std::memory_order_relaxed);
    writer_stopping_.store(false, std::memory_order_release);
    writer_started_.store(true, std::memory_order_release);
    writer_thread_ = std::thread(&WalManager::writer_loop, this);
    std::fseek(fp_, 0, SEEK_END);
    return Status::Ok();
}

void WalManager::close() {
    if (writer_started_.load(std::memory_order_acquire)) {
        (void)flush();
        writer_stopping_.store(true, std::memory_order_release);
        queue_cv_.notify_all();
        if (writer_thread_.joinable()) {
            writer_thread_.join();
        }
        writer_started_.store(false, std::memory_order_release);
    }
    std::lock_guard<std::mutex> lg(mut_);
    if (fp_ != nullptr) {
        std::fclose(fp_);
        fp_ = nullptr;
    }
}

Status WalManager::append_record(uint64_t txn_id, WalOp op, const std::string& table,
                                 const Row* row, const std::vector<uint8_t>* raw_payload) {
    std::vector<uint8_t> row_bytes;
    const std::vector<uint8_t>* row_payload = raw_payload;
    uint32_t row_id_u32 = 0;
    const uint32_t* row_id = nullptr;
    if (row != nullptr) {
        row_id_u32 = static_cast<uint32_t>(row->id);
        row_id = &row_id_u32;
        if (raw_payload == nullptr) {
            Status st = encode_row_payload(*row, TableSchema{}, row_bytes);
            if (!st.ok) return st;
            row_payload = &row_bytes;
        }
    }

    std::vector<uint8_t> payload;
    Status payload_st = walcodec::build_payload(table, row_id, row_payload, payload);
    if (!payload_st.ok) return payload_st;

    QueuedWalOp qop;
    WalRecordHeader& hdr = qop.hdr;
    hdr.magic = 0x57414C30;
    hdr.lsn = next_lsn_.fetch_add(1, std::memory_order_relaxed) + 1;
    hdr.txn_id = txn_id;
    hdr.payload_len = static_cast<uint32_t>(payload.size());
    hdr.type = static_cast<uint8_t>(op);
    hdr.checksum = compute_checksum(&hdr, payload.data(), static_cast<uint32_t>(payload.size()));
    qop.payload = std::move(payload);
    qop.flush_only = false;
    return enqueue_and_wait(std::move(qop));
}

Status WalManager::begin_transaction(uint64_t) { return Status::Ok(); }
Status WalManager::commit_transaction(uint64_t txn_id) {
    return append_control_record_nolock(txn_id, WalOp::COMMIT);
}
Status WalManager::rollback_transaction(uint64_t txn_id) {
    return append_control_record_nolock(txn_id, WalOp::ROLLBACK);
}

Status WalManager::flush_durable_nolock() {
    if (fp_ == nullptr) return Status::Fail("WAL not open");
    if (std::fflush(fp_) != 0) return Status::Fail("fflush failed");
    if (sync_mode_ == WalSyncMode::Off) return Status::Ok();
    if (sync_mode_ == WalSyncMode::Normal) {
        const std::uint64_t now_ms = monotonic_ms();
        if (last_durable_flush_ms_ != 0 && now_ms - last_durable_flush_ms_ < normal_sync_interval_ms_) return Status::Ok();
        last_durable_flush_ms_ = now_ms;
    }
#if defined(_WIN32)
    if (_commit(_fileno(fp_)) != 0) return Status::Fail("commit failed");
#else
    if (::fsync(fileno(fp_)) != 0) return Status::Fail("fsync failed");
#endif
    return Status::Ok();
}

Status WalManager::flush() {
    if (!writer_started_.load(std::memory_order_acquire)) {
        std::lock_guard<std::mutex> lg(mut_);
        return flush_durable_nolock();
    }
    QueuedWalOp op;
    op.flush_only = true;
    return enqueue_and_wait(std::move(op));
}
Status WalManager::checkpoint(uint64_t snapshot_lsn) {
    (void)snapshot_lsn;
    return append_control_record_nolock(0, WalOp::CHECKPOINT);
}

Status WalManager::resync_lsn_from_disk() {
    bool need_flush = false;
    {
        std::lock_guard<std::mutex> lk(mut_);
        need_flush = (fp_ != nullptr);
    }
    if (need_flush) {
        const Status fst = flush();
        if (!fst.ok) return fst;
    }
    std::lock_guard<std::mutex> lg(mut_);
    if (fp_ == nullptr) return Status::Ok();
    resync_lsn_from_open_file_nolock();
    std::fseek(fp_, 0, SEEK_END);
    return Status::Ok();
}

std::uint64_t WalManager::wal_file_size_bytes() const {
    std::lock_guard<std::mutex> lg(mut_);
    const std::string current_path = current_wal_segment_path_nolock();
    long current_pos = -1;
    if (fp_ != nullptr) {
        current_pos = std::ftell(fp_);
    }
    std::uint64_t total = 0;
    for (const auto& p : wal_read_paths_nolock()) {
        std::error_code ec;
        const auto sz = std::filesystem::file_size(p, ec);
        if (ec) continue;
        std::uint64_t use_sz = static_cast<std::uint64_t>(sz);
        if (current_pos >= 0 && p == current_path) {
            use_sz = std::max(use_sz, static_cast<std::uint64_t>(current_pos));
        }
        total += use_sz;
    }
    return total;
}

void WalManager::resync_lsn_from_open_file_nolock() {
    if (fp_ == nullptr) return;
    if (std::fseek(fp_, 0, SEEK_SET) != 0) return;
    std::uint64_t max_lsn = 0;
    for (;;) {
        WalRecordHeader hdr{};
        std::vector<uint8_t> pl;
        const Status st = read_record(fp_, &hdr, pl);
        if (!st.ok) break;
        if (!verify_checksum(&hdr, pl.data())) break;
        max_lsn = std::max(max_lsn, hdr.lsn);
    }
    std::clearerr(fp_);
    current_lsn_ = std::max(current_lsn_, max_lsn);
    (void)std::fseek(fp_, 0, SEEK_END);
}

Status WalManager::checkpoint_and_truncate(uint64_t snapshot_lsn) {
    (void)snapshot_lsn;
    const Status fst = flush();
    if (!fst.ok) return fst;
    std::lock_guard<std::mutex> lg(mut_);
    if (fp_ == nullptr) return Status::Fail("WAL not open");
    if (std::fflush(fp_) != 0) return Status::Fail("fflush failed before wal truncate");
#if defined(_WIN32)
    if (sync_mode_ != WalSyncMode::Off && _commit(_fileno(fp_)) != 0) return Status::Fail("commit failed before wal truncate");
#else
    if (sync_mode_ != WalSyncMode::Off && ::fsync(fileno(fp_)) != 0) return Status::Fail("fsync failed before wal truncate");
#endif
    std::fclose(fp_);
    fp_ = nullptr;
    if (std::FILE* z = std::fopen(wal_path_.c_str(), "wb")) std::fclose(z);
    fp_ = std::fopen(wal_path_.c_str(), "ab+");
    if (fp_ == nullptr) return Status::Fail("cannot reopen wal after truncate: " + wal_path_);
    (void)std::fseek(fp_, 0, SEEK_END);
    return Status::Ok();
}

bool WalManager::wal_exists(const std::string& db_name, const std::string& wal_dir) {
    const std::string path = wal_dir.empty() ? (db_name + ".wal") : (wal_dir + "/" + db_name + ".wal");
    if (std::FILE* fp = std::fopen(path.c_str(), "rb"); fp) {
        std::fseek(fp, 0, SEEK_END);
        const long sz = std::ftell(fp);
        std::fclose(fp);
        return sz > 0;
    }
    return false;
}

Status WalManager::recover(HeapTable* out_table, const TableSchema& schema) { return recover(out_table, schema, nullptr); }

Status WalManager::recover(HeapTable* out_table, const TableSchema& schema, WalRecoveryStats* stats) {
    if (out_table == nullptr) return Status::Fail("null out_table");
    bool need_flush = false;
    {
        std::lock_guard<std::mutex> lk(mut_);
        need_flush = (fp_ != nullptr);
    }
    if (need_flush) {
        const Status fst = flush();
        if (!fst.ok) return fst;
    }
    std::lock_guard<std::mutex> lg(mut_);
    WalRecoveryStats local_stats;
    WalRecoveryStats* out_stats = stats ? stats : &local_stats;
    out_stats->begin_ms = wall_clock_ms();

    struct PendingTxnOps {
        std::unordered_map<int, std::pair<WalOp, Row>> by_row_id;
    };
    std::unordered_map<uint64_t, PendingTxnOps> txn_ops;
    struct SegmentIndexEntry {
        std::string path;
        std::uint64_t min_lsn{0};
        std::uint64_t max_lsn{0};
        std::uint64_t records{0};
        std::vector<std::pair<std::uint64_t, long>> lsn_offsets;
    };
    const bool enable_offset_seek = []() {
        const char* raw = std::getenv("NEWDB_RECOVER_ENABLE_OFFSET_SEEK");
        return raw != nullptr && std::strcmp(raw, "1") == 0;
    }();
    const std::uint64_t min_replay_lsn = []() {
        std::uint64_t v = 0;
        if (parse_u64_env("NEWDB_RECOVER_MIN_LSN", v)) return v;
        return static_cast<std::uint64_t>(0);
    }();
    std::vector<SegmentIndexEntry> seg_index;
    for (const auto& path : wal_read_paths_nolock()) {
        FILE* rf = std::fopen(path.c_str(), "rb");
        if (rf == nullptr) continue;
        WalRecordHeader hdr{};
        std::vector<uint8_t> payload;
        SegmentIndexEntry ent{};
        ent.path = path;
        while (true) {
            const long rec_pos = std::ftell(rf);
            payload.clear();
            Status st = read_record(rf, &hdr, payload);
            if (!st.ok) break;
            if (ent.records == 0) {
                ent.min_lsn = hdr.lsn;
                ent.max_lsn = hdr.lsn;
            } else {
                ent.min_lsn = std::min(ent.min_lsn, hdr.lsn);
                ent.max_lsn = std::max(ent.max_lsn, hdr.lsn);
            }
            ++ent.records;
            if (ent.records == 1 || (ent.records % 128u) == 0u) {
                const std::uint64_t lsn_value = hdr.lsn;
                ent.lsn_offsets.emplace_back(lsn_value, rec_pos);
            }
        }
        std::fclose(rf);
        if (ent.records == 0) {
            ++out_stats->skipped_segments;
            continue;
        }
        ++out_stats->indexed_segments;
        out_stats->indexed_records += ent.records;
        out_stats->indexed_offsets += ent.lsn_offsets.size();
        seg_index.push_back(std::move(ent));
    }
    std::sort(seg_index.begin(), seg_index.end(), [](const SegmentIndexEntry& a, const SegmentIndexEntry& b) {
        return a.min_lsn < b.min_lsn;
    });

    for (const auto& ent : seg_index) {
        ++out_stats->scanned_segments;
        FILE* rf = std::fopen(ent.path.c_str(), "rb");
        if (rf == nullptr) continue;
        if (enable_offset_seek && min_replay_lsn > 0 && !ent.lsn_offsets.empty() && ent.max_lsn < min_replay_lsn) {
            std::fclose(rf);
            ++out_stats->skipped_segments;
            out_stats->seek_skipped_records += ent.records;
            continue;
        }
        if (enable_offset_seek && min_replay_lsn > 0 && !ent.lsn_offsets.empty() && ent.min_lsn < min_replay_lsn) {
            const auto it = std::lower_bound(
                ent.lsn_offsets.begin(), ent.lsn_offsets.end(), min_replay_lsn,
                [](const std::pair<std::uint64_t, long>& a, const std::uint64_t target) {
                    return a.first < target;
                });
            if (it != ent.lsn_offsets.end()) {
                (void)std::fseek(rf, it->second, SEEK_SET);
            }
        }
        WalRecordHeader hdr;
        std::vector<uint8_t> payload;
        while (true) {
            payload.clear();
            Status st = read_record(rf, &hdr, payload);
            if (!st.ok) break;
            ++out_stats->records_read;
            if (!verify_checksum(&hdr, payload.data())) {
                std::fclose(rf);
                ++out_stats->checksum_failures;
                out_stats->end_ms = wall_clock_ms();
                last_recovery_stats_ = *out_stats;
                return Status::Fail("WAL record checksum mismatch at LSN " + std::to_string(hdr.lsn));
            }

            switch (static_cast<WalOp>(hdr.type)) {
            case WalOp::INSERT:
            case WalOp::UPDATE:
            case WalOp::DELETE: {
                const uint8_t* p = payload.data();
                const uint8_t* end = payload.data() + payload.size();
                std::string table_name;
                Status dec_table = walcodec::decode_table_name(p, end, table_name);
                if (!dec_table.ok) {
                    ++out_stats->decode_failures;
                    out_stats->end_ms = wall_clock_ms();
                    last_recovery_stats_ = *out_stats;
                    return Status::Fail("failed to decode table name from WAL: " + dec_table.message);
                }
                if (table_name != out_table->name) break;

                uint32_t row_id = 0;
                const uint8_t* row_payload_ptr = nullptr;
                uint32_t row_payload_len = 0;
                Status dec_row = walcodec::decode_row_fields(p, end, row_id, row_payload_ptr, row_payload_len);
                if (!dec_row.ok) {
                    ++out_stats->decode_failures;
                    out_stats->end_ms = wall_clock_ms();
                    last_recovery_stats_ = *out_stats;
                    return Status::Fail("failed to decode row fields from WAL: " + dec_row.message);
                }
                Row row;
                Status dec = decode_row_payload(row_payload_ptr, row_payload_len, row, schema);
                if (!dec.ok) {
                    ++out_stats->decode_failures;
                    out_stats->end_ms = wall_clock_ms();
                    last_recovery_stats_ = *out_stats;
                    return Status::Fail("failed to decode row from WAL");
                }
                PendingTxnOps& pending = txn_ops[hdr.txn_id];
                pending.by_row_id[row.id] = std::make_pair(static_cast<WalOp>(hdr.type), std::move(row));
                    break;
                }
                case WalOp::COMMIT: {
                auto it = txn_ops.find(hdr.txn_id);
                if (it != txn_ops.end()) {
                    for (auto& [row_id, op_row] : it->second.by_row_id) {
                        (void)row_id;
                        WalOp op = op_row.first;
                        Row& row = op_row.second;
                        switch (op) {
                            case WalOp::INSERT: {
                                const auto idx = out_table->index_by_id.find(row.id);
                                if (idx != out_table->index_by_id.end()) {
                                    out_table->rows[static_cast<std::size_t>(idx->second)] = row;
                                } else {
                                    out_table->rows.push_back(std::move(row));
                                }
                                ++out_stats->apply_count;
                                break;
                            }
                            case WalOp::UPDATE: {
                                const auto idx = out_table->index_by_id.find(row.id);
                                if (idx != out_table->index_by_id.end()) {
                                    out_table->rows[static_cast<std::size_t>(idx->second)] = row;
                                } else {
                                    out_table->rows.push_back(std::move(row));
                                }
                                ++out_stats->apply_count;
                                break;
                            }
                            case WalOp::DELETE: {
                                const Row* target = out_table->find_by_id(row.id);
                                if (target) {
                                    Row tombstone;
                                    tombstone.id = row.id;
                                    tombstone.attrs["__deleted"] = "1";
                                    out_table->rows.push_back(std::move(tombstone));
                                    ++out_stats->apply_count;
                                }
                                break;
                            }
                            default: break;
                        }
                    }
                    txn_ops.erase(it);
                }
                    break;
                }
                case WalOp::ROLLBACK:
                    txn_ops.erase(hdr.txn_id);
                    break;
                case WalOp::CHECKPOINT:
                case WalOp::SESSION_SNAPSHOT:
                    break;
            }
            current_lsn_ = std::max(current_lsn_, hdr.lsn);
        }
        std::fclose(rf);
    }

    out_table->rebuild_indexes(schema);
    out_stats->end_ms = wall_clock_ms();
    last_recovery_stats_ = *out_stats;
    return Status::Ok();
}

Status WalManager::read_all_records(const TableSchema& schema, std::vector<WalDecodedRecord>& out) {
    bool need_flush = false;
    {
        std::lock_guard<std::mutex> lk(mut_);
        need_flush = (fp_ != nullptr);
    }
    if (need_flush) {
        const Status fst = flush();
        if (!fst.ok) return fst;
    }
    std::lock_guard<std::mutex> lg(mut_);
    out.clear();
    for (const auto& path : wal_read_paths_nolock()) {
        FILE* rf = std::fopen(path.c_str(), "rb");
        if (rf == nullptr) continue;
        WalRecordHeader hdr;
        std::vector<uint8_t> payload;
        while (true) {
            payload.clear();
            Status st = read_record(rf, &hdr, payload);
            if (!st.ok) break;
            if (!verify_checksum(&hdr, payload.data())) {
                std::fclose(rf);
                return Status::Fail("WAL checksum mismatch");
            }

            WalDecodedRecord rec;
            rec.lsn = hdr.lsn;
            rec.txn_id = hdr.txn_id;
            rec.op = static_cast<WalOp>(hdr.type);
            if (!payload.empty()) {
            const uint8_t* p = payload.data();
            const uint8_t* end = payload.data() + payload.size();
            std::string table_name;
            Status dec_table = walcodec::decode_table_name(p, end, table_name);
            if (!dec_table.ok) return Status::Fail("decode table name failed: " + dec_table.message);
            rec.table = std::move(table_name);
            if (rec.op == WalOp::INSERT || rec.op == WalOp::UPDATE || rec.op == WalOp::DELETE) {
                uint32_t row_id = 0;
                const uint8_t* row_payload_ptr = nullptr;
                uint32_t plen = 0;
                Status dec_row = walcodec::decode_row_fields(p, end, row_id, row_payload_ptr, plen);
                if (!dec_row.ok) return Status::Fail("decode wal row fields failed: " + dec_row.message);
                Row row;
                Status dr = decode_row_payload(row_payload_ptr, plen, row, schema);
                if (!dr.ok) return dr;
                row.id = static_cast<int>(row_id);
                rec.row = std::move(row);
                rec.has_row = true;
            }
            }
            out.push_back(std::move(rec));
            current_lsn_ = std::max(current_lsn_, hdr.lsn);
        }
        std::fclose(rf);
    }
    std::sort(out.begin(), out.end(), [](const WalDecodedRecord& a, const WalDecodedRecord& b) {
        return a.lsn < b.lsn;
    });
    return Status::Ok();
}

std::optional<WalRecoveryStats> WalManager::last_recovery_stats() const {
    std::lock_guard<std::mutex> lg(mut_);
    return last_recovery_stats_;
}

Status WalManager::encode_row_payload(const Row& row, const TableSchema&, std::vector<uint8_t>& out) const {
    return codec::encode_row_to_heap_payload(row, out);
}

Status WalManager::decode_row_payload(const uint8_t* data, uint32_t len, Row& out, const TableSchema&) const {
    return codec::decode_heap_payload_to_row(data, len, out) ? Status::Ok() : Status::Fail("decode failed");
}

uint16_t WalManager::compute_checksum(const WalRecordHeader* hdr, const uint8_t* payload, uint32_t paylen) {
    const uint8_t* base = reinterpret_cast<const uint8_t*>(hdr);
    size_t hdr_no_chksum = offsetof(WalRecordHeader, checksum);
    uint16_t seed = crc16_ccitt(base, hdr_no_chksum);
    seed = crc16_ccitt(base + hdr_no_chksum + sizeof(uint16_t), sizeof(WalRecordHeader) - hdr_no_chksum - sizeof(uint16_t), seed);
    if (paylen > 0 && payload) seed = crc16_ccitt(payload, paylen, seed);
    return seed;
}

bool WalManager::verify_checksum(const WalRecordHeader* hdr, const uint8_t* payload) {
    return compute_checksum(hdr, payload, hdr->payload_len) == hdr->checksum;
}

Status WalManager::write_record(const WalRecordHeader* hdr, const uint8_t* payload, uint32_t paylen) {
    if (std::fwrite(hdr, sizeof(WalRecordHeader), 1, fp_) != 1) return Status::Fail("fwrite header failed");
    if (paylen > 0 && payload) {
        if (std::fwrite(payload, 1, paylen, fp_) != paylen) return Status::Fail("fwrite payload failed");
    }
    return Status::Ok();
}

Status WalManager::read_record(FILE* fp, WalRecordHeader* hdr, std::vector<uint8_t>& payload) {
    if (fp == nullptr) return Status::Fail("null file");
    if (std::fread(hdr, sizeof(WalRecordHeader), 1, fp) != 1) {
        if (std::feof(fp)) return Status::Fail("EOF");
        return Status::Fail("fread header failed");
    }
    if (hdr->magic != 0x57414C30) return Status::Fail("invalid WAL magic");
    payload.resize(hdr->payload_len);
    if (hdr->payload_len > 0) {
        if (std::fread(payload.data(), 1, hdr->payload_len, fp) != hdr->payload_len) return Status::Fail("fread payload failed");
    }
    return Status::Ok();
}

Status WalManager::append_control_record_nolock(uint64_t txn_id, WalOp op) {
    if (!writer_started_.load(std::memory_order_acquire)) {
        if (fp_ == nullptr) return Status::Fail("WAL not open");
        WalRecordHeader hdr;
        hdr.magic = 0x57414C30;
        hdr.lsn = ++current_lsn_;
        hdr.txn_id = txn_id;
        hdr.payload_len = 0;
        hdr.type = static_cast<uint8_t>(op);
        hdr.checksum = compute_checksum(&hdr, nullptr, 0);
        const Status wr = write_record(&hdr, nullptr, 0);
        if (!wr.ok) return wr;
        return maybe_rotate_segment_nolock();
    }
    QueuedWalOp qop;
    WalRecordHeader& hdr = qop.hdr;
    hdr.magic = 0x57414C30;
    hdr.lsn = next_lsn_.fetch_add(1, std::memory_order_relaxed) + 1;
    hdr.txn_id = txn_id;
    hdr.payload_len = 0;
    hdr.type = static_cast<uint8_t>(op);
    hdr.checksum = compute_checksum(&hdr, nullptr, 0);
    qop.flush_only = false;
    return enqueue_and_wait(std::move(qop));
}

Status WalManager::enqueue_and_wait(QueuedWalOp&& op) {
    if (!writer_started_.load(std::memory_order_acquire)) {
        return Status::Fail("WAL writer not started");
    }
    if (writer_stopping_.load(std::memory_order_acquire)) {
        return Status::Fail("WAL writer stopping");
    }
    auto done = std::make_shared<std::promise<Status>>();
    auto fut = done->get_future();
    op.done = done;
    {
        std::lock_guard<std::mutex> lk(queue_mu_);
        queue_.push_back(std::move(op));
    }
    queue_cv_.notify_one();
    return fut.get();
}

void WalManager::writer_loop() {
    for (;;) {
        QueuedWalOp op;
        {
            std::unique_lock<std::mutex> lk(queue_mu_);
            queue_cv_.wait(lk, [&]() {
                return writer_stopping_.load(std::memory_order_acquire) || !queue_.empty();
            });
            if (queue_.empty()) {
                if (writer_stopping_.load(std::memory_order_acquire)) break;
                continue;
            }
            op = std::move(queue_.front());
            queue_.pop_front();
        }

        Status st = Status::Ok();
        {
            std::lock_guard<std::mutex> lg(mut_);
            if (fp_ == nullptr) {
                st = Status::Fail("WAL not open");
            } else {
                if (!op.flush_only) {
                    st = write_record(&op.hdr,
                                      op.payload.empty() ? nullptr : op.payload.data(),
                                      static_cast<uint32_t>(op.payload.size()));
                    if (st.ok) {
                        current_lsn_ = std::max(current_lsn_, op.hdr.lsn);
                        st = maybe_rotate_segment_nolock();
                    }
                } else if (st.ok) {
                    st = flush_durable_nolock();
                }
            }
        }
        if (op.done) {
            op.done->set_value(st);
        }
    }
}

std::string WalManager::current_wal_segment_path_nolock() const {
    if (segment_index_ == 0) return wal_path_;
    char suffix[16];
    std::snprintf(suffix, sizeof(suffix), ".%06u", segment_index_);
    return wal_path_ + suffix;
}

bool WalManager::open_current_segment_nolock(const char* mode) {
    fp_ = std::fopen(current_wal_segment_path_nolock().c_str(), mode);
    return fp_ != nullptr;
}

Status WalManager::maybe_rotate_segment_nolock() {
    if (segment_max_bytes_ == 0 || fp_ == nullptr) return Status::Ok();
    const long here = std::ftell(fp_);
    if (here < 0) return Status::Ok();
    if (static_cast<std::uint64_t>(here) < segment_max_bytes_) return Status::Ok();
    if (std::fflush(fp_) != 0) return Status::Fail("fflush failed before segment rotate");
    std::fclose(fp_);
    fp_ = nullptr;
    ++segment_index_;
    if (!open_current_segment_nolock("ab+")) {
        return Status::Fail("cannot open rotated WAL segment: " + current_wal_segment_path_nolock());
    }
    return Status::Ok();
}

void WalManager::detect_segment_index_nolock() {
    segment_index_ = 0;
    namespace fs = std::filesystem;
    std::error_code ec;
    fs::path p(wal_path_);
    fs::path dir = p.parent_path();
    if (dir.empty()) dir = fs::current_path(ec);
    const std::string base = p.filename().string();
    std::uint32_t best = 0;
    for (const auto& ent : fs::directory_iterator(dir, ec)) {
        if (ec || !ent.is_regular_file(ec)) continue;
        const std::string fn = ent.path().filename().string();
        if (fn.rfind(base + ".", 0) != 0) continue;
        const std::string tail = fn.substr(base.size() + 1);
        if (tail.empty()) continue;
        bool all_digit = true;
        for (const char ch : tail) {
            if (ch < '0' || ch > '9') { all_digit = false; break; }
        }
        if (!all_digit) continue;
        const std::uint32_t idx = static_cast<std::uint32_t>(std::strtoul(tail.c_str(), nullptr, 10));
        best = std::max(best, idx);
    }
    segment_index_ = best;
}

std::vector<std::string> WalManager::wal_read_paths_nolock() const {
    namespace fs = std::filesystem;
    std::error_code ec;
    fs::path p(wal_path_);
    fs::path dir = p.parent_path();
    if (dir.empty()) dir = fs::current_path(ec);
    const std::string base = p.filename().string();
    std::vector<std::pair<std::uint32_t, std::string>> segs;
    if (fs::exists(p, ec)) {
        segs.emplace_back(0u, wal_path_);
    }
    for (const auto& ent : fs::directory_iterator(dir, ec)) {
        if (ec || !ent.is_regular_file(ec)) continue;
        const std::string fn = ent.path().filename().string();
        if (fn.rfind(base + ".", 0) != 0) continue;
        const std::string tail = fn.substr(base.size() + 1);
        if (tail.empty()) continue;
        bool all_digit = true;
        for (const char ch : tail) {
            if (ch < '0' || ch > '9') { all_digit = false; break; }
        }
        if (!all_digit) continue;
        const std::uint32_t idx = static_cast<std::uint32_t>(std::strtoul(tail.c_str(), nullptr, 10));
        segs.emplace_back(idx, ent.path().string());
    }
    std::sort(segs.begin(), segs.end(), [](const auto& a, const auto& b) { return a.first < b.first; });
    std::vector<std::string> out;
    out.reserve(segs.size());
    for (const auto& kv : segs) out.push_back(kv.second);
    return out;
}

Status WalManager::delete_wal() {
    close();
    if (std::remove(wal_path_.c_str()) != 0) return Status::Fail("failed to delete WAL");
    return Status::Ok();
}

} // namespace newdb
