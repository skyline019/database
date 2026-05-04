#include <newdb/wal_codec.h>

namespace newdb::walcodec {

namespace {
constexpr std::uint8_t kPayloadMarkerV1 = 0xA1;
constexpr std::uint8_t kFlagBeforeImage = 1u << 0;
constexpr std::uint8_t kFlagAfterImage = 1u << 1;
constexpr std::uint8_t kFlagMeta = 1u << 7;
constexpr std::uint8_t kMetaDbObjectId = 1u << 0;
constexpr std::uint8_t kMetaSavepointId = 1u << 1;
constexpr std::uint8_t kMetaUndoPrevLsn = 1u << 2;
constexpr std::uint8_t kMetaPitrTargetLsn = 1u << 3;
constexpr std::uint8_t kMetaPitrTargetTsMs = 1u << 4;
constexpr std::uint8_t kMetaRecordTsMs = 1u << 5;
constexpr std::uint8_t kMetaCheckpointSnapshotLsn = 1u << 6;

void append_u16_le(uint16_t v, std::vector<uint8_t>& out) {
    out.push_back(static_cast<uint8_t>(v & 0xFF));
    out.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
}

void append_u32_le(uint32_t v, std::vector<uint8_t>& out) {
    out.push_back(static_cast<uint8_t>(v & 0xFF));
    out.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
    out.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
    out.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
}

void append_u64_le(std::uint64_t v, std::vector<uint8_t>& out) {
    out.push_back(static_cast<uint8_t>(v & 0xFF));
    out.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
    out.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
    out.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
    out.push_back(static_cast<uint8_t>((v >> 32) & 0xFF));
    out.push_back(static_cast<uint8_t>((v >> 40) & 0xFF));
    out.push_back(static_cast<uint8_t>((v >> 48) & 0xFF));
    out.push_back(static_cast<uint8_t>((v >> 56) & 0xFF));
}

} // namespace

Status build_payload_legacy(const std::string& table,
                            const uint32_t* row_id,
                            const std::vector<uint8_t>* row_payload,
                            std::vector<uint8_t>& out) {
    if (table.size() > 0xFFFFu) {
        return Status::Fail("wal payload table name too long");
    }
    if ((row_id == nullptr) != (row_payload == nullptr)) {
        return Status::Fail("wal payload row fields must appear together");
    }

    out.clear();
    out.reserve(2 + table.size() + (row_id ? (8 + row_payload->size()) : 0));
    append_u16_le(static_cast<uint16_t>(table.size()), out);
    out.insert(out.end(), table.begin(), table.end());
    if (row_id != nullptr) {
        append_u32_le(*row_id, out);
        append_u32_le(static_cast<uint32_t>(row_payload->size()), out);
        out.insert(out.end(), row_payload->begin(), row_payload->end());
    }
    return Status::Ok();
}

Status build_payload(const std::string& table,
                     const RowImageView* before_image,
                     const RowImageView* after_image,
                     const PayloadMetaView* meta,
                     const std::uint64_t op_seq_in_txn,
                     std::vector<uint8_t>& out) {
    if (table.size() > 0xFFFFu) {
        return Status::Fail("wal payload table name too long");
    }
    const auto valid_image = [](const RowImageView* img) {
        if (img == nullptr) return true;
        return (img->row_id != nullptr && img->row_payload != nullptr);
    };
    if (!valid_image(before_image) || !valid_image(after_image)) {
        return Status::Fail("wal payload image row fields must appear together");
    }
    out.clear();
    out.reserve(1 + 2 + table.size() + 1 + 8 +
                (before_image ? (8 + before_image->row_payload->size()) : 0) +
                (after_image ? (8 + after_image->row_payload->size()) : 0) + 1 + 6 * 8);
    out.push_back(kPayloadMarkerV1);
    append_u16_le(static_cast<uint16_t>(table.size()), out);
    out.insert(out.end(), table.begin(), table.end());
    std::uint8_t flags = 0;
    if (before_image != nullptr) flags |= kFlagBeforeImage;
    if (after_image != nullptr) flags |= kFlagAfterImage;
    std::uint8_t meta_mask = 0;
    if (meta != nullptr) {
        if (meta->db_object_id != nullptr) meta_mask |= kMetaDbObjectId;
        if (meta->savepoint_id != nullptr) meta_mask |= kMetaSavepointId;
        if (meta->undo_prev_lsn != nullptr) meta_mask |= kMetaUndoPrevLsn;
        if (meta->pitr_target_lsn != nullptr) meta_mask |= kMetaPitrTargetLsn;
        if (meta->pitr_target_ts_ms != nullptr) meta_mask |= kMetaPitrTargetTsMs;
        if (meta->record_ts_ms != nullptr) meta_mask |= kMetaRecordTsMs;
        if (meta->checkpoint_snapshot_lsn != nullptr) meta_mask |= kMetaCheckpointSnapshotLsn;
        if (meta_mask != 0) flags |= kFlagMeta;
    }
    out.push_back(flags);
    append_u64_le(op_seq_in_txn, out);
    if (before_image != nullptr) {
        append_u32_le(*before_image->row_id, out);
        append_u32_le(static_cast<uint32_t>(before_image->row_payload->size()), out);
        out.insert(out.end(), before_image->row_payload->begin(), before_image->row_payload->end());
    }
    if (after_image != nullptr) {
        append_u32_le(*after_image->row_id, out);
        append_u32_le(static_cast<uint32_t>(after_image->row_payload->size()), out);
        out.insert(out.end(), after_image->row_payload->begin(), after_image->row_payload->end());
    }
    if ((flags & kFlagMeta) != 0) {
        out.push_back(meta_mask);
        if ((meta_mask & kMetaDbObjectId) != 0) append_u64_le(*meta->db_object_id, out);
        if ((meta_mask & kMetaSavepointId) != 0) append_u64_le(*meta->savepoint_id, out);
        if ((meta_mask & kMetaUndoPrevLsn) != 0) append_u64_le(*meta->undo_prev_lsn, out);
        if ((meta_mask & kMetaPitrTargetLsn) != 0) append_u64_le(*meta->pitr_target_lsn, out);
        if ((meta_mask & kMetaPitrTargetTsMs) != 0) append_u64_le(*meta->pitr_target_ts_ms, out);
        if ((meta_mask & kMetaRecordTsMs) != 0) append_u64_le(*meta->record_ts_ms, out);
        if ((meta_mask & kMetaCheckpointSnapshotLsn) != 0) append_u64_le(*meta->checkpoint_snapshot_lsn, out);
    }
    return Status::Ok();
}

Status build_payload(const std::string& table,
                     const uint32_t* row_id,
                     const std::vector<uint8_t>* row_payload,
                     std::vector<uint8_t>& out) {
    return build_payload_legacy(table, row_id, row_payload, out);
}

Status decode_table_name(const uint8_t*& p, const uint8_t* end, std::string& out) {
    if (p + 2 > end) {
        return Status::Fail("wal payload missing table name length");
    }
    const uint16_t len = static_cast<uint16_t>(p[0] | (p[1] << 8));
    p += 2;
    if (p + len > end) {
        return Status::Fail("wal payload table name truncated");
    }
    out.assign(reinterpret_cast<const char*>(p), len);
    p += len;
    return Status::Ok();
}

Status decode_u32(const uint8_t*& p, const uint8_t* end, uint32_t& out) {
    if (p + 4 > end) {
        return Status::Fail("wal payload u32 truncated");
    }
    out = static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
          (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
    p += 4;
    return Status::Ok();
}

Status decode_u64(const uint8_t*& p, const uint8_t* end, std::uint64_t& out) {
    if (p + 8 > end) {
        return Status::Fail("wal payload u64 truncated");
    }
    out = static_cast<std::uint64_t>(p[0]) | (static_cast<std::uint64_t>(p[1]) << 8) |
          (static_cast<std::uint64_t>(p[2]) << 16) | (static_cast<std::uint64_t>(p[3]) << 24) |
          (static_cast<std::uint64_t>(p[4]) << 32) | (static_cast<std::uint64_t>(p[5]) << 40) |
          (static_cast<std::uint64_t>(p[6]) << 48) | (static_cast<std::uint64_t>(p[7]) << 56);
    p += 8;
    return Status::Ok();
}

Status decode_row_fields(const uint8_t*& p,
                         const uint8_t* end,
                         uint32_t& row_id,
                         const uint8_t*& row_payload,
                         uint32_t& row_payload_len) {
    Status st = decode_u32(p, end, row_id);
    if (!st.ok) {
        return Status::Fail("wal payload missing row id");
    }
    st = decode_u32(p, end, row_payload_len);
    if (!st.ok) {
        return Status::Fail("wal payload missing row payload length");
    }
    if (p + row_payload_len > end) {
        return Status::Fail("wal payload row payload truncated");
    }
    row_payload = p;
    p += row_payload_len;
    return Status::Ok();
}

bool is_v1_payload(const uint8_t* data, const std::size_t len) {
    return data != nullptr && len > 0 && data[0] == kPayloadMarkerV1;
}

Status decode_payload_v1(const uint8_t* data, const std::size_t len, DecodedPayloadV1& out) {
    if (!is_v1_payload(data, len)) {
        return Status::Fail("not v1 payload");
    }
    const uint8_t* p = data + 1;
    const uint8_t* end = data + len;
    out = DecodedPayloadV1{};
    Status st = decode_table_name(p, end, out.table);
    if (!st.ok) return st;
    if (p >= end) return Status::Fail("wal payload missing flags");
    const std::uint8_t flags = *p++;
    st = decode_u64(p, end, out.op_seq_in_txn);
    if (!st.ok) return st;
    if ((flags & kFlagBeforeImage) != 0) {
        out.has_before = true;
        st = decode_row_fields(p, end, out.before_row_id, out.before_row_payload, out.before_row_payload_len);
        if (!st.ok) return st;
    }
    if ((flags & kFlagAfterImage) != 0) {
        out.has_after = true;
        st = decode_row_fields(p, end, out.after_row_id, out.after_row_payload, out.after_row_payload_len);
        if (!st.ok) return st;
    }
    if ((flags & kFlagMeta) != 0) {
        if (p >= end) return Status::Fail("wal payload missing meta mask");
        const std::uint8_t meta_mask = *p++;
        if ((meta_mask & kMetaDbObjectId) != 0) {
            out.has_db_object_id = true;
            st = decode_u64(p, end, out.db_object_id);
            if (!st.ok) return st;
        }
        if ((meta_mask & kMetaSavepointId) != 0) {
            out.has_savepoint_id = true;
            st = decode_u64(p, end, out.savepoint_id);
            if (!st.ok) return st;
        }
        if ((meta_mask & kMetaUndoPrevLsn) != 0) {
            out.has_undo_prev_lsn = true;
            st = decode_u64(p, end, out.undo_prev_lsn);
            if (!st.ok) return st;
        }
        if ((meta_mask & kMetaPitrTargetLsn) != 0) {
            out.has_pitr_target_lsn = true;
            st = decode_u64(p, end, out.pitr_target_lsn);
            if (!st.ok) return st;
        }
        if ((meta_mask & kMetaPitrTargetTsMs) != 0) {
            out.has_pitr_target_ts_ms = true;
            st = decode_u64(p, end, out.pitr_target_ts_ms);
            if (!st.ok) return st;
        }
        if ((meta_mask & kMetaRecordTsMs) != 0) {
            out.has_record_ts_ms = true;
            st = decode_u64(p, end, out.record_ts_ms);
            if (!st.ok) return st;
        }
        if ((meta_mask & kMetaCheckpointSnapshotLsn) != 0) {
            out.has_checkpoint_snapshot_lsn = true;
            st = decode_u64(p, end, out.checkpoint_snapshot_lsn);
            if (!st.ok) return st;
        }
    }
    return Status::Ok();
}

} // namespace newdb::walcodec
