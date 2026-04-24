#include <newdb/wal_codec.h>

namespace newdb::walcodec {

namespace {

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

} // namespace

Status build_payload(const std::string& table,
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

} // namespace newdb::walcodec
