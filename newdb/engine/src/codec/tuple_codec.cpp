#include <newdb/tuple_codec.h>

#include <waterfall/storage/record.h>
#include <waterfall/storage/tuple.h>
#include <waterfall/utils/arena.h>

#include <string>

namespace newdb::codec {

namespace {

using wf::storage::logical_tuple;
using wf::storage::record_payload;
using wf::utils::char_slice;
using wf::utils::error_info;
using wf::utils::object_arena;

Status build_logical_tuple(const Row& row,
                           object_arena& arena,
                           logical_tuple& out_tuple,
                           std::vector<std::string>& backing) {
    out_tuple.clear();
    backing.clear();
    backing.reserve(1 + row.attrs.size());
    backing.emplace_back("id=" + std::to_string(row.id));
    out_tuple.emplace_back(wf::utils::char_slice(backing.back()));
    for (const auto& kv : row.attrs) {
        backing.emplace_back(kv.first + "=" + kv.second);
        out_tuple.emplace_back(wf::utils::char_slice(backing.back()));
    }
    if (out_tuple.empty()) {
        return Status::Fail("empty row");
    }
    return Status::Ok();
}

bool parse_logical_tuple(const logical_tuple& tuple, Row& out_row) {
    if (tuple.size() == 0) {
        return false;
    }
    out_row = Row{};
    for (std::size_t i = 0; i < tuple.size(); ++i) {
        const std::string s = tuple[i].ToString();
        const auto pos = s.find('=');
        if (pos == std::string::npos) {
            continue;
        }
        std::string key = s.substr(0, pos);
        std::string val = s.substr(pos + 1);
        if (key == "id") {
            try {
                out_row.id = std::stoi(val);
            } catch (...) {
                return false;
            }
        } else {
            out_row.attrs[std::move(key)] = std::move(val);
        }
    }
    return out_row.id != 0;
}

} // namespace

Status encode_row_to_heap_payload(const Row& row, std::vector<unsigned char>& out_payload) {
    object_arena arena;
    logical_tuple tuple{wf::utils::arena_allocator<char_slice>(&arena)};
    std::vector<std::string> backing;
    const Status st = build_logical_tuple(row, arena, tuple, backing);
    if (!st.ok) {
        return st;
    }
    std::size_t need = 0;
    const error_info rsz = record_payload::required_size(tuple, need);
    if (rsz != wf::utils::OK) {
        return Status::Fail("required_size failed");
    }
    out_payload.resize(need);
    record_payload payload;
    payload.attatch_payload(out_payload.data());
    payload.set_length(out_payload.size());
    if (payload.encode_tuple(tuple) != wf::utils::OK) {
        return Status::Fail("encode_tuple failed");
    }
    return Status::Ok();
}

bool decode_heap_payload_to_row(const unsigned char* payload, const std::size_t len, Row& out_row) {
    if (payload == nullptr || len == 0) {
        return false;
    }
    object_arena dec_arena;
    logical_tuple dec_tuple{wf::utils::arena_allocator<char_slice>(&dec_arena)};
    record_payload rec;
    rec.attatch_payload(const_cast<unsigned char*>(payload));
    rec.set_length(len);
    if (rec.decode_record(dec_tuple) != wf::utils::OK) {
        return false;
    }
    return parse_logical_tuple(dec_tuple, out_row);
}

} // namespace newdb::codec
