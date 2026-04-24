#include <waterfall/config.h>

#include <newdb/heap_page.h>

#include <waterfall/storage/page.h>

#include <algorithm>
#include <cstring>
#include <new>
#include <vector>

namespace newdb::heap_page {

namespace {

using wf::storage::kPageSize;
using wf::storage::page_base;
using wf::storage::page_header_t;

inline std::uint16_t* slot_ptr(unsigned char* page, const unsigned short num_records) {
    return reinterpret_cast<std::uint16_t*>(page + kPageSize) - num_records;
}

} // namespace

std::size_t byte_size() {
    return kPageSize;
}

std::vector<unsigned char> allocate_fresh_page() {
    std::vector<unsigned char> buf(kPageSize);
    std::memset(buf.data(), 0, buf.size());
    auto* hdr = new (buf.data()) page_header_t();
    hdr->set_heap_page();
    hdr->set_free_space(static_cast<unsigned short>(sizeof(page_header_t)));
    hdr->set_free_size(static_cast<unsigned short>(kPageSize - sizeof(page_header_t)));
    return buf;
}

bool verify_checksum(const unsigned char* page, const std::size_t length) {
    if (page == nullptr || length != kPageSize) {
        return false;
    }
    const auto* hdr = reinterpret_cast<const page_header_t*>(page);
    page_base pb(const_cast<unsigned char*>(page));
    return hdr->get_crc32() == pb.compute_checksum();
}

void update_checksum(std::vector<unsigned char>& page) {
    if (page.size() != kPageSize) {
        return;
    }
    page_base pb(page.data());
    auto* hdr = reinterpret_cast<page_header_t*>(page.data());
    hdr->set_crc32(pb.compute_checksum());
}

bool walk_record_slices(const unsigned char* page,
                        const std::size_t length,
                        const std::function<bool(const RecordSlice&)>& visitor) {
    if (page == nullptr || length != kPageSize) {
        return false;
    }
    const auto* header = reinterpret_cast<const page_header_t*>(page);
    const unsigned short num = header->get_num_records();
    const std::uint16_t* const all_slots = slot_ptr(const_cast<unsigned char*>(page), num);
    const unsigned short free_space = header->get_free_space();

    // Slot table order does not match ascending payload offsets (newer slots are at lower indices).
    // Walk records in physical offset order so each length is (next_start - this_start).
    std::vector<unsigned short> order(static_cast<std::size_t>(num));
    for (unsigned short i = 0; i < num; ++i) {
        order[static_cast<std::size_t>(i)] = i;
    }
    std::sort(order.begin(), order.end(), [&](unsigned short a, unsigned short b) {
        return all_slots[a] < all_slots[b];
    });

    for (unsigned short k = 0; k < num; ++k) {
        const unsigned short slot_index = order[static_cast<std::size_t>(k)];
        const unsigned short rec_offset = all_slots[slot_index];
        const unsigned short next_offset =
            (k + 1 < num) ? all_slots[order[static_cast<std::size_t>(k + 1)]] : free_space;
        if (next_offset < rec_offset || next_offset > free_space) {
            return false;
        }
        const std::size_t rec_len = static_cast<std::size_t>(next_offset - rec_offset);
        RecordSlice slice{};
        slice.slot_index = slot_index;
        slice.payload = page + rec_offset;
        slice.payload_length = rec_len;
        if (!visitor(slice)) {
            return false;
        }
    }
    return true;
}

bool walk_record_slices(const std::vector<unsigned char>& page_buffer,
                        const std::function<bool(const RecordSlice&)>& visitor) {
    return walk_record_slices(page_buffer.data(), page_buffer.size(), visitor);
}

bool append_encoded_record(std::vector<unsigned char>& page_buf,
                           const unsigned char* encoded,
                           const std::size_t encoded_length) {
    if (page_buf.size() != kPageSize || encoded == nullptr) {
        return false;
    }
    auto* page = page_buf.data();
    auto* header = reinterpret_cast<page_header_t*>(page);
    const std::size_t rec_len = encoded_length;
    const unsigned short cur_num = header->get_num_records();
    const unsigned short cur_free_space = header->get_free_space();
    const unsigned short cur_free_size = header->get_free_size();
    const std::size_t need = rec_len + sizeof(std::uint16_t);
    if (cur_free_size < need) {
        return false;
    }
    const unsigned short rec_offset = cur_free_space;
    std::memcpy(page + rec_offset, encoded, rec_len);
    const unsigned short new_num = cur_num + 1;
    std::uint16_t* new_slot = slot_ptr(page, new_num);
    *new_slot = rec_offset;
    const unsigned short slot_region_begin =
        static_cast<unsigned short>(reinterpret_cast<unsigned char*>(new_slot) - page);
    const unsigned short new_free_space = static_cast<unsigned short>(rec_offset + rec_len);
    const unsigned short new_free_size =
        static_cast<unsigned short>(slot_region_begin - new_free_space);
    header->set_num_records(new_num);
    header->set_free_space(new_free_space);
    header->set_free_size(new_free_size);
    return true;
}

} // namespace newdb::heap_page
