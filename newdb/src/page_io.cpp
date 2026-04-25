#include <newdb/heap_file_read_view.h>
#include <newdb/heap_page.h>
#include <newdb/heap_storage.h>
#include <newdb/page_io.h>
#include <newdb/schema.h>
#include <newdb/tuple_codec.h>

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <cstring>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <mutex>
#include <unordered_map>

#if !defined(_WIN32) && (defined(__linux__) || defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || \
     defined(__OpenBSD__))
#define NEWDB_IO_HAS_MMAP 1
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace newdb::io {

namespace {

std::mutex g_path_mutex_registry_mutex;
std::unordered_map<std::string, std::shared_ptr<std::mutex>> g_path_mutexes;

std::uint64_t next_mvcc_lsn() {
    static std::atomic<std::uint64_t> g_mvcc_lsn{1};
    return g_mvcc_lsn.fetch_add(1, std::memory_order_relaxed);
}

std::uint64_t parse_u64_or_default(const std::string& v, const std::uint64_t defv) {
    try {
        return static_cast<std::uint64_t>(std::stoull(v));
    } catch (...) {
        return defv;
    }
}

void strip_mvcc_internal_attrs(Row& row) {
    row.attrs.erase("__mvcc_created_lsn");
    row.attrs.erase("__mvcc_deleted_lsn");
    row.attrs.erase("__mvcc_txn_id");
}

RecordMetadata extract_record_metadata(const Row& row, const std::uint64_t fallback_lsn) {
    RecordMetadata meta{};
    meta.created_lsn = fallback_lsn;
    meta.deleted_lsn = 0;
    meta.txn_id = 0;
    meta.is_tombstone = false;
    const auto it_c = row.attrs.find("__mvcc_created_lsn");
    const auto it_d = row.attrs.find("__mvcc_deleted_lsn");
    const auto it_t = row.attrs.find("__mvcc_txn_id");
    if (it_c != row.attrs.end()) {
        meta.created_lsn = parse_u64_or_default(it_c->second, meta.created_lsn);
    }
    if (it_d != row.attrs.end()) {
        meta.deleted_lsn = parse_u64_or_default(it_d->second, 0);
    }
    if (it_t != row.attrs.end()) {
        meta.txn_id = parse_u64_or_default(it_t->second, 0);
    }
    const auto it_del = row.attrs.find("__deleted");
    meta.is_tombstone = (it_del != row.attrs.end() && it_del->second == "1");
    return meta;
}

static std::shared_ptr<std::mutex> mutex_for_heap_path(const char* path) {
    if (path == nullptr) {
        static auto fallback = std::make_shared<std::mutex>();
        return fallback;
    }
    std::error_code ec;
    const std::filesystem::path p(path);
    const std::filesystem::path abs = std::filesystem::absolute(p, ec);
    const std::string key = ec ? std::string(path) : abs.lexically_normal().string();

    std::lock_guard<std::mutex> reg(g_path_mutex_registry_mutex);
    const auto it = g_path_mutexes.find(key);
    if (it != g_path_mutexes.end()) {
        return it->second;
    }
    auto m = std::make_shared<std::mutex>();
    g_path_mutexes.emplace(key, m);
    return m;
}

static Status merge_one_page_into_fingers(const unsigned char* page_base,
                                          const std::size_t psz,
                                          const std::uint64_t page_no,
                                          const TableSchema& schema,
                                          std::unordered_map<int, HeapRowFinger>& latest,
                                          std::unordered_map<int, RecordMetadata>& latest_meta,
                                          std::uint64_t& lsn_counter,
                                          Status& fatal) {
    if (!heap_page::verify_checksum(page_base, psz)) {
        return Status::Ok();
    }
    const bool ok = heap_page::walk_record_slices(page_base, psz, [&](const heap_page::RecordSlice& slice) {
        Row row;
        if (!codec::decode_heap_payload_to_row(slice.payload, slice.payload_length, row)) {
            return true;
        }
        const Status vst = validate_storage_row(schema, row);
        if (!vst.ok) {
            fatal = vst;
            return false;
        }
        const std::uint64_t lsn = ++lsn_counter;
        const RecordMetadata parsed_meta = extract_record_metadata(row, lsn);
        const auto it_del = row.attrs.find("__deleted");
        if (it_del != row.attrs.end() && it_del->second == "1") {
            latest.erase(row.id);
            latest_meta.erase(row.id);
            return true;
        }
        const std::size_t off_in_page = static_cast<std::size_t>(slice.payload - page_base);
        if (off_in_page > psz || slice.payload_length > psz || off_in_page + slice.payload_length > psz) {
            fatal = Status::Fail("record slice out of page bounds");
            return false;
        }
        HeapRowFinger finger{};
        finger.page_no = page_no;
        finger.byte_off_in_page = static_cast<std::uint32_t>(off_in_page);
        finger.payload_len = static_cast<std::uint32_t>(slice.payload_length);
        latest[row.id] = finger;
        latest_meta[row.id] = parsed_meta;
        return true;
    });
    if (!ok || !fatal.ok) {
        return fatal.ok ? Status::Fail("walk_record_slices stopped") : fatal;
    }
    return Status::Ok();
}

static void sort_latest_ids(const std::unordered_map<int, HeapRowFinger>& latest, std::vector<int>& ids) {
    ids.clear();
    ids.reserve(latest.size());
    for (const auto& kv : latest) {
        ids.push_back(kv.first);
    }
    std::sort(ids.begin(), ids.end(), [&](const int a, const int b) {
        const HeapRowFinger& fa = latest.at(a);
        const HeapRowFinger& fb = latest.at(b);
        if (fa.page_no != fb.page_no) {
            return fa.page_no < fb.page_no;
        }
        return a < b;
    });
}

static void build_sorted_id_finger_vectors(const std::unordered_map<int, HeapRowFinger>& latest,
                                           const std::unordered_map<int, RecordMetadata>& latest_meta,
                                           std::vector<int>& ids,
                                           std::vector<HeapRowFinger>& fingers,
                                           std::vector<RecordMetadata>& metadata) {
    sort_latest_ids(latest, ids);
    fingers.clear();
    fingers.reserve(ids.size());
    metadata.clear();
    metadata.reserve(ids.size());
    for (const int id : ids) {
        fingers.push_back(latest.at(id));
        const auto it = latest_meta.find(id);
        metadata.push_back(it != latest_meta.end() ? it->second : RecordMetadata{});
    }
}

[[nodiscard]] static bool try_lazy_adopt_heap(const char* path,
                                                const std::string& table_name,
                                                const TableSchema& schema,
                                                const std::size_t psz,
                                                const std::size_t num_pages,
                                                const std::unordered_map<int, HeapRowFinger>& latest,
                                                const std::unordered_map<int, RecordMetadata>& latest_meta,
                                                HeapTable& out) {
    std::vector<int> ids;
    std::vector<HeapRowFinger> fingers;
    std::vector<RecordMetadata> metadata;
    build_sorted_id_finger_vectors(latest, latest_meta, ids, fingers, metadata);
    const auto view = HeapFileReadView::try_open(path);
    if (!view || view->page_size() != psz || view->num_pages() != num_pages) {
        return false;
    }
    out.adopt_lazy_heap_storage(table_name, view, std::move(ids), std::move(fingers), std::move(metadata), schema);
    return true;
}

static Status finalize_from_fingers(const std::string& table_name,
                                    const TableSchema& schema,
                                    const std::unordered_map<int, HeapRowFinger>& latest,
                                    const std::unordered_map<int, RecordMetadata>& latest_meta,
                                    const unsigned char* mmap_base,
                                    FILE* fp,
                                    const std::size_t psz,
                                    const std::size_t num_pages,
                                    HeapTable& out) {
    out.discard_lazy_storage_without_rebuild();
    out.name = table_name;
    out.rows.clear();
    out.rows.reserve(latest.size());
    out.row_meta.clear();
    out.row_meta.reserve(latest.size());

    std::vector<int> ids;
    sort_latest_ids(latest, ids);

    const unsigned char* page_ptr = nullptr;
    std::vector<unsigned char> buf(psz);
    std::uint64_t loaded_page = UINT64_MAX;

    for (const int id : ids) {
        const HeapRowFinger& f = latest.at(id);
        if (f.page_no >= num_pages) {
            return Status::Fail("heap finger references page beyond file");
        }
        if (static_cast<std::size_t>(f.byte_off_in_page) + static_cast<std::size_t>(f.payload_len) > psz) {
            return Status::Fail("heap finger payload out of page");
        }

        if (f.page_no != loaded_page) {
            loaded_page = f.page_no;
            if (mmap_base != nullptr) {
                page_ptr = mmap_base + loaded_page * psz;
            } else {
                if (fp == nullptr) {
                    return Status::Fail("internal: no backing store for finalize");
                }
                if (std::fseek(fp, static_cast<long>(static_cast<std::size_t>(loaded_page) * psz), SEEK_SET) != 0) {
                    return Status::Fail("fseek failed materializing heap page");
                }
                if (std::fread(buf.data(), 1, psz, fp) != psz) {
                    return Status::Fail("fread failed materializing heap page");
                }
                page_ptr = buf.data();
            }
            if (!heap_page::verify_checksum(page_ptr, psz)) {
                return Status::Fail("checksum mismatch when materializing row");
            }
        }

        Row row;
        if (!codec::decode_heap_payload_to_row(page_ptr + f.byte_off_in_page, f.payload_len, row)) {
            return Status::Fail("decode failed when materializing row");
        }
        strip_mvcc_internal_attrs(row);
        const Status vst = validate_storage_row(schema, row);
        if (!vst.ok) {
            return vst;
        }
        out.rows.push_back(std::move(row));
        const auto mit = latest_meta.find(id);
        out.row_meta.push_back(mit != latest_meta.end() ? mit->second : RecordMetadata{});
    }

    out.rebuild_indexes(schema);
    return Status::Ok();
}

} // namespace

Status load_heap_file(const char* path,
                      const std::string& table_name,
                      const TableSchema& schema,
                      HeapTable& out,
                      const HeapLoadOptions& opts) {
    const auto path_mutex = mutex_for_heap_path(path);
    std::lock_guard<std::mutex> io_lock(*path_mutex);

    out.clear_data();

    std::unordered_map<int, HeapRowFinger> latest;
    std::unordered_map<int, RecordMetadata> latest_meta;
    const std::size_t psz = heap_page::byte_size();
    Status fatal = Status::Ok();
    std::uint64_t lsn_counter = 0;

#if NEWDB_IO_HAS_MMAP
    const int fd = ::open(path, O_RDONLY);
    if (fd >= 0) {
        struct stat st {};
        if (::fstat(fd, &st) == 0 && S_ISREG(st.st_mode) && st.st_size > 0) {
            const auto sz = static_cast<std::size_t>(st.st_size);
            if (sz % psz != 0) {
                ::close(fd);
                return Status::Fail("partial page read");
            }
            void* mapped = ::mmap(nullptr, sz, PROT_READ, MAP_SHARED, fd, 0);
            ::close(fd);
            if (mapped != MAP_FAILED) {
#if defined(MADV_SEQUENTIAL)
                ::madvise(mapped, sz, MADV_SEQUENTIAL);
#endif
                const auto* base = static_cast<const unsigned char*>(mapped);
                const std::size_t num_pages = sz / psz;
                std::uint64_t page_no = 0;
                for (std::size_t off = 0; off < sz; off += psz) {
                    const Status st =
                        merge_one_page_into_fingers(base + off, psz, page_no, schema, latest, latest_meta, lsn_counter, fatal);
                    if (!st.ok) {
                        ::munmap(mapped, sz);
                        return st;
                    }
                    ++page_no;
                }
                if (opts.lazy_decode &&
                    try_lazy_adopt_heap(path, table_name, schema, psz, num_pages, latest, latest_meta, out)) {
                    ::munmap(mapped, sz);
                    return Status::Ok();
                }
                const Status fin =
                    finalize_from_fingers(table_name, schema, latest, latest_meta, base, nullptr, psz, num_pages, out);
                ::munmap(mapped, sz);
                return fin;
            }
        } else {
            ::close(fd);
        }
    }
#endif

    FILE* fp = std::fopen(path, "rb");
    if (!fp) {
        return Status::Fail(std::string("fopen failed: ") + path);
    }

    latest.clear();
    latest_meta.clear();
    fatal = Status::Ok();
    lsn_counter = 0;

    std::vector<unsigned char> buf(psz);
    std::uint64_t page_no = 0;
    while (true) {
        const std::size_t n = std::fread(buf.data(), 1, psz, fp);
        if (n == 0) {
            break;
        }
        if (n != psz) {
            std::fclose(fp);
            return Status::Fail("partial page read");
        }
        const Status st =
            merge_one_page_into_fingers(buf.data(), psz, page_no, schema, latest, latest_meta, lsn_counter, fatal);
        if (!st.ok) {
            std::fclose(fp);
            return st;
        }
        ++page_no;
    }

    const std::size_t num_pages = static_cast<std::size_t>(page_no);
    std::fclose(fp);
    fp = nullptr;

    if (opts.lazy_decode &&
        try_lazy_adopt_heap(path, table_name, schema, psz, num_pages, latest, latest_meta, out)) {
        return Status::Ok();
    }

    fp = std::fopen(path, "rb");
    if (!fp) {
        return Status::Fail(std::string("fopen failed for materialize: ") + path);
    }
    const Status fin =
        finalize_from_fingers(table_name, schema, latest, latest_meta, nullptr, fp, psz, num_pages, out);
    std::fclose(fp);
    return fin;
}

Status append_row(const char* path, const Row& row) {
    if (path == nullptr) {
        return Status::Fail("null path");
    }
    const auto path_mutex = mutex_for_heap_path(path);
    std::lock_guard<std::mutex> io_lock(*path_mutex);

    Row persisted = row;
    const std::uint64_t lsn = next_mvcc_lsn();
    if (persisted.attrs.find("__mvcc_created_lsn") == persisted.attrs.end()) {
        persisted.attrs["__mvcc_created_lsn"] = std::to_string(lsn);
    }
    if (persisted.attrs.find("__mvcc_txn_id") == persisted.attrs.end()) {
        persisted.attrs["__mvcc_txn_id"] = "0";
    }
    if (persisted.attrs.find("__deleted") != persisted.attrs.end() &&
        persisted.attrs.find("__mvcc_deleted_lsn") == persisted.attrs.end()) {
        persisted.attrs["__mvcc_deleted_lsn"] = std::to_string(lsn);
    }

    std::vector<unsigned char> enc;
    const Status st = codec::encode_row_to_heap_payload(persisted, enc);
    if (!st.ok) {
        return st;
    }

    FILE* fp = std::fopen(path, "r+b");
    std::vector<unsigned char> last_page;
    std::size_t file_size = 0;
    const std::size_t psz = heap_page::byte_size();
    if (!fp) {
        fp = std::fopen(path, "w+b");
        if (!fp) {
            return Status::Fail("fopen w+b failed");
        }
    } else {
        if (std::fseek(fp, 0, SEEK_END) != 0) {
            std::fclose(fp);
            return Status::Fail("fseek end failed");
        }
        const long sz = std::ftell(fp);
        if (sz < 0) {
            std::fclose(fp);
            return Status::Fail("ftell failed");
        }
        file_size = static_cast<std::size_t>(sz);
    }

    const bool have_last_page = (file_size >= psz) && (file_size % psz == 0);
    if (have_last_page) {
        last_page.resize(psz);
        if (std::fseek(fp, static_cast<long>(file_size - psz), SEEK_SET) != 0) {
            std::fclose(fp);
            return Status::Fail("fseek last page failed");
        }
        if (std::fread(last_page.data(), 1, psz, fp) != psz) {
            std::fclose(fp);
            return Status::Fail("fread last page failed");
        }
    } else {
        last_page = heap_page::allocate_fresh_page();
    }

    if (!heap_page::append_encoded_record(last_page, enc.data(), enc.size())) {
        if (have_last_page && std::fseek(fp, 0, SEEK_END) != 0) {
            std::fclose(fp);
            return Status::Fail("fseek append failed");
        }
        last_page = heap_page::allocate_fresh_page();
        if (!heap_page::append_encoded_record(last_page, enc.data(), enc.size())) {
            std::fclose(fp);
            return Status::Fail("record too large for empty page");
        }
    }

    heap_page::update_checksum(last_page);

    if (have_last_page && file_size >= psz) {
        if (std::fseek(fp, static_cast<long>(file_size - psz), SEEK_SET) != 0) {
            std::fclose(fp);
            return Status::Fail("fseek overwrite last page failed");
        }
    } else {
        if (std::fseek(fp, 0, SEEK_END) != 0) {
            std::fclose(fp);
            return Status::Fail("fseek end new page failed");
        }
    }
    if (std::fwrite(last_page.data(), 1, psz, fp) != psz) {
        std::fclose(fp);
        return Status::Fail("fwrite page failed");
    }
    std::fclose(fp);
    return Status::Ok();
}

Status append_rows(const char* path, const std::vector<Row>& rows) {
    if (path == nullptr) {
        return Status::Fail("null path");
    }
    if (rows.empty()) {
        return Status::Ok();
    }
    const auto path_mutex = mutex_for_heap_path(path);
    std::lock_guard<std::mutex> io_lock(*path_mutex);

    const std::size_t psz = heap_page::byte_size();
    FILE* fp = std::fopen(path, "r+b");
    std::size_t file_size = 0;
    if (!fp) {
        fp = std::fopen(path, "w+b");
        if (!fp) {
            return Status::Fail("fopen w+b failed");
        }
    } else {
        if (std::fseek(fp, 0, SEEK_END) != 0) {
            std::fclose(fp);
            return Status::Fail("fseek end failed");
        }
        const long sz = std::ftell(fp);
        if (sz < 0) {
            std::fclose(fp);
            return Status::Fail("ftell failed");
        }
        file_size = static_cast<std::size_t>(sz);
    }

    const bool have_last_page = (file_size >= psz) && (file_size % psz == 0);
    std::vector<unsigned char> current_page;
    if (have_last_page) {
        current_page.resize(psz);
        if (std::fseek(fp, static_cast<long>(file_size - psz), SEEK_SET) != 0) {
            std::fclose(fp);
            return Status::Fail("fseek last page failed");
        }
        if (std::fread(current_page.data(), 1, psz, fp) != psz) {
            std::fclose(fp);
            return Status::Fail("fread last page failed");
        }
    } else {
        current_page = heap_page::allocate_fresh_page();
    }

    std::vector<std::vector<unsigned char>> pages_to_write;
    for (const Row& row : rows) {
        Row persisted = row;
        const std::uint64_t lsn = next_mvcc_lsn();
        if (persisted.attrs.find("__mvcc_created_lsn") == persisted.attrs.end()) {
            persisted.attrs["__mvcc_created_lsn"] = std::to_string(lsn);
        }
        if (persisted.attrs.find("__mvcc_txn_id") == persisted.attrs.end()) {
            persisted.attrs["__mvcc_txn_id"] = "0";
        }
        if (persisted.attrs.find("__deleted") != persisted.attrs.end() &&
            persisted.attrs.find("__mvcc_deleted_lsn") == persisted.attrs.end()) {
            persisted.attrs["__mvcc_deleted_lsn"] = std::to_string(lsn);
        }
        std::vector<unsigned char> enc;
        const Status est = codec::encode_row_to_heap_payload(persisted, enc);
        if (!est.ok) {
            std::fclose(fp);
            return est;
        }
        if (!heap_page::append_encoded_record(current_page, enc.data(), enc.size())) {
            heap_page::update_checksum(current_page);
            pages_to_write.push_back(current_page);
            current_page = heap_page::allocate_fresh_page();
            if (!heap_page::append_encoded_record(current_page, enc.data(), enc.size())) {
                std::fclose(fp);
                return Status::Fail("record too large for empty page");
            }
        }
    }
    heap_page::update_checksum(current_page);
    pages_to_write.push_back(current_page);

    if (have_last_page && file_size >= psz) {
        if (std::fseek(fp, static_cast<long>(file_size - psz), SEEK_SET) != 0) {
            std::fclose(fp);
            return Status::Fail("fseek overwrite last page failed");
        }
    } else {
        if (std::fseek(fp, 0, SEEK_END) != 0) {
            std::fclose(fp);
            return Status::Fail("fseek end new page failed");
        }
    }
    for (const auto& pg : pages_to_write) {
        if (std::fwrite(pg.data(), 1, psz, fp) != psz) {
            std::fclose(fp);
            return Status::Fail("fwrite page failed");
        }
    }
    std::fclose(fp);
    return Status::Ok();
}

Status create_heap_file(const char* path, const std::vector<Row>& rows) {
    const auto path_mutex = mutex_for_heap_path(path);
    std::lock_guard<std::mutex> io_lock(*path_mutex);

    std::vector<std::vector<unsigned char>> pages;
    pages.push_back(heap_page::allocate_fresh_page());

    for (const Row& r : rows) {
        std::vector<unsigned char> enc;
        const Status st = codec::encode_row_to_heap_payload(r, enc);
        if (!st.ok) {
            return st;
        }
        if (!heap_page::append_encoded_record(pages.back(), enc.data(), enc.size())) {
            heap_page::update_checksum(pages.back());
            pages.push_back(heap_page::allocate_fresh_page());
            if (!heap_page::append_encoded_record(pages.back(), enc.data(), enc.size())) {
                return Status::Fail("record too large for empty page");
            }
        }
    }
    heap_page::update_checksum(pages.back());

    FILE* fp = std::fopen(path, "wb");
    if (!fp) {
        return Status::Fail("fopen wb failed");
    }
    for (auto& p : pages) {
        if (std::fwrite(p.data(), 1, p.size(), fp) != p.size()) {
            std::fclose(fp);
            return Status::Fail("fwrite pages failed");
        }
    }
    std::fclose(fp);
    return Status::Ok();
}

Status compact_heap_file(const char* path,
                         const std::string& table_name,
                         const TableSchema& schema,
                         std::size_t* out_rows_after) {
    HeapTable tbl;
    const Status lst = load_heap_file(path, table_name, schema, tbl);
    if (!lst.ok) {
        return lst;
    }
    const Status cst = create_heap_file(path, tbl.rows);
    if (!cst.ok) {
        return cst;
    }
    if (out_rows_after != nullptr) {
        *out_rows_after = tbl.rows.size();
    }
    return Status::Ok();
}

void scan_heap_file(const char* path) {
    const auto path_mutex = mutex_for_heap_path(path);
    std::lock_guard<std::mutex> io_lock(*path_mutex);

    FILE* fp = std::fopen(path, "rb");
    if (!fp) {
        std::perror("fopen heap file for scan");
        return;
    }
    const std::size_t psz = heap_page::byte_size();
    std::vector<unsigned char> buf(psz);
    std::size_t page_index = 0;
    std::size_t total_records = 0;
    while (true) {
        const std::size_t read = std::fread(buf.data(), 1, psz, fp);
        if (read == 0) {
            break;
        }
        if (read != psz) {
            std::printf("partial page read, size=%zu\n", read);
            break;
        }
        ++page_index;
        if (!heap_page::verify_checksum(buf.data(), buf.size())) {
            std::printf("  !! checksum mismatch, skip page %zu\n", page_index);
            continue;
        }
        (void)heap_page::walk_record_slices(buf, [&](const heap_page::RecordSlice& slice) {
            ++total_records;
            Row row;
            if (codec::decode_heap_payload_to_row(slice.payload, slice.payload_length, row)) {
                std::printf("  -> record slot=%u on page %zu id=%d attrs=%zu\n",
                            slice.slot_index,
                            page_index,
                            row.id,
                            row.attrs.size());
            }
            return true;
        });
    }
    std::fclose(fp);
    std::printf("Scanned %zu pages and %zu records from %s\n", page_index, total_records, path);
}

void query_attr_int_ge(const char* filename, const char* attr_name, int min_balance) {
    if (filename == nullptr || attr_name == nullptr || attr_name[0] == '\0') {
        std::fprintf(stderr, "[QUERY] invalid path or attr_name\n");
        return;
    }
    const auto path_mutex = mutex_for_heap_path(filename);
    std::lock_guard<std::mutex> io_lock(*path_mutex);

    FILE* fp = std::fopen(filename, "rb");
    if (!fp) {
        std::perror("open heap file for query");
        return;
    }
    const std::size_t psz = heap_page::byte_size();
    std::vector<unsigned char> buf(psz);
    std::size_t page_index = 0;
    std::size_t matched_rows = 0;
    long long sum_col = 0;
    std::printf("=== Run query: scan WHERE %s >= %d ===\n", attr_name, min_balance);
    while (true) {
        const std::size_t read = std::fread(buf.data(), 1, psz, fp);
        if (read == 0) {
            break;
        }
        if (read != psz) {
            std::printf("[QUERY] partial page read, size=%zu\n", read);
            break;
        }
        ++page_index;
        if (!heap_page::verify_checksum(buf.data(), buf.size())) {
            std::printf("[QUERY] page %zu checksum mismatch, skip\n", page_index);
            continue;
        }
        (void)heap_page::walk_record_slices(buf, [&](const heap_page::RecordSlice& slice) {
            Row row;
            if (!codec::decode_heap_payload_to_row(slice.payload, slice.payload_length, row)) {
                std::printf("[QUERY] decode on page %zu slot %u failed\n", page_index, slice.slot_index);
                return true;
            }
            int col = 0;
            const auto it = row.attrs.find(attr_name);
            if (it == row.attrs.end()) {
                return true;
            }
            try {
                col = std::stoi(it->second);
            } catch (...) {
                return true;
            }
            if (col >= min_balance) {
                ++matched_rows;
                sum_col += col;
                std::printf("[QUERY] hit: id=%d %s=%d\n", row.id, attr_name, col);
            }
            return true;
        });
    }
    std::fclose(fp);
    std::printf("=== Query done: matched=%zu, sum(%s)=%lld ===\n", matched_rows, attr_name, sum_col);
}

void query_balance_ge(const char* filename, int min_balance) {
    query_attr_int_ge(filename, "balance", min_balance);
}

} // namespace newdb::io
