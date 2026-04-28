#include <newdb/heap_file_read_view.h>
#include <newdb/heap_page.h>

#include <cstdio>
#include <cstring>

#if !defined(_WIN32) && (defined(__linux__) || defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || \
     defined(__OpenBSD__))
#define NEWDB_IO_HAS_MMAP 1
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace newdb {

HeapFileReadView::HeapFileReadView(std::string path,
                                   const std::size_t psz,
                                   const std::size_t num_pages,
                                   void* mapped,
                                   const std::size_t mapped_sz)
    : path_(std::move(path)), psz_(psz), num_pages_(num_pages), mapped_(mapped), mapped_sz_(mapped_sz) {}

HeapFileReadView::~HeapFileReadView() {
#if NEWDB_IO_HAS_MMAP
    if (mapped_ != nullptr && mapped_sz_ > 0) {
        ::munmap(mapped_, mapped_sz_);
    }
#endif
    mapped_ = nullptr;
    mapped_sz_ = 0;
}

std::shared_ptr<HeapFileReadView> HeapFileReadView::try_open(const char* path) {
    if (path == nullptr || path[0] == '\0') {
        return nullptr;
    }
    const std::size_t psz = heap_page::byte_size();
#if NEWDB_IO_HAS_MMAP
    const int fd = ::open(path, O_RDONLY);
    if (fd >= 0) {
        struct stat st {};
        if (::fstat(fd, &st) == 0 && S_ISREG(st.st_mode) && st.st_size > 0) {
            const auto sz = static_cast<std::size_t>(st.st_size);
            if (sz % psz == 0) {
                void* mapped = ::mmap(nullptr, sz, PROT_READ, MAP_SHARED, fd, 0);
                ::close(fd);
                if (mapped != MAP_FAILED) {
                    const auto num_pages = sz / psz;
                    return std::shared_ptr<HeapFileReadView>(
                        new HeapFileReadView(path, psz, num_pages, mapped, sz));
                }
            }
        }
        ::close(fd);
    }
#endif
    // Size probe without keeping a mapping (stdio / non-mmap platforms).
    FILE* fp = std::fopen(path, "rb");
    if (!fp) {
        return nullptr;
    }
    if (std::fseek(fp, 0, SEEK_END) != 0) {
        std::fclose(fp);
        return nullptr;
    }
    const long szl = std::ftell(fp);
    std::fclose(fp);
    if (szl <= 0 || static_cast<std::size_t>(szl) % psz != 0) {
        return nullptr;
    }
    const auto sz = static_cast<std::size_t>(szl);
    const auto num_pages = sz / psz;
    return std::shared_ptr<HeapFileReadView>(new HeapFileReadView(path, psz, num_pages, nullptr, 0));
}

const unsigned char* HeapFileReadView::page_data(const std::size_t page_no) const {
#if NEWDB_IO_HAS_MMAP
    if (mapped_ == nullptr || page_no >= num_pages_) {
        return nullptr;
    }
    return static_cast<const unsigned char*>(mapped_) + page_no * psz_;
#else
    (void)page_no;
    return nullptr;
#endif
}

bool HeapFileReadView::read_page_copy(const std::size_t page_no, unsigned char* buf) const {
    if (buf == nullptr || page_no >= num_pages_ || psz_ == 0) {
        return false;
    }
#if NEWDB_IO_HAS_MMAP
    if (mapped_ != nullptr) {
        const auto* base = static_cast<const unsigned char*>(mapped_);
        std::memcpy(buf, base + page_no * psz_, psz_);
        return true;
    }
#endif
    FILE* fp = std::fopen(path_.c_str(), "rb");
    if (!fp) {
        return false;
    }
    if (std::fseek(fp, static_cast<long>(page_no * psz_), SEEK_SET) != 0) {
        std::fclose(fp);
        return false;
    }
    const std::size_t n = std::fread(buf, 1, psz_, fp);
    std::fclose(fp);
    return n == psz_;
}

} // namespace newdb
