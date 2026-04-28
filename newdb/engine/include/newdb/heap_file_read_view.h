#pragma once

#include <cstddef>
#include <memory>
#include <string>

namespace newdb {

// Read-only view of a heap file: mmap whole file when supported, otherwise on-demand fread.
class HeapFileReadView {
public:
    // nullptr on failure (missing file, bad size, mmap error, etc.).
    [[nodiscard]] static std::shared_ptr<HeapFileReadView> try_open(const char* path);

    [[nodiscard]] std::size_t page_size() const { return psz_; }
    [[nodiscard]] std::size_t num_pages() const { return num_pages_; }
    [[nodiscard]] const std::string& path() const { return path_; }

    // Direct pointer into mapping; nullptr if not mmap-backed or out of range.
    [[nodiscard]] const unsigned char* page_data(std::size_t page_no) const;

    // Always works when `page_no < num_pages()`; fills `buf` (length must be `page_size()`).
    [[nodiscard]] bool read_page_copy(std::size_t page_no, unsigned char* buf) const;

    HeapFileReadView(const HeapFileReadView&) = delete;
    HeapFileReadView& operator=(const HeapFileReadView&) = delete;
    ~HeapFileReadView();

private:
    HeapFileReadView(std::string path, std::size_t psz, std::size_t num_pages, void* mapped, std::size_t mapped_sz);

    std::string path_;
    std::size_t psz_{};
    std::size_t num_pages_{};
    void* mapped_{nullptr};
    std::size_t mapped_sz_{0};
};

} // namespace newdb
