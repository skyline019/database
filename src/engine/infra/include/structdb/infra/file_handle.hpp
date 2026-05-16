#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <string>
#include <vector>

namespace structdb::infra {

/// Blocking file I/O (Windows + POSIX). IOCP/overlapped can replace internals later.
class FileWriter {
 public:
  FileWriter() = default;
  explicit FileWriter(const std::filesystem::path& path, bool append = true);
  FileWriter(FileWriter&&) noexcept;
  FileWriter& operator=(FileWriter&&) noexcept;
  ~FileWriter();

  FileWriter(const FileWriter&) = delete;
  FileWriter& operator=(const FileWriter&) = delete;

  bool is_open() const {
#if defined(_WIN32)
    return handle_ != nullptr;
#else
    return fd_ >= 0;
#endif
  }
  bool open(const std::filesystem::path& path, bool append = true);
  void close();
  bool write_all(const void* data, std::size_t len);
  /// Write `len` bytes in chunks of at most `chunk_bytes`, invoking `on_chunk_written` after each successful write.
  /// `chunk_bytes` must be > 0.
  bool write_all_chunked(const void* data, std::size_t len, std::size_t chunk_bytes,
                         const std::function<void(std::size_t)>& on_chunk_written);
  bool sync();

 private:
#if defined(_WIN32)
  void* handle_{nullptr};  // HANDLE
#else
  int fd_{-1};
#endif
};

class FileReader {
 public:
  FileReader() = default;
  explicit FileReader(const std::filesystem::path& path);
  FileReader(FileReader&&) noexcept;
  FileReader& operator=(FileReader&&) noexcept;
  ~FileReader();

  FileReader(const FileReader&) = delete;
  FileReader& operator=(const FileReader&) = delete;

  bool is_open() const {
#if defined(_WIN32)
    return handle_ != nullptr;
#else
    return fd_ >= 0;
#endif
  }
  /// @param sequential_scan_hint When true, OS may optimize for full-file sequential read (e.g. Windows
  /// `FILE_FLAG_SEQUENTIAL_SCAN`; POSIX `posix_fadvise` sequential where available).
  bool open(const std::filesystem::path& path, bool sequential_scan_hint = false);
  void close();
  bool read_all(std::vector<std::uint8_t>& out);
  /// Read entire file into `out` using reads of at most `chunk_bytes` bytes, invoking `on_chunk_read` after each
  /// successful read (argument = bytes read in that call). `chunk_bytes` must be > 0.
  bool read_all_chunked(std::vector<std::uint8_t>& out, std::size_t chunk_bytes,
                        const std::function<void(std::size_t)>& on_chunk_read);

 private:
#if defined(_WIN32)
  void* handle_{nullptr};
#else
  int fd_{-1};
#endif
};

}  // namespace structdb::infra
