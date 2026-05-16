#include "structdb/infra/file_handle.hpp"

#include <algorithm>
#include <cstdio>

#if defined(_WIN32)
#  ifndef NOMINMAX
#    define NOMINMAX
#  endif
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif
#  include <windows.h>
#else
#  include <fcntl.h>
#  include <sys/stat.h>
#  include <unistd.h>
#endif

namespace structdb::infra {

#if defined(_WIN32)

namespace {

HANDLE as_handle(void* p) { return static_cast<HANDLE>(p); }

}  // namespace

FileWriter::FileWriter(const std::filesystem::path& path, bool append) { open(path, append); }

FileWriter::FileWriter(FileWriter&& o) noexcept : handle_(o.handle_) { o.handle_ = nullptr; }

FileWriter& FileWriter::operator=(FileWriter&& o) noexcept {
  if (this == &o) return *this;
  close();
  handle_ = o.handle_;
  o.handle_ = nullptr;
  return *this;
}

FileWriter::~FileWriter() { close(); }

bool FileWriter::open(const std::filesystem::path& path, bool append) {
  close();
  DWORD access = GENERIC_WRITE;
  DWORD share = FILE_SHARE_READ;
  DWORD creation = append ? OPEN_ALWAYS : CREATE_ALWAYS;
  DWORD flags = FILE_ATTRIBUTE_NORMAL;
  HANDLE h = CreateFileW(path.c_str(), access, share, nullptr, creation, flags, nullptr);
  if (h == INVALID_HANDLE_VALUE) return false;
  if (append) SetFilePointer(h, 0, nullptr, FILE_END);
  handle_ = h;
  return true;
}

void FileWriter::close() {
  if (handle_) {
    CloseHandle(as_handle(handle_));
    handle_ = nullptr;
  }
}

bool FileWriter::write_all(const void* data, std::size_t len) {
  if (!handle_) return false;
  const auto* p = static_cast<const std::uint8_t*>(data);
  std::size_t off = 0;
  while (off < len) {
    DWORD to_write = static_cast<DWORD>(std::min<std::size_t>(len - off, 0x7fffffffu));
    DWORD written = 0;
    if (!WriteFile(as_handle(handle_), p + off, to_write, &written, nullptr)) return false;
    if (written == 0) return false;
    off += written;
  }
  return true;
}

bool FileWriter::write_all_chunked(const void* data, std::size_t len, std::size_t chunk_bytes,
                                   const std::function<void(std::size_t)>& on_chunk_written) {
  if (!handle_ || chunk_bytes == 0) return false;
  const auto* p = static_cast<const std::uint8_t*>(data);
  std::size_t off = 0;
  while (off < len) {
    const std::size_t want = (std::min)(len - off, chunk_bytes);
    DWORD to_write = static_cast<DWORD>((std::min)(want, static_cast<std::size_t>(0x7fffffffu)));
    DWORD written = 0;
    if (!WriteFile(as_handle(handle_), p + off, to_write, &written, nullptr)) return false;
    if (written == 0) return false;
    if (on_chunk_written) on_chunk_written(static_cast<std::size_t>(written));
    off += static_cast<std::size_t>(written);
  }
  return true;
}

bool FileWriter::sync() {
  if (!handle_) return false;
  return FlushFileBuffers(as_handle(handle_)) != 0;
}

FileReader::FileReader(const std::filesystem::path& path) { open(path); }

FileReader::FileReader(FileReader&& o) noexcept : handle_(o.handle_) { o.handle_ = nullptr; }

FileReader& FileReader::operator=(FileReader&& o) noexcept {
  if (this == &o) return *this;
  close();
  handle_ = o.handle_;
  o.handle_ = nullptr;
  return *this;
}

FileReader::~FileReader() { close(); }

bool FileReader::open(const std::filesystem::path& path, bool sequential_scan_hint) {
  close();
  DWORD flags = FILE_ATTRIBUTE_NORMAL;
  if (sequential_scan_hint) flags |= FILE_FLAG_SEQUENTIAL_SCAN;
  HANDLE h = CreateFileW(
      path.c_str(),
      GENERIC_READ,
      FILE_SHARE_READ | FILE_SHARE_WRITE,
      nullptr,
      OPEN_EXISTING,
      flags,
      nullptr);
  if (h == INVALID_HANDLE_VALUE) return false;
  handle_ = h;
  return true;
}

void FileReader::close() {
  if (handle_) {
    CloseHandle(as_handle(handle_));
    handle_ = nullptr;
  }
}

bool FileReader::read_all(std::vector<std::uint8_t>& out) {
  if (!handle_) return false;
  LARGE_INTEGER sz{};
  if (!GetFileSizeEx(as_handle(handle_), &sz)) return false;
  if (sz.QuadPart < 0) return false;
  const auto n = static_cast<std::size_t>(sz.QuadPart);
  out.resize(n);
  std::size_t off = 0;
  while (off < n) {
    DWORD to_read = static_cast<DWORD>(std::min<std::size_t>(n - off, 0x7fffffffu));
    DWORD got = 0;
    if (!ReadFile(as_handle(handle_), out.data() + off, to_read, &got, nullptr)) return false;
    if (got == 0) return false;
    off += got;
  }
  return true;
}

bool FileReader::read_all_chunked(std::vector<std::uint8_t>& out, std::size_t chunk_bytes,
                                  const std::function<void(std::size_t)>& on_chunk_read) {
  if (!handle_ || chunk_bytes == 0) return false;
  LARGE_INTEGER sz{};
  if (!GetFileSizeEx(as_handle(handle_), &sz)) return false;
  if (sz.QuadPart < 0) return false;
  const auto n = static_cast<std::size_t>(sz.QuadPart);
  out.resize(n);
  std::size_t off = 0;
  while (off < n) {
    const std::size_t want = (std::min)(n - off, chunk_bytes);
    DWORD to_read = static_cast<DWORD>((std::min)(want, static_cast<std::size_t>(0x7fffffffu)));
    DWORD got = 0;
    if (!ReadFile(as_handle(handle_), out.data() + off, to_read, &got, nullptr)) return false;
    if (got == 0) return false;
    if (on_chunk_read) on_chunk_read(static_cast<std::size_t>(got));
    off += static_cast<std::size_t>(got);
  }
  return true;
}

#else

FileWriter::FileWriter(const std::filesystem::path& path, bool append) { open(path, append); }

FileWriter::FileWriter(FileWriter&& o) noexcept : fd_(o.fd_) { o.fd_ = -1; }

FileWriter& FileWriter::operator=(FileWriter&& o) noexcept {
  if (this == &o) return *this;
  close();
  fd_ = o.fd_;
  o.fd_ = -1;
  return *this;
}

FileWriter::~FileWriter() { close(); }

bool FileWriter::open(const std::filesystem::path& path, bool append) {
  close();
  int flags = O_CREAT | O_WRONLY;
  flags |= append ? O_APPEND : O_TRUNC;
  fd_ = ::open(path.c_str(), flags, 0644);
  if (fd_ < 0) return false;
#if defined(__linux__) && defined(POSIX_FADV_SEQUENTIAL)
  if (append) (void)::posix_fadvise(fd_, 0, 0, POSIX_FADV_SEQUENTIAL);
#endif
  return true;
}

void FileWriter::close() {
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
}

bool FileWriter::write_all(const void* data, std::size_t len) {
  if (fd_ < 0) return false;
  const auto* p = static_cast<const std::uint8_t*>(data);
  std::size_t off = 0;
  while (off < len) {
    ssize_t w = ::write(fd_, p + off, len - off);
    if (w <= 0) return false;
    off += static_cast<std::size_t>(w);
  }
  return true;
}

bool FileWriter::write_all_chunked(const void* data, std::size_t len, std::size_t chunk_bytes,
                                   const std::function<void(std::size_t)>& on_chunk_written) {
  if (fd_ < 0 || chunk_bytes == 0) return false;
  const auto* p = static_cast<const std::uint8_t*>(data);
  std::size_t off = 0;
  while (off < len) {
    const std::size_t want = (std::min)(len - off, chunk_bytes);
    ssize_t w = ::write(fd_, p + off, want);
    if (w <= 0) return false;
    const auto uw = static_cast<std::size_t>(w);
    if (on_chunk_written) on_chunk_written(uw);
    off += uw;
  }
  return true;
}

bool FileWriter::sync() {
  if (fd_ < 0) return false;
  return ::fsync(fd_) == 0;
}

FileReader::FileReader(const std::filesystem::path& path) { open(path); }

FileReader::FileReader(FileReader&& o) noexcept : fd_(o.fd_) { o.fd_ = -1; }

FileReader& FileReader::operator=(FileReader&& o) noexcept {
  if (this == &o) return *this;
  close();
  fd_ = o.fd_;
  o.fd_ = -1;
  return *this;
}

FileReader::~FileReader() { close(); }

bool FileReader::open(const std::filesystem::path& path, bool sequential_scan_hint) {
  close();
  fd_ = ::open(path.c_str(), O_RDONLY);
  if (fd_ < 0) return false;
#if defined(__linux__) && defined(POSIX_FADV_SEQUENTIAL)
  if (sequential_scan_hint) (void)::posix_fadvise(fd_, 0, 0, POSIX_FADV_SEQUENTIAL);
#else
  (void)sequential_scan_hint;
#endif
  return true;
}

void FileReader::close() {
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
}

bool FileReader::read_all(std::vector<std::uint8_t>& out) {
  if (fd_ < 0) return false;
  struct stat st {};
  if (fstat(fd_, &st) != 0) return false;
  if (st.st_size < 0) return false;
  const auto n = static_cast<std::size_t>(st.st_size);
  out.resize(n);
  std::size_t off = 0;
  while (off < n) {
    ssize_t r = ::read(fd_, out.data() + off, n - off);
    if (r <= 0) return false;
    off += static_cast<std::size_t>(r);
  }
  return true;
}

bool FileReader::read_all_chunked(std::vector<std::uint8_t>& out, std::size_t chunk_bytes,
                                  const std::function<void(std::size_t)>& on_chunk_read) {
  if (fd_ < 0 || chunk_bytes == 0) return false;
  struct stat st {};
  if (fstat(fd_, &st) != 0) return false;
  if (st.st_size < 0) return false;
  const auto n = static_cast<std::size_t>(st.st_size);
  out.resize(n);
  std::size_t off = 0;
  while (off < n) {
    const std::size_t want = (std::min)(n - off, chunk_bytes);
    ssize_t r = ::read(fd_, out.data() + off, want);
    if (r <= 0) return false;
    const auto ur = static_cast<std::size_t>(r);
    if (on_chunk_read) on_chunk_read(ur);
    off += ur;
  }
  return true;
}

#endif

}  // namespace structdb::infra
