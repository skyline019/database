#include "structdb/infra/io_iocp.hpp"

#if defined(STRUCTDB_HAVE_IOCP) && defined(_WIN32)
#  ifndef NOMINMAX
#    define NOMINMAX
#  endif
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif
#  include <windows.h>

#  include <algorithm>
#  include <cstdint>
#  include <memory>

namespace structdb::infra {

struct IocpSequentialFileWriter::Impl {
  HANDLE file{INVALID_HANDLE_VALUE};
  HANDLE iocp{INVALID_HANDLE_VALUE};
  OVERLAPPED ov{};
  std::uint64_t append_pos{0};
};

IocpSequentialFileWriter::IocpSequentialFileWriter() = default;
IocpSequentialFileWriter::~IocpSequentialFileWriter() { close(); }

IocpSequentialFileWriter::IocpSequentialFileWriter(IocpSequentialFileWriter&& o) noexcept : impl_(std::move(o.impl_)) {}
IocpSequentialFileWriter& IocpSequentialFileWriter::operator=(IocpSequentialFileWriter&& o) noexcept {
  if (this == &o) return *this;
  close();
  impl_ = std::move(o.impl_);
  return *this;
}

void IocpSequentialFileWriter::close() {
  if (!impl_) return;
  if (impl_->file != INVALID_HANDLE_VALUE) {
    CloseHandle(impl_->file);
    impl_->file = INVALID_HANDLE_VALUE;
  }
  if (impl_->iocp != nullptr && impl_->iocp != INVALID_HANDLE_VALUE) {
    CloseHandle(impl_->iocp);
    impl_->iocp = nullptr;
  }
  impl_.reset();
}

bool IocpSequentialFileWriter::is_open() const { return impl_ && impl_->file != INVALID_HANDLE_VALUE; }

bool IocpSequentialFileWriter::open(const std::filesystem::path& path, bool append) {
  close();
  auto p = std::make_unique<Impl>();
  const DWORD creation = append ? OPEN_ALWAYS : CREATE_ALWAYS;
  const DWORD flags = FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OVERLAPPED;
  p->file = CreateFileW(path.c_str(), GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ, nullptr, creation, flags,
                        nullptr);
  if (p->file == INVALID_HANDLE_VALUE) return false;
  p->iocp = CreateIoCompletionPort(INVALID_HANDLE_VALUE, nullptr, 0, 1);
  if (p->iocp == nullptr) {
    CloseHandle(p->file);
    return false;
  }
  if (CreateIoCompletionPort(p->file, p->iocp, 1, 0) == nullptr) {
    CloseHandle(p->iocp);
    CloseHandle(p->file);
    return false;
  }
  if (append) {
    LARGE_INTEGER li{};
    if (!GetFileSizeEx(p->file, &li)) {
      CloseHandle(p->iocp);
      CloseHandle(p->file);
      return false;
    }
    p->append_pos = static_cast<std::uint64_t>(li.QuadPart);
  } else {
    p->append_pos = 0;
  }
  impl_ = std::move(p);
  return true;
}

bool IocpSequentialFileWriter::write_all(const void* data, std::size_t len) {
  if (!is_open()) return false;
  const auto* bytes = static_cast<const std::uint8_t*>(data);
  std::uint64_t cursor = impl_->append_pos;
  std::size_t off = 0;
  while (off < len) {
    const std::size_t chunk =
        (std::min)(len - off, static_cast<std::size_t>(static_cast<std::size_t>(1) << 20));
    ZeroMemory(&impl_->ov, sizeof(OVERLAPPED));
    impl_->ov.Offset = static_cast<DWORD>(cursor & 0xffffffffu);
    impl_->ov.OffsetHigh = static_cast<DWORD>(cursor >> 32);
    const DWORD n = static_cast<DWORD>(chunk);
    BOOL wok = WriteFile(impl_->file, bytes + off, n, nullptr, &impl_->ov);
    if (!wok) {
      const DWORD err = GetLastError();
      if (err != ERROR_IO_PENDING) return false;
    }
    DWORD transferred = 0;
    ULONG_PTR key = 0;
    OVERLAPPED* ovl = nullptr;
    if (!GetQueuedCompletionStatus(impl_->iocp, &transferred, &key, &ovl, INFINITE)) return false;
    if (ovl != &impl_->ov) return false;
    if (transferred == 0) return false;
    cursor += transferred;
    off += transferred;
  }
  impl_->append_pos = cursor;
  return true;
}

bool IocpSequentialFileWriter::sync() {
  if (!is_open()) return false;
  return FlushFileBuffers(impl_->file) != 0;
}

}  // namespace structdb::infra

#else

namespace structdb::infra {

struct IocpSequentialFileWriter::Impl {};

IocpSequentialFileWriter::IocpSequentialFileWriter() = default;
IocpSequentialFileWriter::~IocpSequentialFileWriter() = default;
IocpSequentialFileWriter::IocpSequentialFileWriter(IocpSequentialFileWriter&&) noexcept = default;
IocpSequentialFileWriter& IocpSequentialFileWriter::operator=(IocpSequentialFileWriter&&) noexcept = default;

void IocpSequentialFileWriter::close() { impl_.reset(); }

bool IocpSequentialFileWriter::is_open() const { return false; }

bool IocpSequentialFileWriter::open(const std::filesystem::path&, bool) { return false; }

bool IocpSequentialFileWriter::write_all(const void*, std::size_t) { return false; }

bool IocpSequentialFileWriter::sync() { return false; }

}  // namespace structdb::infra

#endif
