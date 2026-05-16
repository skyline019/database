#pragma once

#include <cstddef>
#include <filesystem>
#include <memory>

namespace structdb::infra {

/// Sequential append-only writer using **one** IOCP + `OVERLAPPED` writes with completion waits (Phase 20C).
/// When `STRUCTDB_HAVE_IOCP` is unset or non-Windows, all methods are no-ops / return false.
class IocpSequentialFileWriter {
 public:
  IocpSequentialFileWriter();
  ~IocpSequentialFileWriter();
  IocpSequentialFileWriter(IocpSequentialFileWriter&&) noexcept;
  IocpSequentialFileWriter& operator=(IocpSequentialFileWriter&&) noexcept;
  IocpSequentialFileWriter(const IocpSequentialFileWriter&) = delete;
  IocpSequentialFileWriter& operator=(const IocpSequentialFileWriter&) = delete;

  bool open(const std::filesystem::path& path, bool append);
  void close();
  bool is_open() const;
  bool write_all(const void* data, std::size_t len);
  bool sync();

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace structdb::infra
