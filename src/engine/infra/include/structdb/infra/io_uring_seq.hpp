#pragma once

#include <cstddef>
#include <filesystem>
#include <memory>

namespace structdb::infra {

/// Sequential append-only writer using **liburing** `io_uring` write + completion wait (Phase 21B).
/// When `STRUCTDB_HAVE_IO_URING` is unset or non-Linux, all methods are no-ops / return false.
class IouringSequentialFileWriter {
 public:
  IouringSequentialFileWriter();
  ~IouringSequentialFileWriter();
  IouringSequentialFileWriter(IouringSequentialFileWriter&&) noexcept;
  IouringSequentialFileWriter& operator=(IouringSequentialFileWriter&&) noexcept;
  IouringSequentialFileWriter(const IouringSequentialFileWriter&) = delete;
  IouringSequentialFileWriter& operator=(const IouringSequentialFileWriter&) = delete;

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
