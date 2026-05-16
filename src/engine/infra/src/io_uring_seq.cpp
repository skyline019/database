#include "structdb/infra/io_uring_seq.hpp"

#if defined(STRUCTDB_HAVE_IO_URING) && defined(__linux__)

#  include <cerrno>
#  include <fcntl.h>
#  include <liburing.h>
#  include <sys/stat.h>
#  include <unistd.h>

#  include <cstring>

namespace structdb::infra {

struct IouringSequentialFileWriter::Impl {
  int fd{-1};
  io_uring ring{};
  bool ring_init{false};
  std::uint64_t append_pos{0};
};

IouringSequentialFileWriter::IouringSequentialFileWriter() = default;
IouringSequentialFileWriter::~IouringSequentialFileWriter() { close(); }

IouringSequentialFileWriter::IouringSequentialFileWriter(IouringSequentialFileWriter&& o) noexcept
    : impl_(std::move(o.impl_)) {}

IouringSequentialFileWriter& IouringSequentialFileWriter::operator=(IouringSequentialFileWriter&& o) noexcept {
  if (this == &o) return *this;
  close();
  impl_ = std::move(o.impl_);
  return *this;
}

void IouringSequentialFileWriter::close() {
  if (!impl_) return;
  if (impl_->ring_init) {
    io_uring_queue_exit(&impl_->ring);
    impl_->ring_init = false;
  }
  if (impl_->fd >= 0) {
    ::close(impl_->fd);
    impl_->fd = -1;
  }
  impl_.reset();
}

bool IouringSequentialFileWriter::is_open() const { return impl_ && impl_->fd >= 0; }

bool IouringSequentialFileWriter::open(const std::filesystem::path& path, bool append) {
  close();
  auto p = std::make_unique<Impl>();
  const std::string native = path.string();
  p->fd = ::open(native.c_str(), O_CREAT | O_RDWR, 0644);
  if (p->fd < 0) return false;
  if (append) {
    struct stat st {};
    if (::fstat(p->fd, &st) != 0) {
      ::close(p->fd);
      return false;
    }
    p->append_pos = static_cast<std::uint64_t>(st.st_size);
  } else {
    if (::ftruncate(p->fd, 0) != 0) {
      ::close(p->fd);
      return false;
    }
    p->append_pos = 0;
  }
  if (io_uring_queue_init(64, &p->ring, 0) != 0) {
    ::close(p->fd);
    p->fd = -1;
    return false;
  }
  p->ring_init = true;
  impl_ = std::move(p);
  return true;
}

bool IouringSequentialFileWriter::write_all(const void* data, std::size_t len) {
  if (!is_open()) return false;
  const auto* bytes = static_cast<const std::uint8_t*>(data);
  std::size_t off = 0;
  while (off < len) {
    io_uring_sqe* sqe = io_uring_get_sqe(&impl_->ring);
    if (!sqe) return false;
    const std::size_t chunk = len - off;
    io_uring_prep_write(sqe, impl_->fd, bytes + off, static_cast<unsigned>(chunk),
                        static_cast<unsigned long long>(impl_->append_pos));
    io_uring_sqe_set_flags(sqe, 0);
    if (io_uring_submit(&impl_->ring) < 0) return false;
    io_uring_cqe* cqe = nullptr;
    if (io_uring_wait_cqe(&impl_->ring, &cqe) != 0) return false;
    const int res = cqe->res;
    io_uring_cqe_seen(&impl_->ring, cqe);
    if (res < 0) return false;
    const auto wrote = static_cast<std::size_t>(res);
    if (wrote == 0) return false;
    impl_->append_pos += wrote;
    off += wrote;
  }
  return true;
}

bool IouringSequentialFileWriter::sync() {
  if (!is_open()) return false;
  return ::fdatasync(impl_->fd) == 0;
}

}  // namespace structdb::infra

#else

namespace structdb::infra {

struct IouringSequentialFileWriter::Impl {};

IouringSequentialFileWriter::IouringSequentialFileWriter() = default;
IouringSequentialFileWriter::~IouringSequentialFileWriter() = default;
IouringSequentialFileWriter::IouringSequentialFileWriter(IouringSequentialFileWriter&&) noexcept = default;
IouringSequentialFileWriter& IouringSequentialFileWriter::operator=(IouringSequentialFileWriter&&) noexcept = default;

void IouringSequentialFileWriter::close() { impl_.reset(); }

bool IouringSequentialFileWriter::is_open() const { return false; }

bool IouringSequentialFileWriter::open(const std::filesystem::path&, bool) { return false; }

bool IouringSequentialFileWriter::write_all(const void*, std::size_t) { return false; }

bool IouringSequentialFileWriter::sync() { return false; }

}  // namespace structdb::infra

#endif
