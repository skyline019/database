#pragma once

#include "file_handle.hpp"
#include "thread_pool.hpp"

#include <cstdint>

// Blocking I/O backend (first milestone).
//
// Optional WAL backends: Windows **IOCP** (`STRUCTDB_HAVE_IOCP`, CMake `STRUCTDB_WITH_IOCP`) and Linux **io_uring**
// (`STRUCTDB_HAVE_IO_URING`, CMake `STRUCTDB_WITH_IO_URING` + liburing). Append ordering is documented as
// `structdb::storage::WalPipeline` on `WalWriter` (see `Docs/PHASE21.md`).
//
// **Support matrix (normative copy in `Docs/POLICY.md` §2.4)**: default `Blocking` everywhere; `IocpAsync` on Windows
// MSVC when built with IOCP; `IoUringAsync` on Linux only when liburing is linked (CMake opt-in). MinGW / non-MSVC
// Windows targets use `Blocking` unless a future port documents otherwise.

namespace structdb::infra {

/// Logical I/O backend kind. `Blocking` matches `FileWriter` / `WalWriter` default behaviour.
enum class IoBackendKind : std::uint8_t {
  Blocking = 0,
  IocpAsync = 1,
  IoUringAsync = 2,
};

struct IoBackendConfig {
  IoBackendKind kind{IoBackendKind::Blocking};
};

inline constexpr bool io_backend_kind_is_blocking(IoBackendKind k) noexcept {
  return k == IoBackendKind::Blocking;
}

inline constexpr bool io_backend_kind_is_async_placeholder(IoBackendKind k) noexcept {
  return k == IoBackendKind::IocpAsync || k == IoBackendKind::IoUringAsync;
}

}  // namespace structdb::infra
