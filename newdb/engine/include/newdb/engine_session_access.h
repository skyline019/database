#pragma once

#include <newdb/engine_session_opaque.h>

namespace newdb {

struct Session;

/// Borrows the `Session` owned by `newdb_engine_session_create` (valid until `newdb_engine_session_destroy`).
/// For full C API / shell integration; not required for slim embed builds.
[[nodiscard]] Session* engine_session_borrow_cpp_session(newdb_engine_session_t* h) noexcept;
[[nodiscard]] const Session* engine_session_borrow_cpp_session(const newdb_engine_session_t* h) noexcept;

} // namespace newdb
