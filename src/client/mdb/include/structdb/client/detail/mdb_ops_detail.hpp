#pragma once

#include <cstddef>

#if defined(_MSC_VER)
#  include <cstring>
#else
#  include <strings.h>
#endif

namespace structdb::client::mdb::detail {

inline int ascii_strncasecmp(const char* a, const char* b, std::size_t n) {
#if defined(_MSC_VER)
  return _strnicmp(a, b, static_cast<int>(n));
#else
  if (n == 0) return 0;
  return ::strncasecmp(a, b, n);
#endif
}

}  // namespace structdb::client::mdb::detail
