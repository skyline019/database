#include "structdb/infra/leak_detector.hpp"

#include <mutex>

#if defined(_MSC_VER) && defined(_DEBUG)
#  include <crtdbg.h>
#endif

namespace structdb::infra {

LeakDetector& LeakDetector::instance() {
  static LeakDetector k;
  return k;
}

void LeakDetector::register_allocation(void*, std::size_t bytes, const char*) {
  static std::mutex mu;
  std::lock_guard<std::mutex> lock(mu);
  ++live_;
  live_bytes_ += bytes;
}

void LeakDetector::unregister_allocation(void*) {
  static std::mutex mu;
  std::lock_guard<std::mutex> lock(mu);
  if (live_ > 0) --live_;
}

#if defined(_MSC_VER)
void LeakDetector::install_msvc_crt_leak_report() {
#  if defined(_DEBUG)
  _CrtSetDbgFlag(_CRTDBG_ALLOC_MEM_DF | _CRTDBG_LEAK_CHECK_DF);
#  endif
}
#endif

}  // namespace structdb::infra
