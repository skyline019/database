#pragma once

#include <cstddef>

namespace structdb::infra {

/// Debug-only hooks: track pooled objects / assert balance at shutdown.
class LeakDetector {
 public:
  static LeakDetector& instance();

  void register_allocation(void* p, std::size_t bytes, const char* tag);
  void unregister_allocation(void* p);

  std::size_t live_count() const { return live_; }
  std::size_t live_bytes() const { return live_bytes_; }

#if defined(_MSC_VER)
  /// Enables CRT leak dump at process exit (MSVC debug builds / _DEBUG).
  static void install_msvc_crt_leak_report();
#endif

 private:
  LeakDetector() = default;
  std::size_t live_{0};
  std::size_t live_bytes_{0};
};

}  // namespace structdb::infra

#define STRUCTDB_LEAK_TRACK(p, bytes, tag) \
  structdb::infra::LeakDetector::instance().register_allocation((p), (bytes), (tag))
#define STRUCTDB_LEAK_UNTRACK(p) structdb::infra::LeakDetector::instance().unregister_allocation((p))
