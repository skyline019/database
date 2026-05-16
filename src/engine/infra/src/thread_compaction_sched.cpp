#include "structdb/infra/thread_compaction_sched.hpp"

#if defined(__linux__)
#  include <pthread.h>
#  include <sched.h>
#  include <unistd.h>
#endif

namespace structdb::infra {

void apply_compaction_background_thread_scheduling(bool enable, const char* thread_name) {
  if (!enable) return;
#if defined(__linux__)
  if (thread_name && thread_name[0] != '\0') {
    (void)pthread_setname_np(pthread_self(), thread_name);
  }
  (void)nice(10);
  sched_param sp{};
  sp.sched_priority = 0;
  (void)pthread_setschedparam(pthread_self(), SCHED_IDLE, &sp);
#else
  (void)thread_name;
#endif
}

}  // namespace structdb::infra
