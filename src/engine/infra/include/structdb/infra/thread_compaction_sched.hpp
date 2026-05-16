#pragma once

namespace structdb::infra {

/// Best-effort background scheduling for compaction-related threads (Linux: thread name, `nice(10)`,
/// `pthread_setschedparam(SCHED_IDLE)` when permitted). No-op when `enable` is false or on unsupported OS.
void apply_compaction_background_thread_scheduling(bool enable, const char* thread_name);

}  // namespace structdb::infra
