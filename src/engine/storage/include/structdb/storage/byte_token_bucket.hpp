#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>

namespace structdb::storage {

/// Thread-safe byte token bucket on a steady clock (WAL append, compaction merge I/O, and other byte budgets).
class SteadyClockByteTokenBucket {
 public:
  void set_max_bytes_per_second(std::uint64_t bytes_per_sec);
  void set_burst_bytes(std::uint64_t burst_bytes);
  void reset_state();

  std::uint64_t max_bytes_per_second() const {
    std::lock_guard<std::mutex> lk(mu_);
    return max_bps_;
  }

  /// Blocks until `bytes` worth of budget is available when rate limiting is enabled (`max_bps_ > 0`).
  void throttle(std::uint64_t bytes, std::atomic<std::uint64_t>* sleep_ns_total_out);

  /// Returns budget after a failed consume (e.g. WAL `append_record` throttled then write failed). No-op if disabled.
  void refund(std::uint64_t bytes);

 private:
  std::uint64_t effective_burst_ceiling_unlocked_() const;
  void refill_unlocked_(std::uint64_t token_ceiling);

  std::uint64_t max_bps_{0};
  std::uint64_t burst_bytes_{0};
  mutable std::mutex mu_;
  bool inited_{false};
  std::chrono::steady_clock::time_point last_{};
  double tokens_{0.0};
};

}  // namespace structdb::storage
