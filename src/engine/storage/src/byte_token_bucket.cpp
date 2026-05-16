#include "structdb/storage/byte_token_bucket.hpp"

#include <algorithm>
#include <thread>

namespace structdb::storage {

void SteadyClockByteTokenBucket::set_max_bytes_per_second(std::uint64_t bytes_per_sec) {
  std::lock_guard<std::mutex> lk(mu_);
  max_bps_ = bytes_per_sec;
  inited_ = false;
}

void SteadyClockByteTokenBucket::set_burst_bytes(std::uint64_t burst_bytes) {
  std::lock_guard<std::mutex> lk(mu_);
  burst_bytes_ = burst_bytes;
  inited_ = false;
}

void SteadyClockByteTokenBucket::reset_state() {
  std::lock_guard<std::mutex> lk(mu_);
  inited_ = false;
  tokens_ = 0.0;
}

std::uint64_t SteadyClockByteTokenBucket::effective_burst_ceiling_unlocked_() const {
  if (burst_bytes_ > 0) return burst_bytes_;
  return (std::max)(max_bps_, static_cast<std::uint64_t>(65536));
}

void SteadyClockByteTokenBucket::refill_unlocked_(std::uint64_t token_ceiling) {
  if (max_bps_ == 0) return;
  const auto now = std::chrono::steady_clock::now();
  if (!inited_) {
    last_ = now;
    tokens_ = 0.0;
    inited_ = true;
    return;
  }
  const auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now - last_).count();
  last_ = now;
  tokens_ += static_cast<double>(elapsed_ns) * static_cast<double>(max_bps_) / 1e9;
  const double cap = static_cast<double>(token_ceiling);
  if (tokens_ > cap) tokens_ = cap;
}

void SteadyClockByteTokenBucket::throttle(std::uint64_t bytes, std::atomic<std::uint64_t>* sleep_ns_total_out) {
  if (max_bps_ == 0 || bytes == 0) return;
  std::uint64_t token_ceiling = bytes;
  {
    std::lock_guard<std::mutex> lk(mu_);
    token_ceiling = (std::max)(effective_burst_ceiling_unlocked_(), bytes);
  }
  for (;;) {
    bool can_consume = false;
    {
      std::lock_guard<std::mutex> lk(mu_);
      refill_unlocked_(token_ceiling);
      if (tokens_ >= static_cast<double>(bytes)) {
        tokens_ -= static_cast<double>(bytes);
        can_consume = true;
      }
    }
    if (can_consume) return;
    double deficit = 0;
    {
      std::lock_guard<std::mutex> lk(mu_);
      deficit = static_cast<double>(bytes) - tokens_;
    }
    const double wait_ns = deficit / static_cast<double>(max_bps_) * 1e9;
    const auto sleep_ns =
        std::chrono::nanoseconds((std::max)(static_cast<std::int64_t>(1), static_cast<std::int64_t>(wait_ns + 1.0)));
    std::this_thread::sleep_for(sleep_ns);
    if (sleep_ns_total_out) {
      sleep_ns_total_out->fetch_add(static_cast<std::uint64_t>(sleep_ns.count()), std::memory_order_relaxed);
    }
  }
}

void SteadyClockByteTokenBucket::refund(std::uint64_t bytes) {
  if (bytes == 0) return;
  std::lock_guard<std::mutex> lk(mu_);
  if (max_bps_ == 0) return;
  const std::uint64_t token_ceiling = (std::max)(effective_burst_ceiling_unlocked_(), bytes);
  refill_unlocked_(token_ceiling);
  tokens_ += static_cast<double>(bytes);
  const double cap = static_cast<double>(token_ceiling);
  if (tokens_ > cap) tokens_ = cap;
}

}  // namespace structdb::storage
