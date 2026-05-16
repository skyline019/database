#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>

namespace structdb::scheduler {

enum class ResourceType : std::uint8_t {
  MemTableBytes = 0,
  WalQueueDepth,
  PagePinCount,
  OpenFiles,
  CompactionSlots,
  COUNT
};

struct BudgetConfig {
  std::int64_t memtable_bytes_budget{64 * 1024 * 1024};
  std::int64_t wal_queue_depth{1024};
  std::int64_t page_pin_budget{4096};
  std::int64_t open_files_budget{512};
  std::int64_t compaction_slots{2};
};

class ResourceBudget {
 public:
  explicit ResourceBudget(BudgetConfig cfg = {});

  bool try_acquire(ResourceType r, std::int64_t amount, std::string* reason_out);
  void release(ResourceType r, std::int64_t amount);

  std::int64_t available(ResourceType r) const;

  /// Phase 14: negative values tighten the effective WAL queue ceiling for scheduler backpressure experiments.
  void set_wal_queue_depth_pressure_delta(std::int64_t delta);
  std::int64_t wal_queue_depth_pressure_delta() const;

  /// Phase 21C: negative values tighten the effective `CompactionSlots` ceiling (same shape as WAL delta).
  void set_compaction_slots_pressure_delta(std::int64_t delta);
  std::int64_t compaction_slots_pressure_delta() const;

 private:
  BudgetConfig cfg_;
  mutable std::mutex mu_;
  std::int64_t mem_used_{0};
  std::int64_t wal_used_{0};
  std::int64_t pins_{0};
  std::int64_t files_{0};
  std::int64_t compact_{0};
  std::int64_t wal_queue_depth_pressure_delta_{0};
  std::int64_t compaction_slots_pressure_delta_{0};
};

}  // namespace structdb::scheduler
