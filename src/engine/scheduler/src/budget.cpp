#include "structdb/scheduler/budget.hpp"

#include <algorithm>

namespace structdb::scheduler {

ResourceBudget::ResourceBudget(BudgetConfig cfg) : cfg_(std::move(cfg)) {}

void ResourceBudget::set_wal_queue_depth_pressure_delta(std::int64_t delta) {
  std::lock_guard<std::mutex> lock(mu_);
  wal_queue_depth_pressure_delta_ = delta;
}

std::int64_t ResourceBudget::wal_queue_depth_pressure_delta() const {
  std::lock_guard<std::mutex> lock(mu_);
  return wal_queue_depth_pressure_delta_;
}

void ResourceBudget::set_compaction_slots_pressure_delta(std::int64_t delta) {
  std::lock_guard<std::mutex> lock(mu_);
  compaction_slots_pressure_delta_ = delta;
}

std::int64_t ResourceBudget::compaction_slots_pressure_delta() const {
  std::lock_guard<std::mutex> lock(mu_);
  return compaction_slots_pressure_delta_;
}

bool ResourceBudget::try_acquire(ResourceType r, std::int64_t amount, std::string* reason_out) {
  std::lock_guard<std::mutex> lock(mu_);
  switch (r) {
    case ResourceType::MemTableBytes: {
      if (mem_used_ + amount > cfg_.memtable_bytes_budget) {
        if (reason_out) *reason_out = "memtable budget";
        return false;
      }
      mem_used_ += amount;
      return true;
    }
    case ResourceType::WalQueueDepth: {
      const std::int64_t ceiling =
          (std::max)(static_cast<std::int64_t>(0), cfg_.wal_queue_depth + wal_queue_depth_pressure_delta_);
      if (wal_used_ + amount > ceiling) {
        if (reason_out) *reason_out = "wal queue";
        return false;
      }
      wal_used_ += amount;
      return true;
    }
    case ResourceType::PagePinCount: {
      if (pins_ + amount > cfg_.page_pin_budget) {
        if (reason_out) *reason_out = "page pins";
        return false;
      }
      pins_ += amount;
      return true;
    }
    case ResourceType::OpenFiles: {
      if (files_ + amount > cfg_.open_files_budget) {
        if (reason_out) *reason_out = "open files";
        return false;
      }
      files_ += amount;
      return true;
    }
    case ResourceType::CompactionSlots: {
      const std::int64_t ceiling =
          (std::max)(static_cast<std::int64_t>(0), cfg_.compaction_slots + compaction_slots_pressure_delta_);
      if (compact_ + amount > ceiling) {
        if (reason_out) *reason_out = "compaction";
        return false;
      }
      compact_ += amount;
      return true;
    }
    default:
      return true;
  }
}

void ResourceBudget::release(ResourceType r, std::int64_t amount) {
  std::lock_guard<std::mutex> lock(mu_);
  switch (r) {
    case ResourceType::MemTableBytes:
      mem_used_ -= amount;
      if (mem_used_ < 0) mem_used_ = 0;
      break;
    case ResourceType::WalQueueDepth:
      wal_used_ -= amount;
      if (wal_used_ < 0) wal_used_ = 0;
      break;
    case ResourceType::PagePinCount:
      pins_ -= amount;
      if (pins_ < 0) pins_ = 0;
      break;
    case ResourceType::OpenFiles:
      files_ -= amount;
      if (files_ < 0) files_ = 0;
      break;
    case ResourceType::CompactionSlots:
      compact_ -= amount;
      if (compact_ < 0) compact_ = 0;
      break;
    default:
      break;
  }
}

std::int64_t ResourceBudget::available(ResourceType r) const {
  std::lock_guard<std::mutex> lock(mu_);
  switch (r) {
    case ResourceType::MemTableBytes:
      return cfg_.memtable_bytes_budget - mem_used_;
    case ResourceType::WalQueueDepth:
      return (std::max)(static_cast<std::int64_t>(0), cfg_.wal_queue_depth + wal_queue_depth_pressure_delta_) -
             wal_used_;
    case ResourceType::PagePinCount:
      return cfg_.page_pin_budget - pins_;
    case ResourceType::OpenFiles:
      return cfg_.open_files_budget - files_;
    case ResourceType::CompactionSlots:
      return (std::max)(static_cast<std::int64_t>(0), cfg_.compaction_slots + compaction_slots_pressure_delta_) -
             compact_;
    default:
      return 0;
  }
}

}  // namespace structdb::scheduler
