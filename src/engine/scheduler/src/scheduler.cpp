#include "structdb/scheduler/scheduler.hpp"

namespace structdb::scheduler {

const char* backpressure_reason_cstr(BackpressureReason r) noexcept {
  switch (r) {
    case BackpressureReason::None:
      return "None";
    case BackpressureReason::MemTableFull:
      return "MemTableFull";
    case BackpressureReason::WalBacklogged:
      return "WalBacklogged";
    case BackpressureReason::PagePinsExhausted:
      return "PagePinsExhausted";
    case BackpressureReason::TooManyOpenFiles:
      return "TooManyOpenFiles";
    case BackpressureReason::CompactionBusy:
      return "CompactionBusy";
  }
  return "BackpressureReason(?)";
}

ExecutionScheduler::ExecutionScheduler(std::shared_ptr<ResourceBudget> budget)
    : budget_(std::move(budget)) {
  if (!budget_) budget_ = std::make_shared<ResourceBudget>();
}

bool ExecutionScheduler::set_active_plan(planner::ExecutionPlan plan, std::string* error_out) {
  std::string err;
  if (!plan.validate_dag(&err)) {
    if (error_out) *error_out = err;
    return false;
  }
  if (has_plan_ && plan.plan_epoch < active_epoch_) {
    if (error_out) *error_out = "plan_epoch regressed";
    return false;
  }
  active_plan_ = std::move(plan);
  active_epoch_ = active_plan_.plan_epoch;
  has_plan_ = true;
  return true;
}

bool ExecutionScheduler::acquire_for_node(planner::OperatorId, ResourceType r, std::int64_t amt,
                                          BackpressureReason* reason) {
  std::string why;
  if (!budget_->try_acquire(r, amt, &why)) {
    BackpressureReason br = BackpressureReason::None;
    switch (r) {
      case ResourceType::MemTableBytes:
        br = BackpressureReason::MemTableFull;
        break;
      case ResourceType::WalQueueDepth:
        br = BackpressureReason::WalBacklogged;
        break;
      case ResourceType::PagePinCount:
        br = BackpressureReason::PagePinsExhausted;
        break;
      case ResourceType::OpenFiles:
        br = BackpressureReason::TooManyOpenFiles;
        break;
      case ResourceType::CompactionSlots:
        br = BackpressureReason::CompactionBusy;
        break;
      default:
        break;
    }
    if (reason) *reason = br;
    if (callbacks_.on_backpressure && has_plan_) callbacks_.on_backpressure(br, active_plan_);
    return false;
  }
  return true;
}

void ExecutionScheduler::release_for_node(planner::OperatorId, ResourceType r, std::int64_t amt) {
  budget_->release(r, amt);
}

}  // namespace structdb::scheduler
