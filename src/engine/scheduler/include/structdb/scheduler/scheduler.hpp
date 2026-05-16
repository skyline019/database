#pragma once

#include <functional>
#include <memory>
#include <string>

#include "structdb/planner/execution_plan.hpp"
#include "structdb/scheduler/budget.hpp"

namespace structdb::scheduler {

enum class BackpressureReason {
  None = 0,
  MemTableFull,
  WalBacklogged,
  PagePinsExhausted,
  TooManyOpenFiles,
  CompactionBusy,
};

const char* backpressure_reason_cstr(BackpressureReason r) noexcept;

using PlanEpoch = std::uint64_t;

struct SchedulerCallbacks {
  /// Called when a plan cannot proceed due to budget (executor should yield / orchestrator may replan).
  std::function<void(BackpressureReason, const planner::ExecutionPlan&)> on_backpressure;
  /// Called when a node completes successfully.
  std::function<void(planner::OperatorId, const planner::ExecutionPlan&)> on_node_done;
  /// Called when a node fails; execution may abort.
  std::function<void(planner::OperatorId, const std::string&)> on_node_error;
};

/// Owns ResourceBudget and validates plan_epoch transitions for replanning.
class ExecutionScheduler {
 public:
  explicit ExecutionScheduler(std::shared_ptr<ResourceBudget> budget);

  void set_callbacks(SchedulerCallbacks cb) { callbacks_ = std::move(cb); }

  /// Accept replacement plan only if epoch matches current or caller explicitly bumps epoch.
  bool set_active_plan(planner::ExecutionPlan plan, std::string* error_out);

  PlanEpoch active_epoch() const { return active_epoch_; }

  ResourceBudget& budget() { return *budget_; }

  const planner::ExecutionPlan* active_plan() const { return has_plan_ ? &active_plan_ : nullptr; }

  /// Hook for operators before heavy work.
  bool acquire_for_node(planner::OperatorId id, ResourceType r, std::int64_t amt, BackpressureReason* reason);

  void release_for_node(planner::OperatorId id, ResourceType r, std::int64_t amt);

 private:
  std::shared_ptr<ResourceBudget> budget_;
  SchedulerCallbacks callbacks_;
  planner::ExecutionPlan active_plan_;
  PlanEpoch active_epoch_{0};
  bool has_plan_{false};
};

}  // namespace structdb::scheduler
