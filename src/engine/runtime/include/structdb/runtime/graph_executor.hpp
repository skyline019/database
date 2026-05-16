#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "structdb/planner/execution_plan.hpp"
#include "structdb/runtime/operator.hpp"
#include "structdb/scheduler/scheduler.hpp"

namespace structdb::runtime {

/// Machine-readable outcome for `GraphExecutor::execute` (orthogonal to `error_out` human string).
enum class GraphExecuteOutcome : std::uint8_t {
  Ok = 0,
  PlanRejected,
  NoActivePlan,
  Cancelled,
  MissingNodeIndex,
  Backpressure,
  MissingOperator,
  PrepareFailed,
  ExecuteFailed,
};

/// Optional structured diagnostics; pass `nullptr` to ignore.
struct GraphExecuteDiagnostics {
  GraphExecuteOutcome outcome{GraphExecuteOutcome::Ok};
  scheduler::BackpressureReason backpressure_reason{scheduler::BackpressureReason::None};
  planner::OperatorId failed_node_id{0};
};

class GraphExecutor {
 public:
  void register_operator(std::string name, std::shared_ptr<IOperator> op);

  /// Ask in-flight `execute` to stop after the current node (cooperative; checked between nodes).
  void request_cancel();

  /// Topological execution with optional per-node budget probe (WAL depth, compaction slot, MemTable bytes
  /// scaled from `OperatorNode::estimated_cost`) to align with `ExecutionScheduler` / `ResourceBudget` backpressure.
  /// When `diag_out` is non-null, `outcome` (and related fields on backpressure) are set on all exit paths.
  bool execute(planner::ExecutionPlan plan, scheduler::ExecutionScheduler& sched, bool use_budget_probe,
               std::string* error_out, GraphExecuteDiagnostics* diag_out = nullptr);

 private:
  std::unordered_map<std::string, std::shared_ptr<IOperator>> ops_;
  std::mutex cancel_mu_;
  std::shared_ptr<std::atomic<bool>> active_cancel_;
};

}  // namespace structdb::runtime
