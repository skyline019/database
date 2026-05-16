#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <string_view>

namespace structdb::planner {
struct ExecutionPlan;
struct OperatorNode;
}  // namespace structdb::planner

namespace structdb::scheduler {
class ExecutionScheduler;
}

namespace structdb::runtime {

struct OperatorContext {
  const planner::ExecutionPlan* plan{nullptr};
  const planner::OperatorNode* node{nullptr};
  scheduler::ExecutionScheduler* sched{nullptr};
  /// When non-null, operators may poll `cancel_requested->load()` to cooperate with GraphExecutor::request_cancel().
  const std::atomic<bool>* cancel_requested{nullptr};
};

class IOperator {
 public:
  virtual ~IOperator() = default;
  virtual std::string_view name() const = 0;
  virtual bool prepare(OperatorContext&) { return true; }
  virtual bool execute(OperatorContext&) = 0;
  virtual void rollback(OperatorContext&) {}
};

}  // namespace structdb::runtime
