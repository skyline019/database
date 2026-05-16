#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>

#include "structdb/planner/execution_plan.hpp"
#include "structdb/runtime/graph_executor.hpp"
#include "structdb/scheduler/scheduler.hpp"

namespace structdb::orchestrator {

using PlanBuilder = std::function<planner::ExecutionPlan(std::uint64_t epoch)>;

class Orchestrator {
 public:
  Orchestrator(std::shared_ptr<scheduler::ExecutionScheduler> sched,
               std::shared_ptr<runtime::GraphExecutor> executor,
               PlanBuilder default_plan);

  void set_plan_builder(PlanBuilder b) {
    std::lock_guard<std::mutex> lock(mu_);
    builder_ = std::move(b);
  }

  std::uint64_t config_version() const { return config_version_.load(std::memory_order_relaxed); }
  void bump_config_version() { config_version_.fetch_add(1, std::memory_order_relaxed); }

  /// Runs default plan at current epoch.
  bool run_default(std::string* error_out);

  /// Increments epoch and runs a new plan (replan).
  bool replan_and_run(std::string* error_out);

  scheduler::ExecutionScheduler& scheduler() { return *sched_; }
  runtime::GraphExecutor& executor() { return *exec_; }

  /// Invoked when the scheduler reports backpressure (GraphExecutor budget probe or future I/O).
  void set_on_backpressure(std::function<void(scheduler::BackpressureReason)> cb);

  /// Runs immediately before each `GraphExecutor::execute` in `run_default` / `replan_and_run` (e.g. sync scheduler
  /// budget from `StorageEngine::read_storage_pressure_snapshot`). Optional; default = no-op.
  void set_before_graph_execute(std::function<void()> hook);

 private:
  std::mutex mu_;
  std::shared_ptr<scheduler::ExecutionScheduler> sched_;
  std::shared_ptr<runtime::GraphExecutor> exec_;
  PlanBuilder builder_;
  std::atomic<std::uint64_t> config_version_{1};
  std::atomic<std::uint64_t> plan_epoch_{1};
  std::mutex pressure_mu_;
  std::function<void(scheduler::BackpressureReason)> on_backpressure_;
  std::mutex before_exec_mu_;
  std::function<void()> before_graph_execute_;
};

}  // namespace structdb::orchestrator
