#include "structdb/orchestrator/orchestrator.hpp"

namespace structdb::orchestrator {

Orchestrator::Orchestrator(std::shared_ptr<scheduler::ExecutionScheduler> sched,
                           std::shared_ptr<runtime::GraphExecutor> executor,
                           PlanBuilder default_plan)
    : sched_(std::move(sched)), exec_(std::move(executor)), builder_(std::move(default_plan)) {
  scheduler::SchedulerCallbacks cbs;
  cbs.on_backpressure = [this](scheduler::BackpressureReason r, const planner::ExecutionPlan& p) {
    (void)p;
    std::function<void(scheduler::BackpressureReason)> copy;
    {
      std::lock_guard<std::mutex> lock(pressure_mu_);
      copy = on_backpressure_;
    }
    if (copy) copy(r);
  };
  sched_->set_callbacks(std::move(cbs));
}

void Orchestrator::set_on_backpressure(std::function<void(scheduler::BackpressureReason)> cb) {
  std::lock_guard<std::mutex> lock(pressure_mu_);
  on_backpressure_ = std::move(cb);
}

void Orchestrator::set_before_graph_execute(std::function<void()> hook) {
  std::lock_guard<std::mutex> lock(before_exec_mu_);
  before_graph_execute_ = std::move(hook);
}

bool Orchestrator::run_default(std::string* error_out) {
  std::lock_guard<std::mutex> lock(mu_);
  if (!builder_) {
    if (error_out) *error_out = "no plan builder";
    return false;
  }
  const auto epoch = plan_epoch_.load(std::memory_order_relaxed);
  auto plan = builder_(epoch);
  plan.plan_epoch = epoch;
  std::function<void()> pre;
  {
    std::lock_guard<std::mutex> lk(before_exec_mu_);
    pre = before_graph_execute_;
  }
  if (pre) pre();
  return exec_->execute(std::move(plan), *sched_, true, error_out);
}

bool Orchestrator::replan_and_run(std::string* error_out) {
  std::lock_guard<std::mutex> lock(mu_);
  if (!builder_) {
    if (error_out) *error_out = "no plan builder";
    return false;
  }
  const auto epoch = plan_epoch_.fetch_add(1, std::memory_order_relaxed) + 1;
  auto plan = builder_(epoch);
  plan.plan_epoch = epoch;
  std::function<void()> pre;
  {
    std::lock_guard<std::mutex> lk(before_exec_mu_);
    pre = before_graph_execute_;
  }
  if (pre) pre();
  return exec_->execute(std::move(plan), *sched_, true, error_out);
}

}  // namespace structdb::orchestrator
