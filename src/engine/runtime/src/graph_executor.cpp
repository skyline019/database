#include "structdb/runtime/graph_executor.hpp"

#include <cmath>
#include <cstdint>
#include <queue>
#include <string>
#include <unordered_map>

#include "structdb/scheduler/budget.hpp"

namespace structdb::runtime {
namespace {

std::int64_t probe_memtable_bytes_for_node(const planner::OperatorNode& node) {
  constexpr std::int64_t kMin = 1;
  constexpr std::int64_t kMax = 1LL << 20;
  const double c = node.estimated_cost > 0.0 ? node.estimated_cost : 1.0;
  const auto raw = static_cast<std::int64_t>(std::ceil(c * 8192.0));
  if (raw < kMin) return kMin;
  if (raw > kMax) return kMax;
  return raw;
}

}  // namespace

void GraphExecutor::register_operator(std::string name, std::shared_ptr<IOperator> op) {
  ops_[std::move(name)] = std::move(op);
}

void GraphExecutor::request_cancel() {
  std::lock_guard<std::mutex> lock(cancel_mu_);
  if (active_cancel_) active_cancel_->store(true, std::memory_order_relaxed);
}

bool GraphExecutor::execute(planner::ExecutionPlan plan, scheduler::ExecutionScheduler& sched,
                            bool use_budget_probe, std::string* error_out, GraphExecuteDiagnostics* diag_out) {
  auto mark = [&](GraphExecuteOutcome o, planner::OperatorId nid = 0,
                  scheduler::BackpressureReason br = scheduler::BackpressureReason::None) {
    if (!diag_out) return;
    diag_out->outcome = o;
    diag_out->failed_node_id = nid;
    diag_out->backpressure_reason = br;
  };
  mark(GraphExecuteOutcome::Ok);

  if (!sched.set_active_plan(std::move(plan), error_out)) {
    mark(GraphExecuteOutcome::PlanRejected);
    return false;
  }
  const auto* p = sched.active_plan();
  if (!p) {
    if (error_out) *error_out = "no active plan";
    mark(GraphExecuteOutcome::NoActivePlan);
    return false;
  }

  auto cancel = std::make_shared<std::atomic<bool>>(false);
  {
    std::lock_guard<std::mutex> lock(cancel_mu_);
    active_cancel_ = cancel;
  }

  auto clear_cancel = [&]() {
    std::lock_guard<std::mutex> lock(cancel_mu_);
    if (active_cancel_ == cancel) active_cancel_.reset();
  };

  std::unordered_map<planner::OperatorId, int> indeg;
  std::unordered_map<planner::OperatorId, std::vector<planner::OperatorId>> adj;
  for (const auto& n : p->nodes) indeg[n.id] = static_cast<int>(n.dependencies.size());
  for (const auto& n : p->nodes) {
    for (auto d : n.dependencies) adj[d].push_back(n.id);
  }

  std::queue<planner::OperatorId> q;
  for (const auto& n : p->nodes) {
    if (indeg[n.id] == 0) q.push(n.id);
  }

  while (!q.empty()) {
    if (cancel->load(std::memory_order_relaxed)) {
      if (error_out) *error_out = "cancelled";
      mark(GraphExecuteOutcome::Cancelled);
      clear_cancel();
      return false;
    }

    const auto id = q.front();
    q.pop();
    const auto it = p->index_by_id.find(id);
    if (it == p->index_by_id.end()) {
      if (error_out) *error_out = "missing node index";
      mark(GraphExecuteOutcome::MissingNodeIndex, id);
      clear_cancel();
      return false;
    }
    const auto& node = p->nodes[it->second];

    const std::int64_t mem_probe = probe_memtable_bytes_for_node(node);
    if (use_budget_probe) {
      scheduler::BackpressureReason br{};
      struct Probe {
        scheduler::ResourceType r;
        std::int64_t amt;
      };
      const Probe kProbe[] = {
          {scheduler::ResourceType::WalQueueDepth, 1},
          {scheduler::ResourceType::CompactionSlots, 1},
          {scheduler::ResourceType::MemTableBytes, mem_probe},
      };
      int acquired = 0;
      for (const auto& pr : kProbe) {
        if (!sched.acquire_for_node(id, pr.r, pr.amt, &br)) {
          for (int j = acquired - 1; j >= 0; --j) {
            sched.release_for_node(id, kProbe[j].r, kProbe[j].amt);
          }
          if (error_out) {
            *error_out = std::string("backpressure: ") + scheduler::backpressure_reason_cstr(br);
          }
          mark(GraphExecuteOutcome::Backpressure, id, br);
          clear_cancel();
          return false;
        }
        ++acquired;
      }
    }

    auto opit = ops_.find(node.name);
    if (opit == ops_.end()) {
      if (error_out) *error_out = std::string("no operator: ") + node.name;
      if (use_budget_probe) {
        sched.release_for_node(id, scheduler::ResourceType::MemTableBytes, mem_probe);
        sched.release_for_node(id, scheduler::ResourceType::CompactionSlots, 1);
        sched.release_for_node(id, scheduler::ResourceType::WalQueueDepth, 1);
      }
      mark(GraphExecuteOutcome::MissingOperator, id);
      clear_cancel();
      return false;
    }

    OperatorContext ctx{p, &node, &sched, cancel.get()};
    if (!opit->second->prepare(ctx)) {
      if (error_out) *error_out = "prepare failed: " + node.name;
      if (use_budget_probe) {
        sched.release_for_node(id, scheduler::ResourceType::MemTableBytes, mem_probe);
        sched.release_for_node(id, scheduler::ResourceType::CompactionSlots, 1);
        sched.release_for_node(id, scheduler::ResourceType::WalQueueDepth, 1);
      }
      mark(GraphExecuteOutcome::PrepareFailed, id);
      clear_cancel();
      return false;
    }
    if (!opit->second->execute(ctx)) {
      if (error_out) *error_out = "execute failed: " + node.name;
      opit->second->rollback(ctx);
      if (use_budget_probe) {
        sched.release_for_node(id, scheduler::ResourceType::MemTableBytes, mem_probe);
        sched.release_for_node(id, scheduler::ResourceType::CompactionSlots, 1);
        sched.release_for_node(id, scheduler::ResourceType::WalQueueDepth, 1);
      }
      mark(GraphExecuteOutcome::ExecuteFailed, id);
      clear_cancel();
      return false;
    }

    if (use_budget_probe) {
      sched.release_for_node(id, scheduler::ResourceType::MemTableBytes, mem_probe);
      sched.release_for_node(id, scheduler::ResourceType::CompactionSlots, 1);
      sched.release_for_node(id, scheduler::ResourceType::WalQueueDepth, 1);
    }

    for (auto v : adj[id]) {
      indeg[v]--;
      if (indeg[v] == 0) q.push(v);
    }
  }

  if (cancel->load(std::memory_order_relaxed)) {
    if (error_out) *error_out = "cancelled";
    mark(GraphExecuteOutcome::Cancelled);
    clear_cancel();
    return false;
  }

  mark(GraphExecuteOutcome::Ok);
  clear_cancel();
  return true;
}

}  // namespace structdb::runtime
