#include "structdb/planner/execution_plan.hpp"

#include <queue>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace structdb::planner {

ExecutionPlan ExecutionPlan::make_linear(std::uint64_t epoch, const std::vector<std::string>& names) {
  ExecutionPlan p;
  p.plan_epoch = epoch;
  OperatorId id = 1;
  OperatorId prev = 0;
  for (const auto& n : names) {
    OperatorNode node;
    node.id = id++;
    node.name = n;
    if (prev != 0) node.dependencies.push_back(prev);
    p.nodes.push_back(std::move(node));
    prev = p.nodes.back().id;
  }
  for (std::size_t i = 0; i < p.nodes.size(); ++i) {
    p.index_by_id[p.nodes[i].id] = i;
  }
  return p;
}

bool ExecutionPlan::validate_dag(std::string* error_out) const {
  std::unordered_set<OperatorId> ids;
  for (const auto& n : nodes) ids.insert(n.id);
  for (const auto& n : nodes) {
    for (auto d : n.dependencies) {
      if (!ids.count(d)) {
        if (error_out) *error_out = "dependency not in plan";
        return false;
      }
    }
  }

  std::unordered_map<OperatorId, int> indeg;
  std::unordered_map<OperatorId, std::vector<OperatorId>> adj;
  for (const auto& n : nodes) indeg[n.id] = static_cast<int>(n.dependencies.size());
  for (const auto& n : nodes) {
    for (auto d : n.dependencies) adj[d].push_back(n.id);
  }

  std::queue<OperatorId> q;
  for (const auto& n : nodes) {
    if (indeg[n.id] == 0) q.push(n.id);
  }

  std::size_t seen = 0;
  while (!q.empty()) {
    const auto u = q.front();
    q.pop();
    ++seen;
    for (auto v : adj[u]) {
      indeg[v]--;
      if (indeg[v] == 0) q.push(v);
    }
  }

  if (seen != nodes.size()) {
    if (error_out) *error_out = "cycle detected";
    return false;
  }
  return true;
}

}  // namespace structdb::planner
