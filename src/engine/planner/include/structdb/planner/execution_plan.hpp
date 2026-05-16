#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace structdb::planner {

using OperatorId = std::uint32_t;

struct OperatorNode {
  OperatorId id{0};
  std::string name;
  bool idempotent{false};
  bool parallel_ok{true};
  double estimated_cost{1.0};
  std::vector<OperatorId> dependencies;  // must complete before this node runs
  /// Logical buffer names (DAG dataflow; operators resolve bindings in orchestrator/runtime).
  std::vector<std::string> logical_inputs;
  std::vector<std::string> logical_outputs;
};

struct ExecutionPlan {
  std::uint64_t plan_epoch{0};
  std::vector<OperatorNode> nodes;
  std::unordered_map<OperatorId, std::size_t> index_by_id;

  static ExecutionPlan make_linear(std::uint64_t epoch, const std::vector<std::string>& names);

  bool validate_dag(std::string* error_out) const;
};

}  // namespace structdb::planner
