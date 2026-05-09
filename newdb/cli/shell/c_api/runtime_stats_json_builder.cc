#include "cli/shell/c_api/runtime_stats_json_builder.h"

#include <newdb/json_escape.h>
#include <newdb/runtime_stats_snapshot_json_write.h>

#include <chrono>
#include <sstream>

#include "cli/modules/txn/coordinator/txn_manager.h"
#include "cli/modules/where/executor/where.h"
#include "cli/shell/state/shell_state_facade.h"

namespace newdb::capi_cli {
#include "_generated_runtime_json.inc"

std::string format_runtime_stats_json(ShellStateFacade& facade) {
    return build_runtime_stats_json(facade);
}

std::string format_runtime_snapshot_jsonl_line(ShellStateFacade& facade, const std::string& label) {
    return build_runtime_snapshot_jsonl_line(facade, label);
}

} // namespace newdb::capi_cli
