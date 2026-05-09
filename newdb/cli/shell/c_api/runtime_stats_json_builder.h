#pragma once

#include <string>

struct ShellStateFacade;

namespace newdb::capi_cli {

[[nodiscard]] std::string format_runtime_stats_json(ShellStateFacade& facade);
[[nodiscard]] std::string format_runtime_snapshot_jsonl_line(ShellStateFacade& facade, const std::string& label);

} // namespace newdb::capi_cli
