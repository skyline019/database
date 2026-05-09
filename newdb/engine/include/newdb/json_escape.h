#pragma once

#include <string>
#include <string_view>

namespace newdb {

[[nodiscard]] std::string json_escape(std::string_view s);

} // namespace newdb
