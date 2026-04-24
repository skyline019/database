#pragma once

#include <string>

namespace newdb {

// Stable machine-parseable error line:
// [ERR] domain=<domain> code=<code> message="<escaped message>"
std::string format_error_line(const std::string& domain,
                              const std::string& code,
                              const std::string& message);

} // namespace newdb
