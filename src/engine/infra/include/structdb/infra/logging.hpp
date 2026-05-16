#pragma once

#include <memory>
#include <string_view>

namespace structdb::infra {

enum class LogLevel { trace, debug, info, warn, err, critical, off };

class Logger {
 public:
  virtual ~Logger() = default;
  virtual void log(LogLevel level, std::string_view msg) = 0;
};

void set_default_logger(std::shared_ptr<Logger> logger);
std::shared_ptr<Logger> default_logger();

void log_trace(std::string_view msg);
void log_debug(std::string_view msg);
void log_info(std::string_view msg);
void log_warn(std::string_view msg);
void log_error(std::string_view msg);

/// Installs spdlog-backed default logger (thread-safe sink).
void install_spdlog_default(std::string_view pattern = "[%H:%M:%S.%e] [%^%l%$] %v");

}  // namespace structdb::infra
