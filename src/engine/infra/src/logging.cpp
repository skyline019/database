#include "structdb/infra/logging.hpp"

#include <fmt/format.h>
#include <mutex>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

namespace structdb::infra {

namespace {

std::mutex g_mu;
std::shared_ptr<Logger> g_logger;

class SpdLogger final : public Logger {
 public:
  void log(LogLevel level, std::string_view msg) override {
    auto lvl = spdlog::level::info;
    switch (level) {
      case LogLevel::trace: lvl = spdlog::level::trace; break;
      case LogLevel::debug: lvl = spdlog::level::debug; break;
      case LogLevel::info: lvl = spdlog::level::info; break;
      case LogLevel::warn: lvl = spdlog::level::warn; break;
      case LogLevel::err: lvl = spdlog::level::err; break;
      case LogLevel::critical: lvl = spdlog::level::critical; break;
      case LogLevel::off: lvl = spdlog::level::off; break;
    }
    spdlog::log(lvl, "{}", msg);
  }
};

void log_with(LogLevel lvl, std::string_view msg) {
  std::lock_guard<std::mutex> lock(g_mu);
  if (g_logger) g_logger->log(lvl, msg);
}

}  // namespace

void set_default_logger(std::shared_ptr<Logger> logger) {
  std::lock_guard<std::mutex> lock(g_mu);
  g_logger = std::move(logger);
}

std::shared_ptr<Logger> default_logger() {
  std::lock_guard<std::mutex> lock(g_mu);
  return g_logger;
}

void log_trace(std::string_view msg) { log_with(LogLevel::trace, msg); }
void log_debug(std::string_view msg) { log_with(LogLevel::debug, msg); }
void log_info(std::string_view msg) { log_with(LogLevel::info, msg); }
void log_warn(std::string_view msg) { log_with(LogLevel::warn, msg); }
void log_error(std::string_view msg) { log_with(LogLevel::err, msg); }

void install_spdlog_default(std::string_view pattern) {
  auto sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
  auto logger = std::make_shared<spdlog::logger>("structdb", sink);
  spdlog::set_default_logger(logger);
  spdlog::set_level(spdlog::level::debug);
  spdlog::set_pattern(std::string(pattern));
  set_default_logger(std::make_shared<SpdLogger>());
}

}  // namespace structdb::infra
