#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace structdb::infra {

struct Span {
  std::uint64_t id{0};
  std::uint64_t parent{0};
  std::string name;
  std::uint64_t plan_epoch{0};
};

class Tracer {
 public:
  virtual ~Tracer() = default;
  virtual void begin_span(const Span& s) = 0;
  virtual void end_span(std::uint64_t id) = 0;
};

void set_default_tracer(std::shared_ptr<Tracer> t);
std::shared_ptr<Tracer> default_tracer();

/// If `STRUCTDB_TRACE=1` (ASCII), installs a lightweight stderr tracer once; otherwise no-op.
/// Safe to call from multiple threads; only the first call reads the environment.
void trace_install_from_env_once();

class SpanGuard {
 public:
  SpanGuard(std::string name, std::uint64_t plan_epoch);
  ~SpanGuard();
  SpanGuard(const SpanGuard&) = delete;
  SpanGuard& operator=(const SpanGuard&) = delete;

 private:
  std::uint64_t id_{0};
};

}  // namespace structdb::infra
