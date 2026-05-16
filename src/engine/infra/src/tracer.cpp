#include "structdb/infra/tracer.hpp"

#include <spdlog/spdlog.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <cstdlib>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace structdb::infra {

namespace {

std::mutex g_mu;
std::shared_ptr<Tracer> g_tracer;

class NoopTracer final : public Tracer {
 public:
  void begin_span(const Span&) override {}
  void end_span(std::uint64_t) override {}
};

std::atomic<std::uint64_t> g_next_id{1};

thread_local std::vector<std::uint64_t> g_stack;

class EnvTracer final : public Tracer {
 public:
  void begin_span(const Span& s) override {
    std::lock_guard<std::mutex> lk(mu_);
    starts_[s.id] = Clock::now();
    names_[s.id] = s.name;
  }
  void end_span(std::uint64_t id) override {
    std::string name;
    Clock::time_point t0;
    {
      std::lock_guard<std::mutex> lk(mu_);
      auto it = starts_.find(id);
      if (it == starts_.end()) return;
      t0 = it->second;
      starts_.erase(it);
      auto nit = names_.find(id);
      if (nit != names_.end()) {
        name = std::move(nit->second);
        names_.erase(nit);
      }
    }
    const auto us = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - t0).count();
    auto* lg = spdlog::default_logger_raw();
    if (lg) {
      lg->log(spdlog::level::info, "[structdb trace] end name=\"{}\" id={} {}us", name, id, us);
    }
  }

 private:
  using Clock = std::chrono::steady_clock;
  std::mutex mu_;
  std::unordered_map<std::uint64_t, Clock::time_point> starts_;
  std::unordered_map<std::uint64_t, std::string> names_;
};

bool env_trace_enabled() {
#if defined(_WIN32)
  char buf[16]{};
  size_t n = 0;
  if (getenv_s(&n, buf, sizeof(buf), "STRUCTDB_TRACE") != 0 || n == 0) return false;
  return std::strcmp(buf, "1") == 0;
#else
  const char* buf = std::getenv("STRUCTDB_TRACE");
  return buf && std::strcmp(buf, "1") == 0;
#endif
}

}  // namespace

void set_default_tracer(std::shared_ptr<Tracer> t) {
  std::lock_guard<std::mutex> lock(g_mu);
  g_tracer = std::move(t);
}

std::shared_ptr<Tracer> default_tracer() {
  std::lock_guard<std::mutex> lock(g_mu);
  if (!g_tracer) g_tracer = std::make_shared<NoopTracer>();
  return g_tracer;
}

void trace_install_from_env_once() {
  static std::once_flag once;
  std::call_once(once, [] {
    if (!env_trace_enabled()) return;
    set_default_tracer(std::make_shared<EnvTracer>());
  });
}

SpanGuard::SpanGuard(std::string name, std::uint64_t plan_epoch) {
  Span s;
  s.id = g_next_id.fetch_add(1, std::memory_order_relaxed);
  s.parent = g_stack.empty() ? 0 : g_stack.back();
  s.name = std::move(name);
  s.plan_epoch = plan_epoch;
  id_ = s.id;
  g_stack.push_back(id_);
  default_tracer()->begin_span(s);
}

SpanGuard::~SpanGuard() {
  if (!g_stack.empty() && g_stack.back() == id_) {
    g_stack.pop_back();
  }
  default_tracer()->end_span(id_);
}

}  // namespace structdb::infra
