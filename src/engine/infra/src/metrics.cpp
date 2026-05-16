#include "structdb/infra/metrics.hpp"

namespace structdb::infra {

namespace {

std::mutex g_mu;
std::shared_ptr<MetricsRegistry> g_metrics;

}  // namespace

void MetricsRegistry::counter_add(std::string_view name, std::int64_t delta) {
  std::lock_guard<std::mutex> lock(mu_);
  counters_[std::string(name)] += delta;
}

std::int64_t MetricsRegistry::counter_get(std::string_view name) const {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = counters_.find(std::string(name));
  if (it == counters_.end()) return 0;
  return it->second;
}

void MetricsRegistry::histogram_record(std::string_view name, double value) {
  std::lock_guard<std::mutex> lock(mu_);
  histograms_[std::string(name)].push_back(value);
}

std::vector<double> MetricsRegistry::histogram_snapshot(std::string_view name) const {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = histograms_.find(std::string(name));
  if (it == histograms_.end()) return {};
  return it->second;
}

void set_default_metrics(std::shared_ptr<MetricsRegistry> m) {
  std::lock_guard<std::mutex> lock(g_mu);
  g_metrics = std::move(m);
}

std::shared_ptr<MetricsRegistry> default_metrics() {
  std::lock_guard<std::mutex> lock(g_mu);
  if (!g_metrics) g_metrics = std::make_shared<MetricsRegistry>();
  return g_metrics;
}

}  // namespace structdb::infra
