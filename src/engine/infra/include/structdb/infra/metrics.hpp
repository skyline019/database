#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace structdb::infra {

class MetricsRegistry {
 public:
  void counter_add(std::string_view name, std::int64_t delta = 1);
  std::int64_t counter_get(std::string_view name) const;

  void histogram_record(std::string_view name, double value);
  std::vector<double> histogram_snapshot(std::string_view name) const;

 private:
  mutable std::mutex mu_;
  std::unordered_map<std::string, std::int64_t> counters_;
  std::unordered_map<std::string, std::vector<double>> histograms_;
};

void set_default_metrics(std::shared_ptr<MetricsRegistry> m);
std::shared_ptr<MetricsRegistry> default_metrics();

}  // namespace structdb::infra
