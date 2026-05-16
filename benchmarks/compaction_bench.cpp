#include <algorithm>
#include <benchmark/benchmark.h>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

/// Baseline for "pick inputs for an L0 compaction" without touching disk (MVP stub).
static void BM_CompactionPickL0Batch(benchmark::State& state) {
  const int n = static_cast<int>(state.range(0));
  std::vector<std::string> l0;
  l0.reserve(static_cast<std::size_t>(n));
  for (int i = 0; i < n; ++i) {
    l0.push_back(std::string("L0-") + std::to_string(i) + ".sst");
  }
  const std::size_t k_batch = 4;
  for (auto _ : state) {
    const std::size_t take = (std::min)(k_batch, l0.size());
    std::vector<std::string> pick(l0.begin(), l0.begin() + static_cast<std::ptrdiff_t>(take));
    benchmark::DoNotOptimize(pick.data());
    benchmark::DoNotOptimize(pick.size());
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()));
}

BENCHMARK(BM_CompactionPickL0Batch)->Arg(8)->Arg(64)->Arg(512);
