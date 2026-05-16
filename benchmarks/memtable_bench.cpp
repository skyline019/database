#include <benchmark/benchmark.h>

#include "structdb/storage/memtable.hpp"

static void BM_MemTablePut(benchmark::State& state) {
  structdb::storage::MemTable t;
  std::size_t i = 0;
  for (auto _ : state) {
    t.put(std::string("k") + std::to_string(i), std::string(32, 'x'));
    ++i;
  }
}
BENCHMARK(BM_MemTablePut);
