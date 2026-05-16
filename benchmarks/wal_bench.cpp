#include <benchmark/benchmark.h>

#include <array>
#include <filesystem>

#include "structdb/storage/wal.hpp"

static void BM_WalAppendRecordNoFsync(benchmark::State& state) {
  const auto dir = std::filesystem::temp_directory_path() / "structdb_bm_wal";
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  structdb::storage::WalWriter w;
  if (!w.open(dir)) {
    state.SkipWithError("wal open");
    return;
  }
  std::array<char, 64> buf{};
  for (auto _ : state) {
    if (!w.append_record(buf.data(), buf.size(), false)) {
      state.SkipWithError("wal append");
      break;
    }
  }
  w.close();
}
BENCHMARK(BM_WalAppendRecordNoFsync);
