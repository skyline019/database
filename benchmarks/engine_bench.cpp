#include <benchmark/benchmark.h>

#include <filesystem>
#include <string>
#include <string_view>

#include "structdb/storage/storage_engine.hpp"

static void BM_StdbStoragePutNoFsync(benchmark::State& state) {
  const auto dir = std::filesystem::temp_directory_path() / "structdb_bm_engine_put";
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  structdb::storage::StorageEngine eng(dir / "data");
  std::string err;
  if (!eng.open(&err)) {
    state.SkipWithError(err.c_str());
    return;
  }
  std::string key = "mdb$bench";
  std::string val = "v";
  for (auto _ : state) {
    if (!eng.put(key, val, false)) {
      state.SkipWithError("put");
      break;
    }
  }
  eng.close();
  std::filesystem::remove_all(dir);
}
BENCHMARK(BM_StdbStoragePutNoFsync);

static void BM_StdbStorageGetAfterPuts(benchmark::State& state) {
  const auto dir = std::filesystem::temp_directory_path() / "structdb_bm_engine_get";
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  structdb::storage::StorageEngine eng(dir / "data");
  std::string err;
  if (!eng.open(&err)) {
    state.SkipWithError(err.c_str());
    return;
  }
  for (int i = 0; i < 1000; ++i) {
    if (!eng.put("mdb$k" + std::to_string(i), "x", false)) {
      state.SkipWithError("put seed");
      eng.close();
      return;
    }
  }
  std::string out;
  for (auto _ : state) {
    out.clear();
    if (!eng.get("mdb$k500", &out)) {
      state.SkipWithError("get");
      break;
    }
    benchmark::DoNotOptimize(out);
  }
  eng.close();
  std::filesystem::remove_all(dir);
}
BENCHMARK(BM_StdbStorageGetAfterPuts);

static void BM_StdbStorageVisitPrefix(benchmark::State& state) {
  const auto dir = std::filesystem::temp_directory_path() / "structdb_bm_engine_prefix";
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  structdb::storage::StorageEngine eng(dir / "data");
  std::string err;
  if (!eng.open(&err)) {
    state.SkipWithError(err.c_str());
    return;
  }
  for (int i = 0; i < 500; ++i) {
    if (!eng.put("mdb$p" + std::to_string(i), "z", false)) {
      state.SkipWithError("put seed");
      eng.close();
      return;
    }
  }
  if (!eng.flush_memtable(&err)) {
    state.SkipWithError(err.c_str());
    eng.close();
    return;
  }
  std::size_t n = 0;
  for (auto _ : state) {
    n = 0;
    eng.visit_prefix("mdb$p", [&](std::string_view, std::string_view) {
      ++n;
      return true;
    });
    benchmark::DoNotOptimize(n);
  }
  eng.close();
  std::filesystem::remove_all(dir);
}
BENCHMARK(BM_StdbStorageVisitPrefix);

static void BM_StdbStorageOpenReplay(benchmark::State& state) {
  const auto dir = std::filesystem::temp_directory_path() / "structdb_bm_engine_reopen";
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  {
    structdb::storage::StorageEngine eng(dir / "data");
    std::string err;
    if (!eng.open(&err)) {
      state.SkipWithError(err.c_str());
      return;
    }
    for (int i = 0; i < 2000; ++i) {
      if (!eng.put("mdb$r" + std::to_string(i), "w", false)) {
        state.SkipWithError("put seed");
        eng.close();
        return;
      }
    }
    eng.close();
  }
  for (auto _ : state) {
    structdb::storage::StorageEngine eng(dir / "data");
    std::string err;
    if (!eng.open(&err)) {
      state.SkipWithError(err.c_str());
      break;
    }
    eng.close();
  }
  std::filesystem::remove_all(dir);
}
BENCHMARK(BM_StdbStorageOpenReplay);

static void BM_StdbStoragePressureSnapshot(benchmark::State& state) {
  const auto dir = std::filesystem::temp_directory_path() / "structdb_bm_pressure_snap";
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  structdb::storage::StorageEngine eng(dir / "data");
  std::string err;
  if (!eng.open(&err)) {
    state.SkipWithError(err.c_str());
    return;
  }
  for (int i = 0; i < 64; ++i) {
    if (!eng.put("mdb$ps" + std::to_string(i), "x", false)) {
      state.SkipWithError("put seed");
      eng.close();
      return;
    }
  }
  structdb::storage::StoragePressureSnapshot p{};
  for (auto _ : state) {
    eng.read_storage_pressure_snapshot(&p);
    benchmark::DoNotOptimize(p.l0_files);
    benchmark::DoNotOptimize(p.compaction_merge_success_total);
    benchmark::DoNotOptimize(p.compaction_worker_tasks_completed_total);
  }
  eng.close();
  std::filesystem::remove_all(dir);
}
BENCHMARK(BM_StdbStoragePressureSnapshot);

static void BM_StdbStorageEmbedBatchManyKeys(benchmark::State& state) {
  const int n = static_cast<int>(state.range(0));
  const auto dir = std::filesystem::temp_directory_path() / "structdb_bm_embed_batch";
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  structdb::storage::StorageEngine eng(dir / "data");
  std::string err;
  if (!eng.open(&err)) {
    state.SkipWithError(err.c_str());
    return;
  }
  eng.set_import_batch_skip_undo(true);
  eng.set_import_store_raw_logical(true);
  eng.set_memtable_bulk_put_enabled(true);
  std::vector<std::pair<std::string, std::string>> puts;
  puts.reserve(static_cast<std::size_t>(n));
  for (int i = 0; i < n; ++i) {
    puts.emplace_back("mdb$v2$row$bm$" + std::to_string(i), std::string(32, 'v'));
  }
  const std::vector<std::string> dels;
  for (auto _ : state) {
    if (!eng.commit_embed_batch(dels, puts, false, &err)) {
      state.SkipWithError(err.c_str());
      break;
    }
  }
  eng.close();
  std::filesystem::remove_all(dir);
}
BENCHMARK(BM_StdbStorageEmbedBatchManyKeys)->Arg(32768);
