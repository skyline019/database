#pragma once

#include "structdb/storage/storage_pressure.hpp"

namespace structdb::storage {

class StorageEngine;

/// Fills `StoragePressureSnapshot` under `mu_` shared lock (same contract as `StorageEngine::read_storage_pressure_snapshot`).
class StorageTelemetry {
 public:
  explicit StorageTelemetry(StorageEngine& engine) noexcept : engine_(engine) {}

  void read_storage_pressure_snapshot(StoragePressureSnapshot* out) const;

 private:
  StorageEngine& engine_;
};

}  // namespace structdb::storage
