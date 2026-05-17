#include "structdb/client/detail/mdb_runner_internal.hpp"

namespace structdb::client::mdb {

MdbDurabilityFsyncFlags mdb_durability_level_to_fsync(int level) {
  MdbDurabilityFsyncFlags f;
  switch (level) {
    case 0:
      f.fsync_each_batch = true;
      f.fsync_each_session_txn_op = true;
      break;
    case 1:
      f.fsync_each_batch = true;
      f.fsync_each_session_txn_op = false;
      break;
    case 2:
    default:
      f.fsync_each_batch = false;
      f.fsync_each_session_txn_op = false;
      break;
  }
  return f;
}

MdbDurabilityFsyncFlags mdb_effective_durability_fsync(std::optional<int> session_level, bool default_batch,
                                                        bool default_session_txn_op) {
  if (session_level.has_value()) return mdb_durability_level_to_fsync(*session_level);
  MdbDurabilityFsyncFlags f;
  f.fsync_each_batch = default_batch;
  f.fsync_each_session_txn_op = default_session_txn_op;
  return f;
}

}  // namespace structdb::client::mdb
