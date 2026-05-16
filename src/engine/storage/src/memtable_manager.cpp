#include "structdb/storage/memtable_manager.hpp"

#include "structdb/storage/memtable.hpp"
#include "structdb/storage/memtable_skiplist.hpp"

namespace structdb::storage {

std::unique_ptr<IMemTable> make_memtable(MemTableBackend b) {
  switch (b) {
    case MemTableBackend::Map:
      return std::make_unique<MemTable>();
    case MemTableBackend::SkipList:
      return std::make_unique<MemTableSkipList>();
  }
  return std::make_unique<MemTable>();
}

MemTableManager::MemTableManager(MemTableBackend b) : backend_(b), active_(make_memtable(b)) {}

void MemTableManager::reset_to_backend(MemTableBackend b) {
  backend_ = b;
  active_ = make_memtable(b);
  frozen_flush_.reset();
}

bool MemTableManager::begin_flush_move_active_to_frozen(std::string* error_out) {
  if (frozen_flush_) {
    if (error_out) *error_out = "flush_memtable already in progress";
    return false;
  }
  frozen_flush_ = std::shared_ptr<IMemTable>(make_memtable(backend_).release());
  active_->swap_with(*frozen_flush_);
  return true;
}

void MemTableManager::merge_frozen_into_active_and_clear() {
  if (!frozen_flush_) return;
  active_->merge_missing_from(*frozen_flush_);
  frozen_flush_.reset();
}

void MemTableManager::clear_frozen_flush_only() { frozen_flush_.reset(); }

void MemTableManager::discard_frozen_snapshot() { frozen_flush_.reset(); }

}  // namespace structdb::storage
