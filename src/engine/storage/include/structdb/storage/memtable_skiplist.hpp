#pragma once

#include <cstddef>
#include <functional>
#include <string>
#include <string_view>
#include <vector>

#include "structdb/storage/imemtable.hpp"

namespace structdb::storage {

/// Skip-list backed `IMemTable` (write-optimized vs `std::map` for large tables; same sorted semantics).
class MemTableSkipList final : public IMemTable {
 public:
  MemTableSkipList();
  ~MemTableSkipList() override;

  MemTableSkipList(const MemTableSkipList&) = delete;
  MemTableSkipList& operator=(const MemTableSkipList&) = delete;

  void put(std::string key, std::string value) override;
  bool has_key(std::string_view key) const override;
  bool get_raw(std::string_view key, std::string* stored_out) const override;
  bool get(std::string_view key, std::string* value_out) const override;
  std::size_t bytes_approx() const override;
  std::size_t size() const override;
  void clear() override;
  void reserve_capacity(std::size_t approx_entries) override;

  void swap_with(IMemTable& other) noexcept override;
  void merge_missing_from(const IMemTable& donor) override;

  bool seal_dump(const std::function<bool(const void*, std::size_t)>& chunk_writer) const override;
  bool for_each_sorted(const std::function<bool(const std::string&, const std::string&)>& visitor) const override;
  bool for_each_sorted_prefix(std::string_view prefix,
                              const std::function<bool(const std::string&, const std::string&)>& visitor) const override;
  bool for_each_sorted_prefix_overlay(std::string_view prefix, const IMemTable& older,
                                      const std::function<bool(const std::string&, const std::string&)>& visitor) const override;

 private:
  struct Node {
    std::string key;
    std::string value;
    std::vector<Node*> fwd;
    Node(std::string k, std::string v, int height);
  };

  static constexpr int kMaxHeight = 20;
  static int random_height(const std::string& key);
  Node* find_predecessors(std::string_view key, Node** preds) const;
  Node* lower_bound_node_(std::string_view prefix) const;
  void destroy_all_nodes();

  Node* head_{nullptr};
  std::size_t n_{0};
  std::size_t bytes_{0};
};

}  // namespace structdb::storage
