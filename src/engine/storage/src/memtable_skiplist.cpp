#include "structdb/storage/memtable_skiplist.hpp"

#include "structdb/storage/versioned_kv.hpp"

#include <cstdlib>
#include <new>
#include <string_view>
#include <utility>

namespace structdb::storage {

MemTableSkipList::Node::Node(std::string k, std::string v, int height)
    : key(std::move(k)), value(std::move(v)), fwd(static_cast<std::size_t>(height), nullptr) {}

MemTableSkipList::MemTableSkipList() { head_ = new Node("", "", kMaxHeight); }

MemTableSkipList::~MemTableSkipList() {
  destroy_all_nodes();
  delete head_;
  head_ = nullptr;
}

MemTableSkipList::Node* MemTableSkipList::alloc_node(std::string key, std::string value, int height) {
  void* mem = arena_.allocate(sizeof(Node), alignof(Node));
  return new (mem) Node(std::move(key), std::move(value), height);
}

void MemTableSkipList::destroy_all_nodes() {
  Node* p = head_->fwd[0];
  while (p) {
    Node* n = p->fwd[0];
    p->~Node();
    p = n;
  }
  arena_.reset();
  for (int i = 0; i < kMaxHeight; ++i) head_->fwd[static_cast<std::size_t>(i)] = nullptr;
  n_ = 0;
  bytes_ = 0;
}

int MemTableSkipList::random_height(const std::string& key) {
  std::uint64_t x = 1469598103934665603ULL;
  for (unsigned char c : key) x = (x ^ static_cast<std::uint64_t>(c)) * 1099511628211ULL;
  int h = 1;
  while (h < kMaxHeight && (x & 1u) == 0) {
    x >>= 1;
    ++h;
  }
  return h;
}

MemTableSkipList::Node* MemTableSkipList::find_predecessors(std::string_view key, Node** preds) const {
  Node* x = head_;
  for (int i = kMaxHeight - 1; i >= 0; --i) {
    while (x->fwd[static_cast<std::size_t>(i)] &&
           std::string_view(x->fwd[static_cast<std::size_t>(i)]->key) < key) {
      x = x->fwd[static_cast<std::size_t>(i)];
    }
    preds[i] = x;
  }
  return preds[0]->fwd[0];
}

MemTableSkipList::Node* MemTableSkipList::lower_bound_node_(std::string_view prefix) const {
  if (prefix.empty()) return head_->fwd[0];
  const std::string pfx(prefix);
  Node* x = head_;
  for (int i = kMaxHeight - 1; i >= 0; --i) {
    while (x->fwd[static_cast<std::size_t>(i)] && x->fwd[static_cast<std::size_t>(i)]->key < pfx) {
      x = x->fwd[static_cast<std::size_t>(i)];
    }
  }
  return x->fwd[0];
}

void MemTableSkipList::put(std::string key, std::string value) {
  Node* pred[kMaxHeight];
  (void)find_predecessors(key, pred);
  Node* cur = pred[0]->fwd[0];
  if (cur && cur->key == key) {
    bytes_ -= cur->key.size() + cur->value.size();
    cur->value = std::move(value);
    bytes_ += cur->key.size() + cur->value.size();
    return;
  }
  const int nh = random_height(key);
  Node* nn = alloc_node(std::move(key), std::move(value), nh);
  bytes_ += nn->key.size() + nn->value.size();
  for (int i = 0; i < nh; ++i) {
    nn->fwd[static_cast<std::size_t>(i)] = pred[i]->fwd[static_cast<std::size_t>(i)];
    pred[i]->fwd[static_cast<std::size_t>(i)] = nn;
  }
  ++n_;
}

bool MemTableSkipList::has_key(std::string_view key) const {
  Node* pred[kMaxHeight];
  Node* cur = find_predecessors(key, pred);
  return cur && cur->key == key;
}

bool MemTableSkipList::get_raw(std::string_view key, std::string* stored_out) const {
  Node* pred[kMaxHeight];
  Node* cur = find_predecessors(key, pred);
  if (!cur || cur->key != key) return false;
  if (stored_out) *stored_out = cur->value;
  return true;
}

bool MemTableSkipList::get(std::string_view key, std::string* value_out) const {
  Node* pred[kMaxHeight];
  Node* cur = find_predecessors(key, pred);
  if (!cur || cur->key != key) return false;
  if (cur->value == versioned_kv::kTomb) return false;
  if (value_out) *value_out = cur->value;
  return true;
}

std::size_t MemTableSkipList::bytes_approx() const { return bytes_; }

std::size_t MemTableSkipList::size() const { return n_; }

void MemTableSkipList::clear() { destroy_all_nodes(); }

void MemTableSkipList::reserve_capacity(std::size_t) {}

void MemTableSkipList::swap_with(IMemTable& other) noexcept {
  auto* o = dynamic_cast<MemTableSkipList*>(&other);
  if (!o) std::abort();
  std::swap(head_, o->head_);
  std::swap(n_, o->n_);
  std::swap(bytes_, o->bytes_);
}

void MemTableSkipList::merge_missing_from(const IMemTable& donor) {
  (void)donor.for_each_sorted([this](const std::string& k, const std::string& v) {
    if (!has_key(k)) put(k, v);
    return true;
  });
}

bool MemTableSkipList::seal_dump(const std::function<bool(const void*, std::size_t)>& chunk_writer) const {
  for (Node* p = head_->fwd[0]; p; p = p->fwd[0]) {
    const std::uint32_t klen = static_cast<std::uint32_t>(p->key.size());
    const std::uint32_t vlen = static_cast<std::uint32_t>(p->value.size());
    if (!chunk_writer(&klen, sizeof(klen))) return false;
    if (!chunk_writer(p->key.data(), p->key.size())) return false;
    if (!chunk_writer(&vlen, sizeof(vlen))) return false;
    if (!chunk_writer(p->value.data(), p->value.size())) return false;
  }
  return true;
}

bool MemTableSkipList::for_each_sorted(const std::function<bool(const std::string&, const std::string&)>& visitor) const {
  for (Node* p = head_->fwd[0]; p; p = p->fwd[0]) {
    if (!visitor(p->key, p->value)) return false;
  }
  return true;
}

bool MemTableSkipList::for_each_sorted_prefix(std::string_view prefix,
                                              const std::function<bool(const std::string&, const std::string&)>& visitor) const {
  if (prefix.empty()) return for_each_sorted(visitor);
  const std::string pfx(prefix);
  const auto in_prefix = [&pfx](const std::string& k) {
    return k.size() >= pfx.size() && k.compare(0, pfx.size(), pfx) == 0;
  };
  for (Node* p = lower_bound_node_(prefix); p && in_prefix(p->key); p = p->fwd[0]) {
    if (!visitor(p->key, p->value)) return false;
  }
  return true;
}

bool MemTableSkipList::for_each_sorted_prefix_overlay(std::string_view prefix, const IMemTable& older,
                                                      const std::function<bool(const std::string&, const std::string&)>& visitor) const {
  const auto* o = dynamic_cast<const MemTableSkipList*>(&older);
  if (!o) return false;
  const std::string pfx = prefix.empty() ? std::string{} : std::string{prefix};
  const auto in_prefix = [&pfx](const std::string& k) {
    if (pfx.empty()) return true;
    return k.size() >= pfx.size() && k.compare(0, pfx.size(), pfx) == 0;
  };
  Node* it_a = prefix.empty() ? head_->fwd[0] : lower_bound_node_(prefix);
  Node* it_b = prefix.empty() ? o->head_->fwd[0] : o->lower_bound_node_(prefix);
  while (it_a && in_prefix(it_a->key) && it_b && in_prefix(it_b->key)) {
    if (it_a->key < it_b->key) {
      if (!visitor(it_a->key, it_a->value)) return false;
      it_a = it_a->fwd[0];
    } else if (it_b->key < it_a->key) {
      if (!visitor(it_b->key, it_b->value)) return false;
      it_b = it_b->fwd[0];
    } else {
      if (!visitor(it_a->key, it_a->value)) return false;
      it_a = it_a->fwd[0];
      it_b = it_b->fwd[0];
    }
  }
  while (it_a && in_prefix(it_a->key)) {
    if (!visitor(it_a->key, it_a->value)) return false;
    it_a = it_a->fwd[0];
  }
  while (it_b && in_prefix(it_b->key)) {
    if (!visitor(it_b->key, it_b->value)) return false;
    it_b = it_b->fwd[0];
  }
  return true;
}

}  // namespace structdb::storage
