#pragma once

#include <memory>
#include <mutex>
#include <typeindex>
#include <unordered_map>

namespace structdb::facade {

class ServiceContainer {
 public:
  template <typename T>
  void register_singleton(std::shared_ptr<T> instance) {
    std::lock_guard<std::mutex> lock(mu_);
    services_[std::type_index(typeid(T))] = std::move(instance);
  }

  template <typename T>
  std::shared_ptr<T> resolve() {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = services_.find(std::type_index(typeid(T)));
    if (it == services_.end()) return nullptr;
    return std::static_pointer_cast<T>(it->second);
  }

  void clear() {
    std::lock_guard<std::mutex> lock(mu_);
    services_.clear();
  }

 private:
  std::mutex mu_;
  std::unordered_map<std::type_index, std::shared_ptr<void>> services_;
};

}  // namespace structdb::facade
