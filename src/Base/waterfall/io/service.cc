////
// @file service.cc
// @brief
// 服务注册表的实现
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <mutex>
#include "service.h"

namespace wf::io {

service_base::service_base()
    : next_(nullptr)
{}

service_registry::service_registry()
    : first_service_(nullptr)
{}

service_registry::~service_registry()
{
    shutdown_services();
    destroy_services();
}

void service_registry::shutdown_services()
{
    service_base *service = first_service_;
    while (service) {
        service->shutdown();
        service = service->next_;
    }
}

void service_registry::destroy_services()
{
    while (first_service_) {
        service_base *next_service = first_service_->next_;
        service_map_.erase(first_service_->id_);
        delete first_service_;
        first_service_ = next_service;
    }
}

service_base *service_registry::do_has_service(const char *id) const
{
    std::shared_lock<std::shared_mutex> lock(mutex_); // 改为共享锁
    if (auto it = service_map_.find(id); it != service_map_.end())
        return it->second;
    else
        return nullptr;
}

service_base *service_registry::do_use_service(
    const char *id,
    std::function<service_base *()> factory)
{
    // 先尝试共享锁查找
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        if (auto it = service_map_.find(id); it != service_map_.end())
            return it->second;
    }

    // 创建服务
    service_base *new_service = {factory()};
    new_service->id_ = id;

    // 获取独占锁插入
    std::unique_lock<std::shared_mutex> lock(mutex_);
    // 再次检查，防止竞争
    if (auto it = service_map_.find(id); it != service_map_.end()) {
        delete new_service;
        return it->second;
    }
    // 插入链表头部
    new_service->next_ = first_service_;
    first_service_ = new_service;
    // 插入哈希表
    service_map_[id] = new_service;

    return new_service;
}

} // namespace wf::io