////
// @file service.h
// @brief
// 服务基类
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#pragma once

#include <functional>
#include <shared_mutex>
#include <unordered_map>
#include <waterfall/meta/type_index.h>
#include <waterfall/utils/noncopyable.h>

namespace wf::io {

// 服务基类，用户不能直接操作
class service_base : private utils::noncopyable
{
  private:
    service_base *next_; // 注册表后继
    const char *id_;     // 服务id

  protected:
    service_base();
    virtual ~service_base() = default;

  private:
    virtual void shutdown() = 0; // 服务关闭函数
    friend class service_registry;
};

// 服务类型检查
template <typename T>
concept is_service = std::is_base_of_v<service_base, T>;

// 服务注册表
class service_registry : private utils::noncopyable
{
  private:
    mutable std::shared_mutex mutex_;                              // 读写锁
    service_base *first_service_;                                  // 服务链表
    std::unordered_map<const char *, service_base *> service_map_; // 查询服务

  public:
    service_registry();
    ~service_registry();

    // 按初始化逆序关闭销毁
    void shutdown_services();
    void destroy_services();

    // 查找服务，没有则创建
    template <typename Service, typename... Args>
        requires is_service<Service>
    Service *use_service(Args &&...args);
    // 判断是否存在服务
    template <typename Service>
    Service *has_service() const;

  private:
    // 服务工厂，用于传递构造函数
    service_base *do_has_service(const char *id) const;
    service_base *
    do_use_service(const char *id, std::function<service_base *()> factory);
};

template <typename Service>
Service *service_registry::has_service() const
{
    return static_cast<Service *>(
        do_has_service(meta::type_id<Service>().name()));
}

template <typename Service, typename... Args>
    requires is_service<Service>
Service *service_registry::use_service(Args &&...args)
{
    std::function<service_base *()> factory = [&args...]() -> service_base * {
        return new Service(std::forward<Args>(args)...);
    };
    return static_cast<Service *>(
        do_use_service(meta::type_id<Service>().name(), factory));
}

} // namespace wf::io