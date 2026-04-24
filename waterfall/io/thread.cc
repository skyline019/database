////
// @file thread.cc
// @brief
// 实现线程
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <assert.h>
#include <unistd.h>
#include <signal.h>
#include "error.h"
#include "thread.h"

namespace wf::io {

int posix_thread::cpu_index_ = 0;

posix_thread::posix_thread(posix_thread &&other) noexcept
    : id_(other.id_)
{
    other.id_ = 0;
}

posix_thread &posix_thread::operator=(posix_thread &&other) noexcept
{
    if (this != &other) {
        // 如果当前对象有活动线程，先join以清理资源
        if (id_ != 0) { ::pthread_join(id_, nullptr); }
        id_ = other.id_;
        other.id_ = 0;
    }
    return *this;
}

posix_thread::~posix_thread() { join(); }

void posix_thread::join()
{
    if (id_ == 0) return;
    ::pthread_join(id_, nullptr);
    id_ = 0; // 重置id_
}

void posix_thread::detach()
{
    if (id_ == 0) return; // 线程不存在
    ::pthread_detach(id_);
    id_ = 0; // 重置id_
}

unsigned int posix_thread::hardware_concurrency() noexcept
{
    return ::sysconf(_SC_NPROCESSORS_ONLN);
}

int posix_thread::set_affinity(int index)
{
    if (id_ == 0) return -1; // 线程未创建
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(index, &cpuset);
    return ::pthread_setaffinity_np(id_, sizeof(cpu_set_t), &cpuset);
}

void *posix_thread::thread_entry(void *arg)
{
    auto wrapper = static_cast<thread_wrapper_base *>(arg);
    wrapper->run();
    delete wrapper; // 运行完成后清理内存
    return nullptr;
}

} // namespace wf::io