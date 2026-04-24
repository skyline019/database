////
// @file thread_pool.cc
// @brief
// 简化版线程池服务实现
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <thread>
#include "thread_pool.h"

namespace wf::io {

thread_pool::~thread_pool() { shutdown(); }

void thread_pool::shutdown() { join_all(); }

void thread_pool::join_all()
{
    for (auto &worker : workers_)
        worker.join();
    workers_.clear(); // 清空线程向量
}

// 执行预执行函数
void thread_pool::execute_preloader()
{
    for (auto &func : preload_functions_) {
        func();
    }
}

// 执行后执行函数
void thread_pool::execute_sweeper()
{
    // 按照逆序执行，与注册顺序相反
    for (auto it = cleanup_functions_.rbegin(); it != cleanup_functions_.rend();
         ++it) {
        (*it)();
    }
}

} // namespace wf::io