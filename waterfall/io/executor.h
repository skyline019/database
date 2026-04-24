////
// @file executor.h
// @brief
// 执行器
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#pragma once

#include <assert.h>
#include <waterfall/utils/call_stack.h>

namespace wf::io {
class async_task;
class task_queue;

// 运行时执行器接口
struct runtime_executor
{
    virtual ~runtime_executor() = default;
    virtual void execute(async_task &task) = 0;
    virtual void execute(task_queue &queue) = 0;
};

// 获取当前执行器
inline runtime_executor *current_executor()
{
    runtime_executor *executor = utils::call_stack<runtime_executor>::top();
    assert(executor); // worker线程环境必须有执行器
    return executor;
}

} // namespace wf::io