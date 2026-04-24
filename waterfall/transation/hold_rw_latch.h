////
// @file hold_rw_latch.h
// @brief
// 持有读写闩
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#pragma once

#include <mutex>
#include <shared_mutex>
#include <condition_variable>

namespace wf::transation {

// 持有读写闩
//
class hold_rw_latch
{
  private:
    std::mutex mutex_;           // 锁
    std::condition_variable cv_; // 条件变量
    bool is_reading_ = false;    // 是否正在读取
    bool is_writing_ = false;    // 是否正在写入
};

} // namespace wf::transation