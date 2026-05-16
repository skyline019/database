////
// @file scn.cc
// @brief
// 系统逻辑时钟实现
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <atomic>
#include "scn.h"

namespace wf::schema {

static std::atomic<scn_t> kScnCounter = 0;

scn_t generate_scn()
{
    // TODO: 持久化系统逻辑时钟
    // relaxed
    return kScnCounter.fetch_add(1, std::memory_order_relaxed);
}

} // namespace wf::schema
