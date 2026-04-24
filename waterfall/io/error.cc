////
// @file error.cc
// @brief
// 定义模块错误码
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include "error.h"

namespace wf::io {

error_info kErrorThreadIsRunning = "thread is already running";
error_info kErrorThreadAttributeInitFailed = "thread attribute init failed";
error_info kErrorThreadAffinitySetFailed = "thread affinity set failed";
error_info kErrorThreadCreateFailed = "thread create failed";

} // namespace wf::io