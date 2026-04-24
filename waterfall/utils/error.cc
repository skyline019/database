////
// @file error.cc
// @brief
// utils模块错误信息
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include "error.h"

namespace wf::utils {

error_info kErrorUleb128Malformed = "malformed uleb128, extends past end";
error_info kErrorUleb128Toolong = "uleb128 too big for uint64";
error_info kErrorSleb128Malformed = "malformed sleb128, extends past end";
error_info kErrorSleb128Toolong = "sleb128 too big for int64";

} // namespace wf::utils
