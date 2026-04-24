////
// @file error.cc
// @brief
// 定义存储模块错误信息
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include "error.h"

namespace wf::storage {

// clang-format off
error_info kErrorNullPayload = "null payload";
error_info kErrorPayloadLengthNotEnough = "payload length not enough";
error_info kErrorTupleSizeIsZero = "tuple size is zero";
error_info kErrorFirstFieldIsEmpty = "first field is empty";
error_info kErrorInvalidIndex = "invalid index";
error_info kErrorUnalignedRecord = "unaligned record pointer";
error_info kErrorNotOverflow = "not overflow record";
error_info kErrorNotVersioned = "not versioned record";
error_info kErrorNullBitmap = "segment bitmap is null";
error_info kErrorDeserializationFailed = "segment bitmap deserialization failed";
// clang-format on

} // namespace wf::storage
