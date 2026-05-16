////
// @file error.h
// @brief
// 存储模块错误信息
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#pragma once

#include <waterfall/utils/system_errors.h>

namespace wf::storage {

using error_info = wf::utils::error_info;

extern error_info kErrorNullPayload;
extern error_info kErrorPayloadLengthNotEnough;
extern error_info kErrorTupleSizeIsZero;
extern error_info kErrorFirstFieldIsEmpty;
extern error_info kErrorInvalidIndex;
extern error_info kErrorUnalignedRecord;
extern error_info kErrorNotOverflow;
extern error_info kErrorNotVersioned;
extern error_info kErrorNullBitmap;
extern error_info kErrorDeserializationFailed;

} // namespace wf::storage
