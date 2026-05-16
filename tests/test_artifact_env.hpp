#pragma once

#include <filesystem>

namespace structdb::testing {

// 单次测试进程根目录：<CMake 构建根>/test_runs/<时间戳>/（由 CMake 生成 test_artifact_config.hpp）。
// 所有落盘测试应写在该目录之下，避免多次运行或并行 ctest 交叉污染。
const std::filesystem::path& test_artifact_run_root();

}  // namespace structdb::testing
