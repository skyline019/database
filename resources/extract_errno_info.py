#!/usr/bin/env python3
"""
enhanced_errno_extractor.py
增强版errno.h提取器，适配新的error_info结构
生成分离的头文件和源文件
"""

import re
import subprocess
import sys
import os
from pathlib import Path
import tempfile

def find_errno_dependencies(header_path):
    """查找errno.h依赖的头文件"""
    dependencies = set()

    try:
        # 使用gcc的依赖生成功能
        with tempfile.NamedTemporaryFile(mode='w', suffix='.c', delete=False) as f:
            f.write("#include <errno.h>\n")
            temp_file = f.name

        # 生成依赖关系
        result = subprocess.run(
            ["gcc", "-M", "-I/usr/include", temp_file],
            capture_output=True,
            text=True,
            timeout=10
        )

        os.unlink(temp_file)

        if result.returncode == 0:
            # 解析依赖输出
            deps = result.stdout.strip().split()
            for dep in deps:
                if dep.endswith('.h') and 'errno' in dep.lower():
                    dependencies.add(dep)

    except Exception as e:
        print(f"查找依赖失败: {e}")

    return list(dependencies)

def extract_all_errno_definitions(header_paths):
    """从多个头文件中提取错误码定义"""
    all_error_defs = {}

    for header_path in header_paths:
        print(f"处理头文件: {header_path}")

        if not Path(header_path).exists():
            print(f"  警告: 文件不存在 {header_path}")
            continue

        error_defs = extract_errno_from_file(header_path)
        all_error_defs.update(error_defs)

    return all_error_defs

def extract_errno_from_file(file_path):
    """从单个文件中提取错误码定义"""
    error_defs = {}

    try:
        # 使用预处理器处理单个文件
        preprocessed = subprocess.run(
            ["gcc", "-E", "-dD", "-I/usr/include", file_path],
            capture_output=True,
            text=True,
            timeout=10
        )

        if preprocessed.returncode != 0:
            print(f"  警告: 预处理 {file_path} 失败，尝试直接解析")
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
        else:
            content = preprocessed.stdout

        # 匹配错误码定义
        patterns = [
            r'#define\s+(E[A-Z0-9_]+)\s+(\d+\b)',  # #define ENAME 123
            r'#define\s+(E[A-Z0-9_]+)\s+\((\d+)\)',  # #define ENAME (123)
            r'#define\s+(E[A-Z0-9_]+)\s+\(-\s*(\d+)\)',  # #define ENAME (-123)
            r'#define\s+(E[A-Z0-9_]+)\s+-\s*(\d+)',  # #define ENAME -123
        ]

        for pattern in patterns:
            matches = re.findall(pattern, content)
            for name, value in matches:
                try:
                    errno_value = int(value)
                    if errno_value > 0:
                        # 检查是否已存在，避免重复
                        if errno_value not in error_defs or len(name) > len(error_defs[errno_value]):
                            error_defs[errno_value] = name
                except ValueError:
                    continue

        print(f"  从 {file_path} 找到 {len([k for k in error_defs.keys()])} 个错误码")

    except Exception as e:
        print(f"  处理文件 {file_path} 时出错: {e}")

    return error_defs

def get_comprehensive_errno_list():
    """获取全面的错误码列表"""
    # 首先查找主要的errno.h
    main_errno_paths = [
        "/usr/include/errno.h",
        "/usr/include/linux/errno.h",
        "/usr/include/asm-generic/errno.h",
        "/usr/include/asm-generic/errno-base.h",
        "/usr/include/bits/errno.h"
    ]

    # 查找实际存在的文件
    existing_paths = [p for p in main_errno_paths if Path(p).exists()]

    if not existing_paths:
        print("错误: 找不到任何errno.h文件")
        return {}

    # 查找依赖的头文件
    all_header_paths = set(existing_paths)
    for path in existing_paths:
        deps = find_errno_dependencies(path)
        all_header_paths.update(deps)

    print(f"找到 {len(all_header_paths)} 个相关头文件")

    # 从所有相关头文件中提取错误码
    return extract_all_errno_definitions(list(all_header_paths))

def get_error_messages_robust(error_defs):
    """更健壮地获取错误描述"""
    error_messages = {}

    # 创建一个C程序来获取所有错误描述
    c_program = """
#include <stdio.h>
#include <string.h>
#include <errno.h>

int main() {
"""

    # 为每个错误码添加输出
    for errno_value, name in sorted(error_defs.items()):
        c_program += f'    printf("{name}|%d|%s\\n", {name}, strerror({name}));\n'

    c_program += """
    return 0;
}
"""

    try:
        # 写入临时文件
        with tempfile.NamedTemporaryFile(mode='w', suffix='.c', delete=False) as f:
            f.write(c_program)
            c_file = f.name

        # 编译
        compile_result = subprocess.run(
            ["gcc", "-o", "/tmp/get_all_errors", c_file],
            capture_output=True,
            text=True,
            timeout=15
        )

        if compile_result.returncode != 0:
            print(f"编译失败: {compile_result.stderr}")
            # 尝试使用更简单的方法
            return get_error_messages_fallback(error_defs)

        # 运行
        run_result = subprocess.run(
            ["/tmp/get_all_errors"],
            capture_output=True,
            text=True,
            timeout=10
        )

        if run_result.returncode == 0:
            for line in run_result.stdout.strip().split('\n'):
                if '|' in line:
                    parts = line.split('|')
                    if len(parts) >= 3:
                        name = parts[0]
                        message = parts[2]
                        # 找到对应的错误码
                        for errno_val, err_name in error_defs.items():
                            if err_name == name:
                                error_messages[errno_val] = message
                                break

        # 清理
        os.unlink(c_file)
        if Path("/tmp/get_all_errors").exists():
            os.unlink("/tmp/get_all_errors")

    except Exception as e:
        print(f"获取错误描述时出错: {e}")
        error_messages = get_error_messages_fallback(error_defs)

    return error_messages

def get_error_messages_fallback(error_defs):
    """备用的错误描述获取方法"""
    error_messages = {}

    # 使用Python的ctypes调用strerror
    try:
        import ctypes
        libc = ctypes.CDLL("libc.so.6")
        strerror = libc.strerror
        strerror.restype = ctypes.c_char_p
        strerror.argtypes = [ctypes.c_int]

        for errno_value, name in error_defs.items():
            msg = strerror(errno_value)
            if msg:
                error_messages[errno_value] = msg.decode('utf-8', errors='ignore')
            else:
                error_messages[errno_value] = "Unknown system error"

    except Exception as e:
        print(f"使用ctypes失败: {e}")
        # 最后的手段：使用硬编码的常见错误描述
        common_messages = {
            1: "Operation not permitted",
            2: "No such file or directory",
            5: "Input/output error",
            13: "Permission denied",
            17: "File exists",
            22: "Invalid argument",
            24: "Too many open files",
            28: "No space left on device",
            116: "Stale file handle"
        }
        for errno_value, name in error_defs.items():
            error_messages[errno_value] = common_messages.get(errno_value, "Unknown system error")

    return error_messages

def generate_separated_cpp_files(error_defs, error_messages):
    """生成分离的头文件和源文件"""

    # 按错误码值排序
    sorted_errors = sorted(error_defs.items())

    # 生成头文件
    header_code = """// 自动生成的系统错误码定义头文件
// extract_errno_info.py从系统errno.h及相关头文件提取

#pragma once

#include "error.h"

namespace wf::utils {

// 系统错误静态对象命名空间
namespace system_errors {
extern error_info system_UNKNOWN;
"""

    # 生成静态错误对象声明
    for errno_value, name in sorted_errors:
        header_code += f'extern error_info system_{name};\n'

    header_code += """}

// 根据错误码值获取对应的错误信息对象
error_info system_error(int errno_value);

} // namespace wf::utils
"""

    # 生成源文件
    source_code = """// 自动生成的系统错误码定义源文件
// extract_errno_info.py从系统errno.h及相关头文件提取

#include "system_errors.h"

namespace wf::utils {

// 系统错误静态对象定义
namespace system_errors {
error_info system_UNKNOWN = "Unknown system error";
"""

    # 生成静态错误对象定义
    for errno_value, name in sorted_errors:
        message = error_messages.get(errno_value, "Unknown system error")
        # 转义字符串中的特殊字符
        escaped_message = message.replace('"', '\\"').replace('\n', '\\n')
        source_code += f'error_info system_{name} = "{escaped_message}";\n'

    source_code += """}

// 根据错误码值获取对应的错误信息对象
error_info system_error(int errno_value) {
    switch (errno_value) {
      case 0: return nullptr;
"""

    # 生成查找函数
    for errno_value, name in sorted_errors:
        source_code += f"      case {errno_value}: return system_errors::system_{name};\n"

    source_code += """      default: return system_errors::system_UNKNOWN;
    }
}

} // namespace wf::utils
"""

    return header_code, source_code

def main():
    print("系统错误码提取器 - 分离头文件和源文件版本")
    print("=" * 60)

    print("1. 查找errno.h及相关头文件...")
    error_defs = get_comprehensive_errno_list()

    if not error_defs:
        print("错误: 无法提取任何错误码定义")
        sys.exit(1)

    print(f"2. 找到 {len(error_defs)} 个唯一的错误码")

    print("3. 获取错误描述信息...")
    error_messages = get_error_messages_robust(error_defs)

    print(f"4. 获取到 {len(error_messages)} 个错误描述")

    print("5. 生成分离的头文件和源文件...")
    header_code, source_code = generate_separated_cpp_files(error_defs, error_messages)

    # 输出头文件
    header_file = "system_errors.h"
    with open(header_file, "w") as f:
        f.write(header_code)
    print(f"   已生成头文件: {header_file}")

    # 输出源文件
    source_file = "system_errors.cc"
    with open(source_file, "w") as f:
        f.write(source_code)
    print(f"   已生成源文件: {source_file}")

    # 显示统计信息
    print(f"\n统计信息:")
    print(f"  总错误码: {len(error_defs)}")
    print(f"  有描述的错误码: {len(error_messages)}")

    if error_defs:
        print(f"\n错误码范围: {min(error_defs.keys())} - {max(error_defs.keys())}")
        print(f"\n示例错误码 (前10个):")
        for i, (errno_value, name) in enumerate(sorted(error_defs.items())[:10]):
            message = error_messages.get(errno_value, "No description")
            print(f"  {name} ({errno_value}): {message}")

if __name__ == "__main__":
    main()