#pragma once

#include <string>
#include <vector>

// 去掉首尾空白
std::string trim(const std::string& s);

// 从形如 CMD(arg1, arg2, ...) 中提取括号内的内容，并按逗号切分为参数列表；
// 双引号内的逗号不拆分，引号从参数首尾剥掉一层。命令名部分需由调用方已判断。
bool parse_comma_args(const char* begin_after_cmd, std::vector<std::string>& out_args);

// 当前日期 "YYYY-MM-DD"
std::string get_current_date_str();

// 当前日期时间 "YYYY-MM-DD HH:MM:SS"
std::string get_current_datetime_str();

// 简单 date 格式校验：YYYY-MM-DD
bool is_valid_date_str(const std::string& s);

// 简单 datetime 格式校验：YYYY-MM-DD HH:MM:SS
bool is_valid_datetime_str(const std::string& s);
