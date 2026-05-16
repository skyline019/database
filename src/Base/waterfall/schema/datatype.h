////
// @file datatype.h
// @brief
// 数据类型
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#pragma once

namespace wf::schema {

// 数据类型
enum class datatype_t
{
    INT,
    FLOAT,
    CHAR,
    VARCHAR,
    DATE,
    TIME,
    DATETIME,
    TIMESTAMP,
};

} // namespace wf::schema