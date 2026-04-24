#pragma once

#include <string>

// WHERE/COUNT 条件运算符
enum class CondOp {
    Eq,
    Ne,
    Gt,
    Lt,
    Ge,
    Le,
    Contains,
    Unknown
};

CondOp parse_cond_op(const std::string& op);
