#include "condition.h"
#include <cctype>
#include <cstring>

CondOp parse_cond_op(const std::string& op) {
    if (op == "=" || op == "==") return CondOp::Eq;
    if (op == "!=" || op == "<>") return CondOp::Ne;
    if (op == ">") return CondOp::Gt;
    if (op == "<") return CondOp::Lt;
    if (op == ">=") return CondOp::Ge;
    if (op == "<=") return CondOp::Le;
    {
        std::string s = op;
        for (auto& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        if (s == "contains") return CondOp::Contains;
    }
    return CondOp::Unknown;
}
