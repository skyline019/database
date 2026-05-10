#include <waterfall/config.h>

#include <cstddef>

#include "cli/shell/dispatch/router/dispatch_routing.h"

#include "cli/shell/dispatch/shared/dispatch_internal.h"

namespace {

const char* skip_ws(const char* p) {
    while (*p == ' ' || *p == '\t') {
        ++p;
    }
    return p;
}

bool command_has_prefix_token(const char* s, const char* pfx, std::size_t len) {
    if (strncasecmp_ascii(s, pfx, len) != 0) {
        return false;
    }
    if (len > 0 && pfx[len - 1] == '(') {
        return true;
    }
    const char c = s[len];
    return c == '\0' || c == ' ' || c == '\t' || c == '(';
}

} // namespace

bool shell_line_targets_phase2_only(const char* line) {
    if (line == nullptr) {
        return false;
    }
    const char* s = skip_ws(line);
    if (*s == '\0') {
        return false;
    }
    // Prefixes aligned with handlers in query/dml/io/ddl (phase-2 chain order is irrelevant here).
    static const struct {
        const char* pfx;
        std::size_t len;
    } kPhase2Prefixes[] = {
        {"PAGE", 4},
        {"WHEREP", 6},
        {"WHERE", 5},
        {"COUNT", 5},
        {"MIN", 3},
        {"MAX", 3},
        {"SUM", 3},
        {"AVG", 3},
        {"FIND(", 5},
        {"FINDPK", 6},
        {"QBAL", 4},
        {"UPDATEWHERE", 11},
        {"UPDATE", 6},
        {"DELETEWHERE", 11},
        {"DELETE(", 7},
        {"DELETEPK", 8},
        {"EXPORT", 6},
        {"SETATTRMULTI", 12},
        {"SETATTR", 7},
        {"RENATTR", 7},
        {"DELATTR", 7},
        {"SET PRIMARY KEY", 15},
    };
    for (const auto& e : kPhase2Prefixes) {
        if (command_has_prefix_token(s, e.pfx, e.len)) {
            return true;
        }
    }
    return false;
}
