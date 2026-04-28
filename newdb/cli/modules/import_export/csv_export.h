#pragma once

#include <string>

// RFC4180-style: quote when needed, double internal quotes.
inline std::string csv_escape_cell(const std::string& s) {
    bool need_quote = false;
    for (char c : s) {
        if (c == '"' || c == ',' || c == '\n' || c == '\r') {
            need_quote = true;
            break;
        }
    }
    if (!need_quote) {
        return s;
    }
    std::string out;
    out.push_back('"');
    for (char c : s) {
        if (c == '"') {
            out += "\"\"";
        } else {
            out += c;
        }
    }
    out.push_back('"');
    return out;
}
