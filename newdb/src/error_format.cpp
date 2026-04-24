#include <newdb/error_format.h>

namespace newdb {

namespace {

std::string escape_error_message(const std::string& message) {
    std::string out;
    out.reserve(message.size() + 8);
    for (const char ch : message) {
        if (ch == '\\' || ch == '"') {
            out.push_back('\\');
            out.push_back(ch);
        } else if (ch == '\n') {
            out += "\\n";
        } else if (ch == '\r') {
            out += "\\r";
        } else if (ch == '\t') {
            out += "\\t";
        } else {
            out.push_back(ch);
        }
    }
    return out;
}

} // namespace

std::string format_error_line(const std::string& domain,
                              const std::string& code,
                              const std::string& message) {
    return "[ERR] domain=" + domain + " code=" + code + " message=\"" + escape_error_message(message) + "\"";
}

} // namespace newdb
