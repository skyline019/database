#include "utils.h"
#include <cctype>
#include <ctime>
#include <cstring>

std::string trim(const std::string& s) {
    std::size_t b = 0, e = s.size();
    while (b < e && std::isspace(static_cast<unsigned char>(s[b]))) ++b;
    while (e > b && std::isspace(static_cast<unsigned char>(s[e - 1]))) --e;
    return s.substr(b, e - b);
}

bool parse_comma_args(const char* begin_after_cmd, std::vector<std::string>& out_args) {
    std::string rest = trim(begin_after_cmd);
    auto lp = rest.find('(');
    auto rp = rest.rfind(')');
    if (lp == std::string::npos || rp == std::string::npos || rp <= lp + 1) {
        return false;
    }
    std::string inside = rest.substr(lp + 1, rp - lp - 1);
    std::vector<std::string> args;
    std::string current;
    bool in_dquote = false;
    for (std::size_t i = 0; i < inside.size(); ++i) {
        char ch = inside[i];
        if (ch == '"') {
            in_dquote = !in_dquote;
            current.push_back(ch);
        } else if (ch == ',' && !in_dquote) {
            std::string s = trim(current);
            if (s.size() >= 2 && s.front() == '"' && s.back() == '"') {
                s = s.substr(1, s.size() - 2);
            }
            args.push_back(std::move(s));
            current.clear();
        } else {
            current.push_back(ch);
        }
    }
    {
        std::string s = trim(current);
        if (s.size() >= 2 && s.front() == '"' && s.back() == '"') {
            s = s.substr(1, s.size() - 2);
        }
        if (!s.empty() || !args.empty()) {
            args.push_back(std::move(s));
        }
    }
    out_args = std::move(args);
    return true;
}

std::string get_current_date_str() {
    std::time_t now = std::time(nullptr);
    std::tm tm_buf{};
#if defined(_WIN32)
    localtime_s(&tm_buf, &now);
#else
    localtime_r(&now, &tm_buf);
#endif
    char buf[32]{};
    if (std::strftime(buf, sizeof(buf), "%Y-%m-%d", &tm_buf) == 0) {
        return "1970-01-01";
    }
    return std::string(buf);
}

std::string get_current_datetime_str() {
    std::time_t now = std::time(nullptr);
    std::tm tm_buf{};
#if defined(_WIN32)
    localtime_s(&tm_buf, &now);
#else
    localtime_r(&now, &tm_buf);
#endif
    char buf[32]{};
    if (std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm_buf) == 0) {
        return "1970-01-01 00:00:00";
    }
    return std::string(buf);
}

bool is_valid_date_str(const std::string& s) {
    if (s.size() != 10) return false;
    for (int i : {0, 1, 2, 3, 5, 6, 8, 9}) {
        if (s[i] < '0' || s[i] > '9') return false;
    }
    return s[4] == '-' && s[7] == '-';
}

bool is_valid_datetime_str(const std::string& s) {
    if (s.size() != 19) return false;
    for (int i : {0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18}) {
        if (s[i] < '0' || s[i] > '9') return false;
    }
    return s[4] == '-' && s[7] == '-' && s[10] == ' ' &&
           s[13] == ':' && s[16] == ':';
}
