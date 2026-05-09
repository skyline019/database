#include <newdb/c_api_helpers.h>

#include <cctype>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>

namespace newdb::c_api_detail {

namespace {

// Keep in sync with [`newdb_error_code_string`](c_api.cpp) / [`c_api.h`](c_api.h) `newdb_error_code`.
const char* capi_error_code_label(int code) {
    switch (code) {
        case 0:
            return "ok";
        case 1:
            return "invalid_argument";
        case 2:
            return "invalid_handle";
        case 3:
            return "execution_failed";
        case 4:
            return "internal";
        case 5:
            return "log_io";
        case 6:
            return "session_terminated";
        default:
            return "unknown";
    }
}

} // namespace

TailReadResult read_file_tail(const std::string& path, std::uintmax_t start_pos) {
    TailReadResult out{};
    std::error_code ec;
    const auto file_size = std::filesystem::file_size(path, ec);
    if (ec || file_size <= start_pos) {
        out.ok = !ec;
        return out;
    }
    std::ifstream in(path, std::ios::binary);
    if (!in.good()) {
        out.ok = false;
        return out;
    }
    in.seekg(static_cast<std::streamoff>(start_pos), std::ios::beg);
    out.data.assign(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
    return out;
}

bool output_indicates_business_error(const std::string& out) {
    if (out.find("[ERROR]") != std::string::npos) {
        return true;
    }
    if (out.find("expects ") != std::string::npos && out.find(", got '") != std::string::npos) {
        return true;
    }
    if (out.find(" failed") != std::string::npos || out.find(" invalid") != std::string::npos) {
        return true;
    }
    if (out.find("duplicate ") != std::string::npos || out.find(" missing ") != std::string::npos) {
        return true;
    }
    if (out.find("usage:") != std::string::npos) {
        return true;
    }
    return false;
}

std::string trim_copy(const std::string& in) {
    size_t begin = 0;
    while (begin < in.size() && std::isspace(static_cast<unsigned char>(in[begin])) != 0) {
        ++begin;
    }
    size_t end = in.size();
    while (end > begin && std::isspace(static_cast<unsigned char>(in[end - 1])) != 0) {
        --end;
    }
    return in.substr(begin, end - begin);
}

bool starts_with_ci(const std::string& text, const std::string& prefix_upper) {
    if (text.size() < prefix_upper.size()) {
        return false;
    }
    for (size_t i = 0; i < prefix_upper.size(); ++i) {
        const char c = static_cast<char>(std::toupper(static_cast<unsigned char>(text[i])));
        if (c != prefix_upper[i]) {
            return false;
        }
    }
    return true;
}

std::string normalize_paren_txn_command(const std::string& raw) {
    const std::string trimmed = trim_copy(raw);
    if (trimmed.empty()) {
        return raw;
    }

    auto normalize_after_prefix = [&](const char* canonical_prefix) -> std::string {
        const std::string prefix = canonical_prefix;
        std::string rest = trim_copy(trimmed.substr(prefix.size()));
        if (rest.empty() || rest.front() != '(' || rest.back() != ')') {
            return raw;
        }
        rest = trim_copy(rest.substr(1, rest.size() - 2));
        if (rest.empty()) {
            return raw;
        }
        return prefix + " " + rest;
    };

    if (starts_with_ci(trimmed, "SAVEPOINT")) {
        return normalize_after_prefix("SAVEPOINT");
    }
    if (starts_with_ci(trimmed, "ROLLBACK TO SAVEPOINT")) {
        return normalize_after_prefix("ROLLBACK TO SAVEPOINT");
    }
    if (starts_with_ci(trimmed, "ROLLBACK TO")) {
        return normalize_after_prefix("ROLLBACK TO");
    }
    if (starts_with_ci(trimmed, "RELEASE SAVEPOINT")) {
        return normalize_after_prefix("RELEASE SAVEPOINT");
    }
    return raw;
}

void prepend_capi_error_line(std::string& out, int code) {
    if (code == 0) {
        return;
    }
    const std::string prefix = std::string("[CAPI_ERROR] code=") + capi_error_code_label(code) +
                               " numeric=" + std::to_string(code) + "\n";
    out.insert(0, prefix);
}

} // namespace newdb::c_api_detail
