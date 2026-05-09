#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace newdb::c_api_detail {

struct TailReadResult {
    std::string data;
    bool ok{true};
};

TailReadResult read_file_tail(const std::string& path, std::uintmax_t start_pos);

bool output_indicates_business_error(const std::string& out);

std::string trim_copy(const std::string& in);

bool starts_with_ci(const std::string& text, const std::string& prefix_upper);

std::string normalize_paren_txn_command(const std::string& raw);

/// Uses the same code labels as [`newdb_error_code_string`](c_api.h) (duplicated here so `newdb_core` does not export the C API TU).
void prepend_capi_error_line(std::string& out, int code);

} // namespace newdb::c_api_detail
