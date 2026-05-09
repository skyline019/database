#pragma once

#include <filesystem>
#include <string>

/// Path-resolution slice of ShellState without txn/WHERE/LSM headers (thin include surface).
/// Use when a TU only needs workspace paths (option C / thin headers).
struct ShellStatePathsView {
    const std::string& data_dir;
    const std::string& session_data_path;
};

inline std::string resolve_table_file_paths(const ShellStatePathsView& v, const std::string& rel_or_abs) {
    if (rel_or_abs.empty()) {
        return {};
    }
    namespace fs = std::filesystem;
    std::error_code ec;
    fs::path p(rel_or_abs);
    if (p.is_absolute()) {
        const fs::path c = fs::weakly_canonical(p, ec);
        return ec ? p.string() : c.string();
    }
    if (!v.data_dir.empty()) {
        const fs::path base = fs::absolute(v.data_dir, ec);
        return (base / p).lexically_normal().string();
    }
    const fs::path c = fs::absolute(p, ec);
    return ec ? (fs::current_path(ec) / p).lexically_normal().string() : c.string();
}

inline std::string effective_data_path_paths(const ShellStatePathsView& v) {
    return resolve_table_file_paths(v, v.session_data_path);
}

inline std::filesystem::path workspace_directory_paths(const ShellStatePathsView& v) {
    namespace fs = std::filesystem;
    std::error_code ec;
    if (!v.data_dir.empty()) {
        return fs::absolute(v.data_dir, ec);
    }
    return fs::current_path(ec);
}
