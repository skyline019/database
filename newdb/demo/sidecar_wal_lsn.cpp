#include "sidecar_wal_lsn.h"

#include <filesystem>
#include <fstream>
#include <string>

namespace fs = std::filesystem;

std::string workspace_dir_for_data_file(const std::string& data_file) {
    if (data_file.empty()) {
        return {};
    }
    const fs::path p = fs::path(data_file).parent_path();
    if (p.empty() || p == p.root_path()) {
        return data_file; // e.g. "t.bin" in cwd: treat as workspace "."
    }
    return p.lexically_normal().string();
}

std::uint64_t read_wal_lsn_for_workspace(const std::string& workspace_dir) {
    if (workspace_dir.empty()) {
        return 0;
    }
    const fs::path f = fs::path(workspace_dir) / "demodb.wal_lsn";
    std::ifstream in(f.string().c_str());
    if (!in) {
        return 0;
    }
    std::uint64_t v = 0;
    in >> v;
    return v;
}

void write_wal_lsn_for_workspace(const std::string& workspace_dir, std::uint64_t lsn) {
    if (workspace_dir.empty()) {
        return;
    }
    std::error_code ec;
    fs::create_directories(fs::path(workspace_dir), ec);
    (void)ec;
    const fs::path f = fs::path(workspace_dir) / "demodb.wal_lsn";
    std::ofstream out(f.string().c_str(), std::ios::out | std::ios::trunc);
    if (!out) {
        return;
    }
    out << lsn << "\n";
}
