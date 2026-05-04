#include <newdb/wal/wal_segment_scanner.h>

#include <algorithm>
#include <filesystem>
#include <system_error>

namespace newdb {

std::vector<std::string> list_wal_segment_paths(const std::string& wal_dir) {
    std::vector<std::filesystem::path> paths;
    if (wal_dir.empty()) {
        return {};
    }
    namespace fs = std::filesystem;
    std::error_code ec;
    const fs::path root(wal_dir);
    if (!fs::is_directory(root, ec)) {
        return {};
    }
    for (const fs::directory_entry& ent : fs::directory_iterator(root, ec)) {
        if (ec) {
            break;
        }
        if (!ent.is_regular_file(ec)) {
            continue;
        }
        const fs::path& p = ent.path();
        const std::string ps = p.string();
        if (ps.size() > 4 && ps.compare(ps.size() - 4, 4, ".wal") == 0) {
            paths.push_back(p);
        }
    }
    std::sort(paths.begin(), paths.end());
    std::vector<std::string> out;
    out.reserve(paths.size());
    for (const auto& p : paths) {
        out.push_back(p.string());
    }
    return out;
}

std::vector<WalSegmentFileInfo> list_wal_segment_inventory(const std::string& wal_dir) {
    std::vector<WalSegmentFileInfo> out;
    if (wal_dir.empty()) {
        return out;
    }
    namespace fs = std::filesystem;
    std::error_code ec;
    const fs::path root(wal_dir);
    if (!fs::is_directory(root, ec)) {
        return out;
    }
    for (const fs::directory_entry& ent : fs::directory_iterator(root, ec)) {
        if (ec) {
            break;
        }
        if (!ent.is_regular_file(ec)) {
            continue;
        }
        const fs::path p = ent.path();
        const std::string ps = p.string();
        if (ps.size() <= 4 || ps.compare(ps.size() - 4, 4, ".wal") != 0) {
            continue;
        }
        WalSegmentFileInfo row{};
        row.path = ps;
        row.size_bytes = static_cast<std::uint64_t>(fs::file_size(p, ec));
        if (ec) {
            row.size_bytes = 0;
        }
        out.push_back(std::move(row));
    }
    std::sort(out.begin(), out.end(), [](const WalSegmentFileInfo& a, const WalSegmentFileInfo& b) {
        return a.path < b.path;
    });
    return out;
}

}  // namespace newdb
