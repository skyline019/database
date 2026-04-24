#include <newdb/catalog.h>

#include <algorithm>
#include <ctime>
#include <filesystem>
#include <fstream>

namespace fs = std::filesystem;

namespace newdb {

Catalog::Catalog(std::string root_path)
    : root_(std::move(root_path)) {
    if (root_.empty()) {
        std::error_code ec;
        root_ = fs::current_path(ec).string();
        if (ec) root_ = ".";
    }
}

static bool is_db_directory(const fs::path& p) {
    std::error_code ec;
    if (!fs::is_directory(p, ec) || ec) return false;
    return fs::exists(p / ".dbinfo", ec) && !ec;
}

Status Catalog::create_database(const std::string& name) {
    if (name.empty()) return Status::Fail("empty database name");
    if (has_database(name)) return Status::Fail("database exists");
    std::error_code ec;
    const fs::path db_path = fs::path(root_) / name;
    fs::create_directory(db_path, ec);
    if (ec) return Status::Fail("create_directory: " + ec.message());
    std::ofstream info(db_path / ".dbinfo", std::ios::binary | std::ios::trunc);
    if (!info) return Status::Fail("cannot write .dbinfo");
    info << "name=" << name << '\n';
    info << "created=" << static_cast<long long>(std::time(nullptr)) << '\n';
    return Status::Ok();
}

Status Catalog::drop_database(const std::string& name) {
    if (name.empty()) return Status::Fail("empty database name");
    if (name == current_db_) return Status::Fail("cannot drop active database");
    const fs::path db_path = fs::path(root_) / name;
    std::error_code ec;
    if (!fs::exists(db_path, ec)) return Status::Fail("database missing");
    fs::remove_all(db_path, ec);
    if (ec) return Status::Fail("remove_all: " + ec.message());
    return Status::Ok();
}

Status Catalog::use_database(const std::string& name) {
    if (name.empty()) return Status::Fail("empty database name");
    if (!has_database(name)) return Status::Fail("database not found");
    current_db_ = name;
    return Status::Ok();
}

bool Catalog::has_database(const std::string& name) const {
    if (name.empty()) return false;
    return is_db_directory(fs::path(root_) / name);
}

std::vector<std::string> Catalog::list_databases() const {
    std::vector<std::string> out;
    std::error_code ec;
    for (const auto& entry : fs::directory_iterator(root_, ec)) {
        if (ec) break;
        if (!entry.is_directory(ec) || ec) continue;
        const std::string stem = entry.path().filename().string();
        if (stem.empty() || stem[0] == '.') continue;
        if (stem == "build" || stem == "backup" || stem == "docs") continue;
        if (is_db_directory(entry.path())) {
            out.push_back(stem);
        }
    }
    std::sort(out.begin(), out.end());
    return out;
}

std::string Catalog::current_database_path() const {
    if (current_db_.empty()) return root_;
    return (fs::path(root_) / current_db_).string();
}

} // namespace newdb
