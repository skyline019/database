#pragma once

#include <filesystem>
#include <random>
#include <string>

namespace newdb::test {

inline std::filesystem::path unique_temp_subdir(const std::string& prefix) {
    static std::mt19937_64 rng{std::random_device{}()};
    const std::string name = prefix + "_" + std::to_string(rng());
    return std::filesystem::temp_directory_path() / name;
}

class ScopedCwd {
public:
    explicit ScopedCwd(std::filesystem::path dir) : prev_(std::filesystem::current_path()) {
        std::filesystem::current_path(std::move(dir));
    }
    ScopedCwd(const ScopedCwd&) = delete;
    ScopedCwd& operator=(const ScopedCwd&) = delete;
    ~ScopedCwd() {
        std::error_code ec;
        std::filesystem::current_path(prev_, ec);
    }

private:
    std::filesystem::path prev_;
};

} // namespace newdb::test
