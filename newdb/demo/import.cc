#include "import.h"

#include <newdb/schema_io.h>

#include "logging.h"
#include <algorithm>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

bool import_tables_from_directory(const char* dir_path, const char* dest_workspace, const char* log_file) {
    namespace fs = std::filesystem;
    if (dir_path == nullptr || std::strlen(dir_path) == 0) {
        log_and_print(log_file, "[IMPORTDIR] empty path.\n");
        return false;
    }
    std::error_code ec;
    fs::path src_dir = fs::path(dir_path);
    if (!fs::exists(src_dir, ec) || !fs::is_directory(src_dir, ec)) {
        log_and_print(log_file, "[IMPORTDIR] not a directory: %s\n", dir_path);
        return false;
    }

    fs::path dst_dir;
    if (dest_workspace != nullptr && dest_workspace[0] != '\0') {
        dst_dir = fs::absolute(dest_workspace, ec);
    } else {
        dst_dir = fs::current_path(ec);
    }
    if (ec) {
        log_and_print(log_file, "[IMPORTDIR] cannot resolve destination directory.\n");
        return false;
    }
    fs::create_directories(dst_dir, ec);

    std::vector<fs::path> data_files;
    for (fs::directory_iterator it(src_dir, ec), end; !ec && it != end; it.increment(ec)) {
        const fs::directory_entry& ent = *it;
        if (!ent.is_regular_file(ec)) continue;
        fs::path p = ent.path();
        if (p.extension() != ".bin") continue;

        std::string filename = p.filename().string();
        std::string stem = p.stem().string();
        bool looks_like_log = (log_file != nullptr && filename == log_file);
        if (!looks_like_log) {
            const std::string suf = "_log";
            if (stem.size() >= suf.size() &&
                stem.compare(stem.size() - suf.size(), suf.size(), suf) == 0) {
                looks_like_log = true;
            }
        }
        if (looks_like_log) continue;
        data_files.push_back(p);
    }

    if (data_files.empty()) {
        log_and_print(log_file, "[IMPORTDIR] no table files (*.bin) found in %s\n", src_dir.string().c_str());
        return true;
    }

    std::sort(data_files.begin(), data_files.end());
    log_and_print(log_file, "[IMPORTDIR] importing from %s -> %s\n",
                  src_dir.string().c_str(), dst_dir.string().c_str());

    std::size_t ok_cnt = 0;
    std::size_t skip_cnt = 0;
    std::size_t fail_cnt = 0;

    for (const auto& src_data : data_files) {
        fs::path dst_data = dst_dir / src_data.filename();
        fs::path src_attr = src_dir / newdb::schema_sidecar_path_for_data_file(src_data.filename().string());
        fs::path dst_attr = dst_dir / src_attr.filename();

        bool data_ready = false;
        bool data_skipped = false;
        bool attr_ready = false;
        bool attr_skipped = false;

        {
            std::error_code ec_exist;
            if (fs::exists(dst_data, ec_exist)) {
                ++skip_cnt;
                data_ready = true;
                data_skipped = true;
                log_and_print(log_file, "  [SKIP] %s already exists\n", dst_data.filename().string().c_str());
            } else {
                std::error_code ec_copy;
                if (!fs::copy_file(src_data, dst_data, fs::copy_options::none, ec_copy)) {
                    ++fail_cnt;
                    log_and_print(log_file, "  [FAIL] copy %s -> %s (%s)\n",
                                  src_data.string().c_str(),
                                  dst_data.string().c_str(),
                                  ec_copy.message().c_str());
                    continue;
                }
                data_ready = true;
            }
        }

        {
            std::error_code ec_attr_exist;
            if (fs::exists(src_attr, ec_attr_exist) && !ec_attr_exist) {
                std::error_code ec_dst_attr_exist;
                if (fs::exists(dst_attr, ec_dst_attr_exist)) {
                    attr_ready = true;
                    attr_skipped = true;
                    log_and_print(log_file, "  [SKIP] %s already exists\n", dst_attr.filename().string().c_str());
                } else {
                    std::error_code ec_copy_attr;
                    if (!fs::copy_file(src_attr, dst_attr, fs::copy_options::none, ec_copy_attr)) {
                        log_and_print(log_file, "  [WARN] copy attr %s -> %s (%s)\n",
                                      src_attr.string().c_str(),
                                      dst_attr.string().c_str(),
                                      ec_copy_attr.message().c_str());
                    } else {
                        attr_ready = true;
                    }
                }
            }
        }

        ++ok_cnt;
        log_and_print(log_file,
                      "  [OK] table %s ready%s%s\n",
                      src_data.stem().string().c_str(),
                      (data_ready ? (data_skipped ? " (data exists)" : " (data copied)") : ""),
                      (attr_ready ? (attr_skipped ? " +attr exists" : " +attr copied") : ""));
    }

    log_and_print(log_file, "[IMPORTDIR] ok=%zu skip=%zu fail=%zu\n", ok_cnt, skip_cnt, fail_cnt);
    return fail_cnt == 0;
}
