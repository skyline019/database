#include "cli/shell/c_api/show_storage_log.h"

#include "cli/modules/common/logging/logging.h"
#include "cli/modules/sidecar/common/sidecar_wal_lsn.h"
#include "cli/shell/state/shell_state_facade.h"

#include <cstdint>
#include <filesystem>
#include <system_error>

namespace newdb::capi_cli {

void emit_show_storage_log_lines(ShellStateFacade& f, const char* log_file) {
    namespace fs = std::filesystem;
    std::error_code ec;
    const fs::path ws = f.workspace_directory();
    const fs::path wal = ws / "demodb.wal";
    std::uint64_t wal_bytes{0};
    if (fs::exists(wal, ec)) {
        const auto sz = fs::file_size(wal, ec);
        if (!ec) {
            wal_bytes = static_cast<std::uint64_t>(sz);
        }
    }
    const std::uint64_t lsn = read_wal_lsn_for_workspace(ws.string());
    std::uint64_t bin_bytes{0};
    std::uint32_t bin_files{0};
    if (fs::is_directory(ws, ec)) {
        for (const auto& ent : fs::directory_iterator(ws, ec)) {
            if (ec) break;
            if (!ent.is_regular_file(ec)) continue;
            if (ent.path().extension() == ".bin") {
                const auto fsz = fs::file_size(ent.path(), ec);
                if (!ec) {
                    bin_bytes += static_cast<std::uint64_t>(fsz);
                }
                ++bin_files;
            }
        }
    }
    log_and_print(log_file,
                  "[STORAGE] workspace=%s demodb.wal bytes=%llu demodb.wal_lsn=%llu total *.bin files=%u bytes=%llu\n",
                  ws.string().c_str(),
                  static_cast<unsigned long long>(wal_bytes),
                  static_cast<unsigned long long>(lsn),
                  static_cast<unsigned int>(bin_files),
                  static_cast<unsigned long long>(bin_bytes));
}

} // namespace newdb::capi_cli
