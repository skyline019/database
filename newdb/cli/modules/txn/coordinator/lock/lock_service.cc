#include <waterfall/config.h>

#include "cli/modules/txn/coordinator/txn_manager.h"
#include "cli/modules/txn/coordinator/internal/txn_internal.h"
#include "cli/modules/common/logging/logging.h"
#include "cli/modules/common/util/constants.h"
#include "cli/modules/sidecar/common/sidecar_wal_lsn.h"

#include <newdb/heap_table.h>
#include <newdb/page_io.h>
#include <newdb/schema_io.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#if defined(_WIN32)
#include <windows.h>
#include <process.h>
#ifdef DELETE
#undef DELETE
#endif
#else
#include <fcntl.h>
#include <unistd.h>
#endif

Result<bool> TxnCoordinator::acquireLock(const std::string& file_path) {
    std::lock_guard<std::mutex> lk(m_lock_mutex);
    if (m_lock_handles.find(file_path) != m_lock_handles.end()) {
        m_file_lock_same_process_reuse_count.fetch_add(1, std::memory_order_relaxed);
        return Result<bool>::Ok(true);
    }

    const std::string lock_file = file_path + ".lock";
    const bool strict_stale = []() {
        const char* raw = std::getenv("NEWDB_FILE_LOCK_STRICT");
        return raw != nullptr && raw[0] == '1' && raw[1] == '\0';
    }();

    LockHandleState state{};
    state.lock_file_path = lock_file;
    bool retried_stale = false;
#if defined(_WIN32)
    auto try_remove_stale_lock_marker = [&]() -> bool {
        if (!strict_stale || retried_stale) {
            return false;
        }
        namespace fs = std::filesystem;
        std::error_code ec;
        const fs::path p(lock_file);
        if (!fs::is_regular_file(p, ec) || ec) {
            return false;
        }
        if (fs::file_size(p, ec) != 0 || ec) {
            return false;
        }
        fs::remove(p, ec);
        if (!ec) {
            m_file_lock_stale_marker_count.fetch_add(1, std::memory_order_relaxed);
            retried_stale = true;
            return true;
        }
        return false;
    };
    HANDLE h = INVALID_HANDLE_VALUE;
    for (int attempt = 0; attempt < 2; ++attempt) {
        h = ::CreateFileA(lock_file.c_str(),
                          GENERIC_READ | GENERIC_WRITE,
                          FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                          nullptr,
                          OPEN_ALWAYS,
                          FILE_ATTRIBUTE_NORMAL,
                          nullptr);
        if (h == INVALID_HANDLE_VALUE) {
            m_file_lock_acquire_fail_count.fetch_add(1, std::memory_order_relaxed);
            return Result<bool>::Err("failed to open lock file: " + lock_file);
        }
        OVERLAPPED ov{};
        if (::LockFileEx(h, LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY, 0, MAXDWORD, MAXDWORD, &ov)) {
            break;
        }
        const DWORD err = ::GetLastError();
        ::CloseHandle(h);
        h = INVALID_HANDLE_VALUE;
        if (try_remove_stale_lock_marker()) {
            continue;
        }
        m_file_lock_acquire_fail_count.fetch_add(1, std::memory_order_relaxed);
        return Result<bool>::Err(
            "lock is already held or lock acquisition failed (" +
            std::to_string(static_cast<unsigned long long>(err)) + "): " +
            lock_file);
    }
    state.handle = h;
#else
    auto try_remove_stale_lock_marker = [&]() -> bool {
        if (!strict_stale || retried_stale) {
            return false;
        }
        namespace fs = std::filesystem;
        std::error_code ec;
        const fs::path p(lock_file);
        if (!fs::is_regular_file(p, ec) || ec) {
            return false;
        }
        if (fs::file_size(p, ec) != 0 || ec) {
            return false;
        }
        fs::remove(p, ec);
        if (!ec) {
            m_file_lock_stale_marker_count.fetch_add(1, std::memory_order_relaxed);
            retried_stale = true;
            return true;
        }
        return false;
    };
    int fd = -1;
    for (int attempt = 0; attempt < 2; ++attempt) {
        fd = ::open(lock_file.c_str(), O_CREAT | O_RDWR, 0644);
        if (fd < 0) {
            m_file_lock_acquire_fail_count.fetch_add(1, std::memory_order_relaxed);
            return Result<bool>::Err("failed to open lock file: " + lock_file);
        }
        struct flock fl{};
        fl.l_type = F_WRLCK;
        fl.l_whence = SEEK_SET;
        fl.l_start = 0;
        fl.l_len = 0;
        if (::fcntl(fd, F_SETLK, &fl) == 0) {
            break;
        }
        ::close(fd);
        fd = -1;
        if (try_remove_stale_lock_marker()) {
            continue;
        }
        m_file_lock_acquire_fail_count.fetch_add(1, std::memory_order_relaxed);
        return Result<bool>::Err("lock is already held or lock acquisition failed: " + lock_file);
    }
    state.fd = fd;
#endif

    m_lock_handles[file_path] = state;
    m_locked_files.push_back(file_path);
    return Result<bool>::Ok(true);
}


Result<bool> TxnCoordinator::releaseLock(const std::string& file_path) {
    std::lock_guard<std::mutex> lk(m_lock_mutex);
    const auto it_handle = m_lock_handles.find(file_path);
    if (it_handle != m_lock_handles.end()) {
#if defined(_WIN32)
        const HANDLE h = static_cast<HANDLE>(it_handle->second.handle);
        if (h != nullptr && h != INVALID_HANDLE_VALUE) {
            OVERLAPPED ov{};
            (void)::UnlockFileEx(h, 0, MAXDWORD, MAXDWORD, &ov);
            ::CloseHandle(h);
        }
#else
        if (it_handle->second.fd >= 0) {
            struct flock fl{};
            fl.l_type = F_UNLCK;
            fl.l_whence = SEEK_SET;
            fl.l_start = 0;
            fl.l_len = 0;
            (void)::fcntl(it_handle->second.fd, F_SETLK, &fl);
            ::close(it_handle->second.fd);
        }
#endif
        std::remove(it_handle->second.lock_file_path.c_str());
        m_lock_handles.erase(it_handle);
    } else {
        const std::string lock_file = file_path + ".lock";
        std::remove(lock_file.c_str());
    }

    // 从列表中移除
    for (auto it = m_locked_files.begin(); it != m_locked_files.end(); ) {
        if (*it == file_path) {
            it = m_locked_files.erase(it);
        } else {
            ++it;
        }
    }
    
    return Result<bool>::Ok(true);
}


bool TxnCoordinator::isLocked(const std::string& file_path) const {
    std::lock_guard<std::mutex> lk(m_lock_mutex);
    return m_lock_handles.find(file_path) != m_lock_handles.end();
}

// ========== WAL ==========



