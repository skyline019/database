#include <waterfall/config.h>

#include "cli/modules/txn/coordinator/txn_manager.h"
#include "cli/modules/txn/coordinator/internal/txn_internal.h"
#include "cli/modules/logging/logging.h"
#include "cli/modules/util/constants.h"
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
#include <sys/stat.h>

Result<bool> TxnCoordinator::acquireLock(const std::string& file_path) {
    std::lock_guard<std::mutex> lk(m_lock_mutex);
    if (m_lock_handles.find(file_path) != m_lock_handles.end()) {
        return Result<bool>::Ok(true);
    }

    const std::string lock_file = file_path + ".lock";
    LockHandleState state{};
    state.lock_file_path = lock_file;
#if defined(_WIN32)
    HANDLE h = ::CreateFileA(lock_file.c_str(),
                             GENERIC_READ | GENERIC_WRITE,
                             FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                             nullptr,
                             OPEN_ALWAYS,
                             FILE_ATTRIBUTE_NORMAL,
                             nullptr);
    if (h == INVALID_HANDLE_VALUE) {
        return Result<bool>::Err("无法打开锁文�? " + lock_file);
    }

    OVERLAPPED ov{};
    if (!::LockFileEx(h, LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY, 0, MAXDWORD, MAXDWORD, &ov)) {
        const DWORD err = ::GetLastError();
        ::CloseHandle(h);
        return Result<bool>::Err("锁被占用或获取失�?" + std::to_string(static_cast<unsigned long long>(err)) + "): " + lock_file);
    }
    state.handle = h;
#else
    const int fd = ::open(lock_file.c_str(), O_CREAT | O_RDWR, 0644);
    if (fd < 0) {
        return Result<bool>::Err("无法打开锁文�? " + lock_file);
    }
    struct flock fl{};
    fl.l_type = F_WRLCK;
    fl.l_whence = SEEK_SET;
    fl.l_start = 0;
    fl.l_len = 0;
    if (::fcntl(fd, F_SETLK, &fl) != 0) {
        ::close(fd);
        return Result<bool>::Err("锁被占用或获取失�? " + lock_file);
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
    const std::string lock_file = file_path + ".lock";
    struct stat buffer;
    return (stat(lock_file.c_str(), &buffer) == 0);
}

// ========== WAL ==========



