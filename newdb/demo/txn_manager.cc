#include "txn_manager.h"
#include "logging.h"
#include "constants.h"
#include "sidecar_wal_lsn.h"

#include <newdb/heap_table.h>
#include <newdb/page_io.h>
#include <newdb/schema_io.h>

#include <map>
#include <unordered_map>
#include <unordered_set>
#include <filesystem>
#include <cstdio>
#include <cstring>
#include <cctype>
#include <ctime>
#include <chrono>
#include <sstream>
#include <cstdlib>
#include <fstream>
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

TxnCoordinator::TxnCoordinator() {
    // 构造函数
}

TxnCoordinator::~TxnCoordinator() {
    stopVacuumThread();
    std::vector<std::string> locked_copy;
    {
        std::lock_guard<std::mutex> lk(m_lock_mutex);
        locked_copy = m_locked_files;
    }
    for (const auto& f : locked_copy) {
        (void)releaseLock(f);
    }
}

// ========== 事务控制 ==========

Result<bool> TxnCoordinator::begin(const std::string& table_name) {
    std::lock_guard<std::mutex> lk(m_txn_mutex);
    
    if (m_state.load() == TxnState::Active) {
        return Result<bool>::Err("已有活跃事务");
    }
    
    // 生成新事务ID (基于时间戳)
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    m_txn_id.store(ms);
    
    m_state.store(TxnState::Active);
    m_txn_records.clear();
    
    m_active_table = table_name;
    const std::string bin_file = resolveDataFilePath(table_name);
    auto lockResult = acquireLock(bin_file);
    if (lockResult.isErr()) {
        m_state.store(TxnState::None);
        return lockResult;
    }
    writeWAL("TXN_BEGIN", table_name, "", "", "");
    flushWAL();
    
    return Result<bool>::Ok(true);
}

Result<bool> TxnCoordinator::commit() {
    {
        std::lock_guard<std::mutex> lk(m_txn_mutex);
        
        if (m_state.load() != TxnState::Active) {
            return Result<bool>::Err("没有活跃事务");
        }
        
        writeWAL("TXN_COMMIT", m_active_table, "", "", "");
        flushWAL();
        
        std::vector<std::string> locked_copy;
        {
            std::lock_guard<std::mutex> lk2(m_lock_mutex);
            locked_copy = m_locked_files;
        }
        for (const auto& f : locked_copy) {
            (void)releaseLock(f);
        }
        
        m_state.store(TxnState::Committed);
        m_txn_records.clear();
        m_locked_files.clear();
        m_active_table.clear();
    }
    maybeCompactWalAfterCommit();
    return Result<bool>::Ok(true);
}

Result<bool> TxnCoordinator::rollback() {
    std::lock_guard<std::mutex> lk(m_txn_mutex);
    
    if (m_state.load() != TxnState::Active) {
        return Result<bool>::Err("没有活跃事务");
    }
    
    auto parse_attrs = [](const std::string& packed) {
        std::map<std::string, std::string> attrs;
        std::size_t i = 0;
        while (i < packed.size()) {
            const std::size_t sep = packed.find('=', i);
            if (sep == std::string::npos) {
                break;
            }
            const std::size_t end = packed.find(';', sep + 1);
            const std::string key = packed.substr(i, sep - i);
            const std::string val =
                (end == std::string::npos) ? packed.substr(sep + 1) : packed.substr(sep + 1, end - (sep + 1));
            if (!key.empty()) {
                attrs[key] = val;
            }
            if (end == std::string::npos) {
                break;
            }
            i = end + 1;
        }
        return attrs;
    };

    auto append_undo_record = [&](const TxnRecord& rec) {
        const std::string data_file = resolveDataFilePath(rec.table_name);
        try {
            const int id = std::stoi(rec.key);
            if (rec.operation == "INSERT") {
                newdb::Row tomb;
                tomb.id = id;
                tomb.attrs["__deleted"] = "1";
                (void)newdb::io::append_row(data_file.c_str(), tomb);
                return;
            }
            newdb::Row row;
            row.id = id;
            const auto attrs = parse_attrs(rec.old_value);
            for (const auto& kv : attrs) {
                if (kv.first != "__deleted") {
                    row.attrs[kv.first] = kv.second;
                }
            }
            (void)newdb::io::append_row(data_file.c_str(), row);
        } catch (...) {
            // ignore malformed undo records
        }
    };

    // 实际回滚：按逆序追加补偿记录
    for (auto it = m_txn_records.rbegin(); it != m_txn_records.rend(); ++it) {
        append_undo_record(*it);
    }
    
    writeWAL("TXN_ROLLBACK", m_active_table, "", "", "");
    flushWAL();
    
    std::vector<std::string> locked_copy;
    {
        std::lock_guard<std::mutex> lk2(m_lock_mutex);
        locked_copy = m_locked_files;
    }
    for (const auto& f : locked_copy) {
        (void)releaseLock(f);
    }
    
    m_state.store(TxnState::RolledBack);
    m_txn_records.clear();
    m_locked_files.clear();
    m_active_table.clear();
    
    return Result<bool>::Ok(true);
}

// ========== 文件锁 ==========

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
        return Result<bool>::Err("无法打开锁文件: " + lock_file);
    }

    OVERLAPPED ov{};
    if (!::LockFileEx(h, LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY, 0, MAXDWORD, MAXDWORD, &ov)) {
        const DWORD err = ::GetLastError();
        ::CloseHandle(h);
        return Result<bool>::Err("锁被占用或获取失败(" + std::to_string(static_cast<unsigned long long>(err)) + "): " + lock_file);
    }
    state.handle = h;
#else
    const int fd = ::open(lock_file.c_str(), O_CREAT | O_RDWR, 0644);
    if (fd < 0) {
        return Result<bool>::Err("无法打开锁文件: " + lock_file);
    }
    struct flock fl{};
    fl.l_type = F_WRLCK;
    fl.l_whence = SEEK_SET;
    fl.l_start = 0;
    fl.l_len = 0;
    if (::fcntl(fd, F_SETLK, &fl) != 0) {
        ::close(fd);
        return Result<bool>::Err("锁被占用或获取失败: " + lock_file);
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

std::string TxnCoordinator::getWALPath(const std::string& table_name) const {
    namespace fs = std::filesystem;
    const fs::path base = m_workspace_root.empty() ? fs::current_path() : fs::path(m_workspace_root);
    if (table_name.empty()) {
        return (base / (std::string("demodb") + Constants::WAL_FILE_EXT)).lexically_normal().string();
    }
    return (base / (table_name + Constants::WAL_FILE_EXT)).lexically_normal().string();
}

std::string TxnCoordinator::walSyncConfigPath() const {
    namespace fs = std::filesystem;
    const fs::path base = m_workspace_root.empty() ? fs::current_path() : fs::path(m_workspace_root);
    return (base / "demodb.walsync.conf").lexically_normal().string();
}

std::string TxnCoordinator::vacuumConfigPath() const {
    namespace fs = std::filesystem;
    const fs::path base = m_workspace_root.empty() ? fs::current_path() : fs::path(m_workspace_root);
    return (base / "demodb.vacuum.conf").lexically_normal().string();
}

void TxnCoordinator::loadVacuumConfig() {
    std::ifstream in(vacuumConfigPath());
    if (!in) {
        return;
    }
    std::size_t threshold = 0;
    in >> threshold;
    if (threshold > 0) {
        m_vacuum_ops_threshold.store(threshold);
    }
}

void TxnCoordinator::persistVacuumConfig() const {
    std::ofstream out(vacuumConfigPath(), std::ios::out | std::ios::trunc);
    if (!out) {
        return;
    }
    out << m_vacuum_ops_threshold.load() << "\n";
}

bool TxnCoordinator::loadWalSyncConfig(newdb::WalManager& wm) const {
    std::ifstream in(walSyncConfigPath());
    if (!in) {
        return false;
    }
    std::string mode;
    std::uint64_t interval = wm.normal_sync_interval_ms();
    in >> mode >> interval;
    for (auto& ch : mode) {
        ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    }
    if (mode == "off") {
        wm.set_sync_mode(newdb::WalSyncMode::Off);
    } else if (mode == "normal") {
        wm.set_sync_mode(newdb::WalSyncMode::Normal);
    } else if (mode == "full") {
        wm.set_sync_mode(newdb::WalSyncMode::Full);
    }
    wm.set_normal_sync_interval_ms(interval);
    return true;
}

void TxnCoordinator::persistWalSyncConfig(const newdb::WalManager& wm) const {
    std::ofstream out(walSyncConfigPath(), std::ios::out | std::ios::trunc);
    if (!out) {
        return;
    }
    const char* mode = (wm.sync_mode() == newdb::WalSyncMode::Off) ? "off" :
                       (wm.sync_mode() == newdb::WalSyncMode::Normal) ? "normal" : "full";
    out << mode << " " << wm.normal_sync_interval_ms() << "\n";
}

std::string TxnCoordinator::resolveDataFilePath(const std::string& table_name) const {
    namespace fs = std::filesystem;
    std::error_code ec;
    fs::path p = table_name + Constants::DATA_FILE_EXT;
    if (!m_workspace_root.empty()) {
        p = fs::absolute(m_workspace_root, ec) / p;
    } else {
        p = fs::absolute(p, ec);
    }
    return p.lexically_normal().string();
}

newdb::WalManager* TxnCoordinator::ensureWal() {
    if (!wal_) {
        const std::string ws = m_workspace_root.empty() ? "." : m_workspace_root;
        wal_ = std::make_unique<newdb::WalManager>("demodb", ws);
        const bool loaded = loadWalSyncConfig(*wal_);
        if (!loaded) {
            // Default startup profile for balanced throughput/durability.
            wal_->set_sync_mode(newdb::WalSyncMode::Normal);
            wal_->set_normal_sync_interval_ms(20);
            persistWalSyncConfig(*wal_);
        }
        if (const char* mode = std::getenv("NEWDB_WAL_SYNC")) {
            std::string m = mode;
            for (auto& ch : m) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
            if (m == "off") wal_->set_sync_mode(newdb::WalSyncMode::Off);
            else if (m == "normal") wal_->set_sync_mode(newdb::WalSyncMode::Normal);
            else wal_->set_sync_mode(newdb::WalSyncMode::Full);
        }
        (void)wal_->open();
    }
    return wal_.get();
}

void TxnCoordinator::writeWAL(const std::string& operation, const std::string& table,
                          const std::string& key, const std::string& old_val, const std::string& new_val) {
    std::lock_guard<std::mutex> lk(m_wal_mutex);
    newdb::WalManager* wm = ensureWal();
    if (!wm) {
        return;
    }
    const uint64_t txn = static_cast<uint64_t>(m_txn_id.load());

    if (operation == "TXN_BEGIN") {
        (void)wm->begin_transaction(txn);
        return;
    }
    if (operation == "TXN_COMMIT") {
        (void)wm->commit_transaction(txn);
        return;
    }
    if (operation == "TXN_ROLLBACK") {
        (void)wm->rollback_transaction(txn);
        return;
    }
    if (operation == "RECOVERY_DONE") {
        (void)wm->append_record(txn, newdb::WalOp::SESSION_SNAPSHOT, table, nullptr);
        return;
    }

    newdb::WalOp op = newdb::WalOp::UPDATE;
    if (operation == "INSERT") {
        op = newdb::WalOp::INSERT;
    } else if (operation == "DELETE") {
        op = newdb::WalOp::DELETE;
    }

    newdb::Row row;
    try {
        row.id = std::stoi(key);
    } catch (...) {
        row.id = 0;
    }
    row.attrs["__wal_old"] = old_val;
    row.attrs["__wal_new"] = new_val;
    row.attrs["__wal_op"] = operation;
    (void)wm->append_record(txn, op, table, &row);
}

void TxnCoordinator::recordOperation(const std::string& operation, const std::string& table,
                                   const std::string& key, const std::string& old_val, const std::string& new_val) {
    if (m_state.load() != TxnState::Active) {
        return;  // 非活跃事务不记录
    }
    
    TxnRecord rec;
    rec.txn_id = m_txn_id.load();
    rec.state = m_state.load();
    rec.table_name = table;
    rec.operation = operation;
    rec.key = key;
    rec.old_value = old_val;
    rec.new_value = new_val;
    rec.timestamp = std::time(nullptr);
    
    m_txn_records.push_back(rec);
    
    // 同时写入 WAL
    writeWAL(operation, table, key, old_val, new_val);

    if (m_vacuum_running.load()) {
        const std::size_t threshold = m_vacuum_ops_threshold.load();
        const std::size_t count = m_vacuum_op_counter.fetch_add(1) + 1;
        if (threshold > 0 && count >= threshold) {
            m_vacuum_op_counter.store(0);
            triggerVacuum(table.empty() ? m_active_table : table);
        }
    }
}

void TxnCoordinator::persistWalsnHighWaterUnlocked(newdb::WalManager* wm) {
    if (wm == nullptr) {
        return;
    }
    const std::string ws = m_workspace_root.empty() ? std::string(".") : m_workspace_root;
    write_wal_lsn_for_workspace(ws, wm->current_lsn());
}

void TxnCoordinator::maybeCompactWalAfterCommit() {
    std::lock_guard<std::mutex> lk(m_wal_mutex);
    newdb::WalManager* w2 = ensureWal();
    if (w2 == nullptr) {
        return;
    }
    std::uint64_t cap = wal_compact_max_bytes;
    if (const char* env = std::getenv("NEWDB_WAL_COMPACT_BYTES")) {
        const std::uint64_t v = static_cast<std::uint64_t>(std::strtoull(env, nullptr, 10));
        if (v > 0) {
            cap = v;
        }
    }
    if (w2->wal_file_size_bytes() < cap) {
        return;
    }
    (void)w2->checkpoint_and_truncate(w2->current_lsn());
    persistWalsnHighWaterUnlocked(w2);
}

void TxnCoordinator::flushWAL() {
    std::lock_guard<std::mutex> lk(m_wal_mutex);
    newdb::WalManager* wm = ensureWal();
    if (!wm) {
        return;
    }
    (void)wm->flush();
    persistWalsnHighWaterUnlocked(wm);
}

void TxnCoordinator::setWalSyncMode(newdb::WalSyncMode mode) {
    std::lock_guard<std::mutex> lk(m_wal_mutex);
    newdb::WalManager* wm = ensureWal();
    if (wm) {
        wm->set_sync_mode(mode);
        persistWalSyncConfig(*wm);
    }
}

newdb::WalSyncMode TxnCoordinator::walSyncMode() {
    std::lock_guard<std::mutex> lk(m_wal_mutex);
    if (!wal_) {
        return newdb::WalSyncMode::Full;
    }
    return wal_->sync_mode();
}

void TxnCoordinator::setWalNormalSyncIntervalMs(const std::uint64_t ms) {
    std::lock_guard<std::mutex> lk(m_wal_mutex);
    newdb::WalManager* wm = ensureWal();
    if (wm) {
        wm->set_normal_sync_interval_ms(ms);
        persistWalSyncConfig(*wm);
    }
}

std::uint64_t TxnCoordinator::walNormalSyncIntervalMs() {
    std::lock_guard<std::mutex> lk(m_wal_mutex);
    if (!wal_) {
        return 50;
    }
    return wal_->normal_sync_interval_ms();
}

bool TxnCoordinator::recoverFromWAL() {
    newdb::WalManager* wm = ensureWal();
    if (!wm) {
        return true;
    }
    newdb::TableSchema schema;
    std::vector<newdb::WalDecodedRecord> recs;
    const newdb::Status rd = wm->read_all_records(schema, recs);
    if (!rd.ok) {
        return false;
    }
    std::unordered_map<uint64_t, std::vector<TxnRecord>> txn_records;
    std::unordered_set<uint64_t> committed;
    std::unordered_set<uint64_t> rolled_back;
    for (const auto& wr : recs) {
        if (wr.op == newdb::WalOp::COMMIT) {
            committed.insert(wr.txn_id);
            continue;
        }
        if (wr.op == newdb::WalOp::ROLLBACK) {
            rolled_back.insert(wr.txn_id);
            continue;
        }
        if (!wr.has_row) {
            continue;
        }
        TxnRecord rec;
        rec.txn_id = static_cast<int64_t>(wr.txn_id);
        rec.table_name = wr.table;
        rec.key = std::to_string(wr.row.id);
        const auto it_old = wr.row.attrs.find("__wal_old");
        const auto it_new = wr.row.attrs.find("__wal_new");
        const auto it_op = wr.row.attrs.find("__wal_op");
        rec.old_value = (it_old != wr.row.attrs.end()) ? it_old->second : std::string{};
        rec.new_value = (it_new != wr.row.attrs.end()) ? it_new->second : std::string{};
        if (it_op != wr.row.attrs.end()) {
            rec.operation = it_op->second;
        } else if (wr.op == newdb::WalOp::INSERT) {
            rec.operation = "INSERT";
        } else if (wr.op == newdb::WalOp::DELETE) {
            rec.operation = "DELETE";
        } else {
            rec.operation = "UPDATE";
        }
        txn_records[wr.txn_id].push_back(std::move(rec));
    }

    std::unordered_map<uint64_t, std::vector<TxnRecord>> dangling_by_txn;
    for (auto& kv : txn_records) {
        if (committed.find(kv.first) != committed.end()) {
            continue;
        }
        if (rolled_back.find(kv.first) != rolled_back.end()) {
            continue;
        }
        dangling_by_txn.emplace(kv.first, kv.second);
    }

    if (dangling_by_txn.empty()) {
        return true;
    }

    std::size_t dangling_count = 0;
    for (const auto& kv : dangling_by_txn) {
        dangling_count += kv.second.size();
    }
    
    logging_console_printf("[WAL] Found %zu uncommitted operations, starting recovery...\n",
                           dangling_count);

    std::map<std::string, std::vector<TxnRecord>> ops_by_table;
    for (const auto& kv : dangling_by_txn) {
        for (const auto& rec : kv.second) {
            ops_by_table[rec.table_name].push_back(rec);
        }
    }

    auto parse_attrs = [](const std::string& packed) {
        std::map<std::string, std::string> attrs;
        std::size_t i = 0;
        while (i < packed.size()) {
            const std::size_t sep = packed.find('=', i);
            if (sep == std::string::npos) {
                break;
            }
            const std::size_t end = packed.find(';', sep + 1);
            const std::string key = packed.substr(i, sep - i);
            const std::string val =
                (end == std::string::npos) ? packed.substr(sep + 1) : packed.substr(sep + 1, end - (sep + 1));
            if (!key.empty()) {
                attrs[key] = val;
            }
            if (end == std::string::npos) {
                break;
            }
            i = end + 1;
        }
        return attrs;
    };

    for (auto& table_kv : ops_by_table) {
        const std::string& table_name = table_kv.first;
        std::vector<TxnRecord>& table_ops = table_kv.second;
        const std::string data_file = resolveDataFilePath(table_name);

        for (auto it = table_ops.rbegin(); it != table_ops.rend(); ++it) {
            const auto& rec = *it;
            if (rec.operation == "TXN_BEGIN") {
                continue;
            }
            try {
                const int id = std::stoi(rec.key);
                if (rec.operation == "INSERT") {
                    newdb::Row tombstone;
                    tombstone.id = id;
                    tombstone.attrs["__deleted"] = "1";
                    (void)newdb::io::append_row(data_file.c_str(), tombstone);
                    logging_console_printf("[WAL] Undo INSERT id=%d on %s\n", id, table_name.c_str());
                } else if (rec.operation == "UPDATE" || rec.operation == "DELETE") {
                    newdb::Row recovered;
                    recovered.id = id;
                    const auto attrs = parse_attrs(rec.old_value);
                    for (const auto& kv : attrs) {
                        if (kv.first != "__deleted") {
                            recovered.attrs[kv.first] = kv.second;
                        }
                    }
                    (void)newdb::io::append_row(data_file.c_str(), recovered);
                    logging_console_printf("[WAL] Undo %s id=%d on %s\n",
                                           rec.operation.c_str(),
                                           id,
                                           table_name.c_str());
                }
            } catch (...) {
                logging_console_printf("[WAL] Skip malformed WAL record key=%s\n", rec.key.c_str());
            }
        }
    }

    // 为 dangling 事务补写回滚终态，确保恢复幂等（下次启动不会再次补偿）
    for (const auto& kv : dangling_by_txn) {
        const uint64_t txn = kv.first;
        m_txn_id.store(static_cast<int64_t>(txn));
        writeWAL("TXN_ROLLBACK", "", "", "", "recovered");
    }
    // 恢复完成后保留 WAL（审计与故障排查），追加标记
    writeWAL("RECOVERY_DONE", "", "", "", "");
    flushWAL();
    logging_console_printf("[WAL] Recovery complete\n");
    
    return true;
}

// ========== 后台 VACUUM ==========

void TxnCoordinator::startVacuumThread() {
    if (m_vacuum_running.load()) {
        return;
    }
    
    m_vacuum_running.store(true);
    m_vacuum_op_counter.store(0);
    
    m_vacuum_thread = std::thread([this]() {
        while (m_vacuum_running.load()) {
            std::unique_lock<std::mutex> lk(m_vacuum_mutex);
            
            // 等待信号或定期检查 (每 60 秒)
            m_vacuum_cv.wait_for(lk, std::chrono::seconds(60), [this]() {
                return !m_vacuum_running.load() || !m_vacuum_queue.empty();
            });
            
            if (!m_vacuum_running.load()) {
                break;
            }
            
            // 处理 VACUUM 队列
            while (!m_vacuum_queue.empty()) {
                std::string table = m_vacuum_queue.back();
                m_vacuum_queue.pop_back();
                m_vacuum_pending.erase(table);
                
                lk.unlock();
                
                // 执行 VACUUM 回调
                if (m_vacuum_callback) {
                    m_vacuum_callback(table);
                }
                
                lk.lock();
            }
        }
    });
}

void TxnCoordinator::stopVacuumThread() {
    if (!m_vacuum_running.load()) {
        return;
    }
    
    m_vacuum_running.store(false);
    m_vacuum_cv.notify_all();
    
    if (m_vacuum_thread.joinable()) {
        m_vacuum_thread.join();
    }
    m_vacuum_op_counter.store(0);
}

void TxnCoordinator::triggerVacuum(const std::string& table_name) {
    std::lock_guard<std::mutex> lk(m_vacuum_mutex);
    if (table_name.empty()) {
        return;
    }
    if (!m_vacuum_pending.insert(table_name).second) {
        return;
    }
    m_vacuum_queue.push_back(table_name);
    m_vacuum_cv.notify_one();
}

void TxnCoordinator::setVacuumCallback(std::function<void(const std::string&)> cb) {
    m_vacuum_callback = cb;
}

void TxnCoordinator::setVacuumOpsThreshold(const std::size_t threshold) {
    m_vacuum_ops_threshold.store(threshold == 0 ? 1 : threshold);
    persistVacuumConfig();
}

void TxnCoordinator::set_workspace_root(std::string path) {
    m_workspace_root = std::move(path);
    loadVacuumConfig();
}
