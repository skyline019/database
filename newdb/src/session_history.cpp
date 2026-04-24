#include <newdb/session_history.h>
#include <newdb/wal_manager.h>

#include <filesystem>
#include <fstream>
#include <chrono>
#include <iomanip>
#include <sstream>

namespace fs = std::filesystem;

namespace newdb {

namespace {

// Session log file name in workspace
std::string session_log_path(const std::string& workspace) {
    if (workspace.empty()) {
        return ".session_history";
    }
    return (fs::path(workspace) / ".session_history").string();
}

// Current session ID (thread-local)
thread_local uint64_t tls_current_session_id = 0;

} // namespace

SessionHistoryManager::SessionHistoryManager(std::string workspace_root)
    : workspace_root_(std::move(workspace_root)) {
    // Initialize WAL manager for session persistence (optional, using file log for now)
}

SessionHistoryManager::~SessionHistoryManager() = default;

uint64_t SessionHistoryManager::begin_session(const std::string& client_type, const std::string& workspace) {
    std::lock_guard<std::mutex> lg(mut_);
    
    // Generate session ID: timestamp (ms) + random
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    uint64_t session_id = ms;
    
    tls_current_session_id = session_id;
    
    // Write to log file
    std::string path = session_log_path(workspace.empty() ? workspace_root_ : workspace);
    std::ofstream out(path, std::ios::app);
    if (out) {
        auto time_t_now = std::chrono::system_clock::to_time_t(now);
        out << "=== SESSION START: " << session_id 
            << " | type=" << client_type
            << " | time=" << std::put_time(std::localtime(&time_t_now), "%F %T")
            << " ===\n";
    }
    
    return session_id;
}

void SessionHistoryManager::record_command(uint64_t session_id, const std::string& command) {
    if (session_id == 0) return;
    
    std::string path = session_log_path(workspace_root_);
    std::ofstream out(path, std::ios::app);
    if (out) {
        auto now = std::chrono::system_clock::now();
        auto ts = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
        out << "[" << session_id << "] " << ts << ": " << command << "\n";
    }
}

void SessionHistoryManager::end_session(uint64_t session_id) {
    if (session_id == 0) return;
    
    std::string path = session_log_path(workspace_root_);
    std::ofstream out(path, std::ios::app);
    if (out) {
        out << "=== SESSION END: " << session_id << " ===\n";
    }
}

std::vector<std::string> SessionHistoryManager::get_session_history(uint64_t session_id) const {
    std::vector<std::string> result;
    if (session_id == 0) return result;
    
    std::string path = session_log_path(workspace_root_);
    std::ifstream in(path);
    if (!in) return result;
    
    std::string line;
    std::string prefix = "[" + std::to_string(session_id) + "] ";
    while (std::getline(in, line)) {
        if (line.find(prefix) == 0) {
            result.push_back(line.substr(prefix.size()));
        }
    }
    return result;
}

std::vector<uint64_t> SessionHistoryManager::list_sessions() const {
    std::vector<uint64_t> result;
    std::string path = session_log_path(workspace_root_);
    std::ifstream in(path);
    if (!in) return result;
    
    std::string line;
    while (std::getline(in, line)) {
        if (line.find("=== SESSION START:") == 0) {
            // Parse session ID
            size_t pos = line.find(':');
            if (pos != std::string::npos) {
                std::string sid_str = line.substr(pos + 2); // skip ": "
                size_t space = sid_str.find(' ');
                if (space != std::string::npos) {
                    sid_str = sid_str.substr(0, space);
                    try {
                        uint64_t sid = std::stoull(sid_str);
                        result.push_back(sid);
                    } catch (...) {}
                }
            }
        }
    }
    return result;
}

SessionHistoryManager::SessionInfo SessionHistoryManager::get_session_info(uint64_t session_id) const {
    SessionInfo info{session_id, 0, 0, "", "", {}};
    std::string path = session_log_path(workspace_root_);
    std::ifstream in(path);
    if (!in) return info;
    
    std::string line;
    std::string start_marker = "=== SESSION START: " + std::to_string(session_id);
    bool in_session = false;
    
    while (std::getline(in, line)) {
        if (line.find(start_marker) == 0) {
            in_session = true;
            // Parse start time and client type
            size_t type_pos = line.find("type=");
            if (type_pos != std::string::npos) {
                info.client_type = line.substr(type_pos + 5);
                size_t space_end = info.client_type.find(' ');
                if (space_end != std::string::npos) {
                    info.client_type = info.client_type.substr(0, space_end);
                }
            }
            // Parse timestamp
            size_t time_pos = line.find("time=");
            if (time_pos != std::string::npos) {
                std::string ts_str = line.substr(time_pos + 5);
                // parse time string to timestamp (simplified: store as string)
                // For demo purposes, just keep as string or leave as 0
            }
            continue;
        }
        if (in_session) {
            if (line.find("=== SESSION END: " + std::to_string(session_id)) == 0) {
                info.ended_at = 1; // Mark as ended (real impl would parse timestamp)
                in_session = false;
                break;
            } else if (line.find("[") == 0) {
                // Command line: [sid] timestamp: cmd
                size_t colon = line.find(':');
                if (colon != std::string::npos) {
                    info.commands.push_back(line.substr(colon + 2));
                }
            } else if (line.find("=== SESSION START:") != std::string::npos) {
                // Next session started, stop
                break;
            }
        }
    }
    
    return info;
}

void SessionHistoryManager::cleanup_old_sessions(int max_age_days) {
    // Not implemented for demo
}

// Global accessor
SessionHistoryManager& get_session_history_manager() {
    static SessionHistoryManager instance(".");
    return instance;
}

} // namespace newdb