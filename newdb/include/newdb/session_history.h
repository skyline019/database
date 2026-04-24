#pragma once

#include <string>
#include <vector>
#include <memory>
#include <mutex>

namespace newdb {

class WalManager;
class Session;

// SessionHistoryManager: tracks and queries session command history
class SessionHistoryManager {
public:
    explicit SessionHistoryManager(std::string workspace_root);
    ~SessionHistoryManager();
    
    // Start a new session (called when CLI/GUI starts)
    uint64_t begin_session(const std::string& client_type, const std::string& workspace);
    
    // Record a command to current session log
    void record_command(uint64_t session_id, const std::string& command);
    
    // End a session (called on exit)
    void end_session(uint64_t session_id);
    
    // Get command history for a session
    std::vector<std::string> get_session_history(uint64_t session_id) const;
    
    // List all sessions
    std::vector<uint64_t> list_sessions() const;
    
    // Get session info
    struct SessionInfo {
        uint64_t session_id;
        uint64_t started_at;
        uint64_t ended_at;  // 0 if active
        std::string client_type;
        std::string workspace;
        std::vector<std::string> commands;  // empty for list_sessions (lazy load)
    };
    SessionInfo get_session_info(uint64_t session_id) const;
    
    // Cleanup: remove old sessions (older than N days)
    void cleanup_old_sessions(int max_age_days);
    
private:
    std::string workspace_root_;
    mutable std::mutex mut_;
    
    // In-memory cache of sessions (loaded from _sessions table)
    // For simplicity, we'll store sessions in a dedicated table using WalManager
    std::shared_ptr<WalManager> wal_;
    
    // Current session (thread-local for CLI, per-GUI-instance)
    thread_local static uint64_t current_session_id_;
};

// Global accessor
SessionHistoryManager& get_session_history_manager();

} // namespace newdb