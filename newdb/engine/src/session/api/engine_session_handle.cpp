#include <newdb/engine_session_handle.h>
#include <newdb/engine_session_access.h>

#include <newdb/engine_session_opaque.h>
#include <newdb/session.h>

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>

namespace {

std::string null_to_str(const char* p) {
    return p ? std::string(p) : std::string();
}

// Mirrors [`shell_state_paths.h`](../../../cli/shell/state/shell_state_paths.h) without linking CLI.
std::string resolve_table_file(const std::string& data_dir, const std::string& rel_or_abs) {
    if (rel_or_abs.empty()) {
        return {};
    }
    namespace fs = std::filesystem;
    std::error_code ec;
    fs::path p(rel_or_abs);
    if (p.is_absolute()) {
        const fs::path c = fs::weakly_canonical(p, ec);
        return ec ? p.string() : c.string();
    }
    if (!data_dir.empty()) {
        const fs::path base = fs::absolute(data_dir, ec);
        return (base / p).lexically_normal().string();
    }
    const fs::path c = fs::absolute(p, ec);
    return ec ? (fs::current_path(ec) / p).lexically_normal().string() : c.string();
}

// Distinct from the C forward-declared `newdb_engine_session_opaque` typedef tag (MSVC name lookup).
struct EngineSessionStorage {
    std::string data_dir;
    std::string log_file_path;
    std::uint32_t flags{0};
    std::unique_ptr<newdb::Session> session;
};

} // namespace

namespace newdb {

Session* engine_session_borrow_cpp_session(newdb_engine_session_t* h) noexcept {
    if (h == nullptr) {
        return nullptr;
    }
    auto* st = reinterpret_cast<EngineSessionStorage*>(h);
    return st->session.get();
}

const Session* engine_session_borrow_cpp_session(const newdb_engine_session_t* h) noexcept {
    return engine_session_borrow_cpp_session(const_cast<newdb_engine_session_t*>(h));
}

} // namespace newdb

extern "C" {

newdb_engine_session_t* newdb_engine_session_create(const char* data_dir_c,
                                                    const char* default_table_c,
                                                    const char* log_path_c,
                                                    const uint32_t flags) {
    try {
        auto impl = std::make_unique<EngineSessionStorage>();
        impl->flags = flags;
        impl->data_dir = null_to_str(data_dir_c);
        impl->log_file_path = null_to_str(log_path_c);
        impl->session = std::make_unique<newdb::Session>();

        const std::string rel_or_abs = null_to_str(default_table_c);
        if (!rel_or_abs.empty()) {
            namespace fs = std::filesystem;
            const fs::path p(rel_or_abs);
            impl->session->table_name = p.stem().string();
            impl->session->data_path = resolve_table_file(impl->data_dir, rel_or_abs);
        }

        return reinterpret_cast<newdb_engine_session_t*>(impl.release());
    } catch (...) {
        return nullptr;
    }
}

void newdb_engine_session_destroy(newdb_engine_session_t* session) {
    if (session == nullptr) {
        return;
    }
    delete reinterpret_cast<EngineSessionStorage*>(session);
}

} // extern "C"
