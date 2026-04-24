#include <newdb/session.h>

#include <newdb/heap_storage.h>
#include <newdb/error_format.h>
#include <newdb/page_io.h>
#include <newdb/schema_io.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <utility>

namespace newdb {

namespace {

const char* session_error_code(const Status& st) {
    if (st.message == "no table selected") {
        return "no_table_selected";
    }
    return "load_failed";
}

Status session_reload_impl(Session& s) {
    const std::string sidecar = schema_sidecar_path_for_data_file(s.data_path);
    const Status s1 = load_schema_text(sidecar, s.schema);
    if (!s1.ok) {
        return s1;
    }
    HeapLoadOptions load_opts{};
    const char* lazy_env = std::getenv("NEWDB_LAZY_HEAP");
    if (lazy_env != nullptr && std::strcmp(lazy_env, "1") == 0) {
        load_opts.lazy_decode = true;
    }
    const Status s2 = io::load_heap_file(s.data_path.c_str(), s.table_name, s.schema, s.table, load_opts);
    s.cache_valid = s2.ok;
    return s2;
}

Status session_ensure_loaded_impl(Session& s) {
    if (s.data_path.empty()) {
        return Status::Fail("no table selected");
    }
    if (s.cache_valid) {
        return Status::Ok();
    }
    return session_reload_impl(s);
}

} // namespace

Session::HeapAccess::~HeapAccess() = default;

void Session::reset_memory() {
    std::lock_guard<std::mutex> g(mut_);
    table.clear_data();
    cache_valid = false;
}

void Session::invalidate() {
    std::lock_guard<std::mutex> g(mut_);
    cache_valid = false;
}

Status Session::reload() {
    std::lock_guard<std::mutex> g(mut_);
    return session_reload_impl(*this);
}

Status Session::ensure_loaded() {
    std::lock_guard<std::mutex> g(mut_);
    return session_ensure_loaded_impl(*this);
}

void Session::set_snapshot(const MVCCSnapshot& snapshot) {
    std::lock_guard<std::mutex> g(mut_);
    table.set_snapshot(snapshot);
    table.rebuild_indexes(schema);
}

void Session::clear_snapshot() {
    std::lock_guard<std::mutex> g(mut_);
    table.clear_snapshot();
    table.rebuild_indexes(schema);
}

Session::HeapAccess Session::lock_heap(const char* log_file) {
    std::unique_lock<std::mutex> lk(mut_);
    const Status s = session_ensure_loaded_impl(*this);
    if (!s.ok) {
        if (log_file != nullptr) {
            const std::string msg = format_error_line("session", session_error_code(s), s.message);
            std::fprintf(stderr, "%s\n", msg.c_str());
        }
        lk.unlock();
        return HeapAccess{};
    }
    HeapAccess out;
    out.lock_.emplace(std::move(lk));
    out.session_ = this;
    out.ok_ = true;
    return out;
}

HeapTable* Session::mutable_heap(const char* log_file) {
    std::lock_guard<std::mutex> g(mut_);
    const Status s = session_ensure_loaded_impl(*this);
    if (!s.ok) {
        if (log_file != nullptr) {
            const std::string msg = format_error_line("session", session_error_code(s), s.message);
            std::fprintf(stderr, "%s\n", msg.c_str());
        }
        return nullptr;
    }
    return &table;
}

} // namespace newdb
