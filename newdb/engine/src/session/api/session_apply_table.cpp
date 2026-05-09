#include <newdb/session_apply_table.h>

#include <newdb/schema_io.h>

#include <filesystem>
#include <string>

namespace newdb {

std::string resolve_workspace_table_file(const std::string& data_dir, const std::string& rel_or_abs) {
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

Status session_apply_table_stem_and_reload_schema(Session& session,
                                                  const std::string& data_dir,
                                                  const std::string& table_stem) {
    if (table_stem.empty()) {
        return Status::Fail("empty table stem");
    }
    const std::string data_file = table_stem + ".bin";
    const std::string resolved = resolve_workspace_table_file(data_dir, data_file);
    session.table_name = table_stem;
    session.data_path = resolved;
    // Match shell `reload_schema_from_data_path`: tolerate missing/invalid sidecar (best-effort load).
    (void)load_schema_text(schema_sidecar_path_for_data_file(resolved), session.schema);
    session.invalidate();
    return Status::Ok();
}

} // namespace newdb
