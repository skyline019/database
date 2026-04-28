#include <newdb/page_io.h>
#include <newdb/schema_io.h>
#include <newdb/session.h>

#include <cstdio>
#include <chrono>
#include <cstring>
#include <sstream>
#include <string>

static void usage(const char* argv0) {
    std::fprintf(stderr,
                 "newdb_smoke — minimal harness for newdb/\n"
                 "  %s load <file.bin>           # load schema sidecar + heap pages\n"
                 "  %s create <file.bin>         # create empty heap + default schema\n"
                 "  %s append <file.bin> <id>    # append id=... row (demo)\n",
                 argv0,
                 argv0,
                 argv0);
}

static std::string stem_of_bin(const std::string& p) {
    auto slash = p.find_last_of("/\\");
    const std::string name = (slash == std::string::npos) ? p : p.substr(slash + 1);
    if (name.size() > 4 && name.compare(name.size() - 4, 4, ".bin") == 0) {
        return name.substr(0, name.size() - 4);
    }
    return name;
}

static std::string json_escape(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 8);
    for (const char ch : s) {
        switch (ch) {
            case '\"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default: out.push_back(ch); break;
        }
    }
    return out;
}

static int finish_result(bool json,
                         int code,
                         const char* command,
                         const std::string& bin,
                         double elapsed_ms,
                         const char* status,
                         const std::string& message = {},
                         std::size_t rows = 0,
                         const std::string& pk = {}) {
    if (!json) return code;
    std::ostringstream out;
    out << "{"
        << "\"tool\":\"newdb_smoke\","
        << "\"status\":\"" << status << "\","
        << "\"command\":\"" << command << "\","
        << "\"bin\":\"" << json_escape(bin) << "\","
        << "\"elapsed_ms\":" << elapsed_ms;
    if (!message.empty()) {
        out << ",\"message\":\"" << json_escape(message) << "\"";
    }
    if (rows > 0 || std::strcmp(command, "load") == 0) {
        out << ",\"rows\":" << rows;
    }
    if (!pk.empty()) {
        out << ",\"primary_key\":\"" << json_escape(pk) << "\"";
    }
    out << "}";
    std::printf("%s\n", out.str().c_str());
    return code;
}

int main(int argc, char** argv) {
    bool json = false;
    int argi = 1;
    if (argc >= 2 && std::strcmp(argv[1], "--json") == 0) {
        json = true;
        argi = 2;
    }
    if (argc - argi < 2) {
        usage(argv[0]);
        return finish_result(json, 1, "", "", 0.0, "error", "invalid arguments");
    }
    const char* cmd = argv[argi];
    const std::string bin = argv[argi + 1];
    const auto t0 = std::chrono::steady_clock::now();

    newdb::Session se;
    se.data_path = bin;
    se.table_name = stem_of_bin(bin);

    if (std::strcmp(cmd, "load") == 0) {
        const newdb::Status st = se.reload();
        if (!st.ok) {
            std::fprintf(stderr, "load failed: %s\n", st.message.c_str());
            const auto t1 = std::chrono::steady_clock::now();
            const auto ms = static_cast<double>(
                std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count());
            return finish_result(json, 2, cmd, bin, ms, "error", st.message);
        }
        std::printf("loaded %s rows=%zu pk=%s\n",
                    se.table_name.c_str(),
                    se.table.rows.size(),
                    se.schema.primary_key.c_str());
        const auto t1 = std::chrono::steady_clock::now();
        const auto ms = static_cast<double>(
            std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count());
        return finish_result(json, 0, cmd, bin, ms, "ok", "", se.table.rows.size(), se.schema.primary_key);
    }

    if (std::strcmp(cmd, "create") == 0) {
        newdb::TableSchema sch;
        sch.primary_key = "id";
        const newdb::Status w1 = newdb::io::create_heap_file(bin.c_str(), {});
        if (!w1.ok) {
            std::fprintf(stderr, "create heap failed: %s\n", w1.message.c_str());
            const auto t1 = std::chrono::steady_clock::now();
            const auto ms = static_cast<double>(
                std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count());
            return finish_result(json, 2, cmd, bin, ms, "error", w1.message);
        }
        const std::string sidecar = newdb::schema_sidecar_path_for_data_file(bin);
        const newdb::Status w2 = newdb::save_schema_text(sidecar, sch);
        if (!w2.ok) {
            std::fprintf(stderr, "write schema failed: %s\n", w2.message.c_str());
            const auto t1 = std::chrono::steady_clock::now();
            const auto ms = static_cast<double>(
                std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count());
            return finish_result(json, 2, cmd, bin, ms, "error", w2.message);
        }
        std::printf("created %s and %s\n", bin.c_str(), sidecar.c_str());
        const auto t1 = std::chrono::steady_clock::now();
        const auto ms = static_cast<double>(
            std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count());
        return finish_result(json, 0, cmd, bin, ms, "ok");
    }

    if (std::strcmp(cmd, "append") == 0) {
        if (argc - argi < 3) {
            usage(argv[0]);
            const auto t1 = std::chrono::steady_clock::now();
            const auto ms = static_cast<double>(
                std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count());
            return finish_result(json, 1, cmd, bin, ms, "error", "append requires id");
        }
        const int id = std::atoi(argv[argi + 2]);
        newdb::Row r;
        r.id = id;
        r.attrs["note"] = "smoke";
        const newdb::Status st = newdb::io::append_row(bin.c_str(), r);
        if (!st.ok) {
            std::fprintf(stderr, "append failed: %s\n", st.message.c_str());
            const auto t1 = std::chrono::steady_clock::now();
            const auto ms = static_cast<double>(
                std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count());
            return finish_result(json, 2, cmd, bin, ms, "error", st.message);
        }
        std::printf("appended id=%d\n", id);
        const auto t1 = std::chrono::steady_clock::now();
        const auto ms = static_cast<double>(
            std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count());
        return finish_result(json, 0, cmd, bin, ms, "ok");
    }

    usage(argv[0]);
    const auto t1 = std::chrono::steady_clock::now();
    const auto ms = static_cast<double>(
        std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count());
    return finish_result(json, 1, cmd, bin, ms, "error", "unknown command");
}
