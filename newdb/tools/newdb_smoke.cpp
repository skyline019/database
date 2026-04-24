#include <newdb/page_io.h>
#include <newdb/schema_io.h>
#include <newdb/session.h>

#include <cstdio>
#include <cstring>
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

int main(int argc, char** argv) {
    if (argc < 3) {
        usage(argv[0]);
        return 1;
    }
    const char* cmd = argv[1];
    const std::string bin = argv[2];

    newdb::Session se;
    se.data_path = bin;
    se.table_name = stem_of_bin(bin);

    if (std::strcmp(cmd, "load") == 0) {
        const newdb::Status st = se.reload();
        if (!st.ok) {
            std::fprintf(stderr, "load failed: %s\n", st.message.c_str());
            return 2;
        }
        std::printf("loaded %s rows=%zu pk=%s\n",
                    se.table_name.c_str(),
                    se.table.rows.size(),
                    se.schema.primary_key.c_str());
        return 0;
    }

    if (std::strcmp(cmd, "create") == 0) {
        newdb::TableSchema sch;
        sch.primary_key = "id";
        const newdb::Status w1 = newdb::io::create_heap_file(bin.c_str(), {});
        if (!w1.ok) {
            std::fprintf(stderr, "create heap failed: %s\n", w1.message.c_str());
            return 2;
        }
        const std::string sidecar = newdb::schema_sidecar_path_for_data_file(bin);
        const newdb::Status w2 = newdb::save_schema_text(sidecar, sch);
        if (!w2.ok) {
            std::fprintf(stderr, "write schema failed: %s\n", w2.message.c_str());
            return 2;
        }
        std::printf("created %s and %s\n", bin.c_str(), sidecar.c_str());
        return 0;
    }

    if (std::strcmp(cmd, "append") == 0) {
        if (argc < 4) {
            usage(argv[0]);
            return 1;
        }
        const int id = std::atoi(argv[3]);
        newdb::Row r;
        r.id = id;
        r.attrs["note"] = "smoke";
        const newdb::Status st = newdb::io::append_row(bin.c_str(), r);
        if (!st.ok) {
            std::fprintf(stderr, "append failed: %s\n", st.message.c_str());
            return 2;
        }
        std::printf("appended id=%d\n", id);
        return 0;
    }

    usage(argv[0]);
    return 1;
}
