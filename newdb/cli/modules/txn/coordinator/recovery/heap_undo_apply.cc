#include "cli/modules/txn/coordinator/recovery/heap_undo_apply.h"

#include <newdb/page_io.h>

namespace cli_txn_heap_undo {

std::map<std::string, std::string> parse_packed_attrs(const std::string& packed) {
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
}

bool append_undo_row_to_heap(const char* data_file_path, const TxnWalUndoView& op) {
    if (data_file_path == nullptr || op.rec == nullptr) {
        return false;
    }
    const TxnRecord& rec = *op.rec;
    if (rec.operation == "TXN_BEGIN") {
        return true;
    }
    try {
        const int id = std::stoi(rec.key);
        if (rec.operation == "INSERT") {
            newdb::Row tomb;
            tomb.id = id;
            tomb.attrs["__deleted"] = "1";
            (void)newdb::io::append_row(data_file_path, tomb);
            return true;
        }
        if (rec.operation == "UPDATE" || rec.operation == "DELETE") {
            if (op.has_before && op.before_row != nullptr) {
                (void)newdb::io::append_row(data_file_path, *op.before_row);
            } else {
                newdb::Row recovered;
                recovered.id = id;
                const auto attrs = parse_packed_attrs(rec.old_value);
                for (const auto& kv : attrs) {
                    if (kv.first != "__deleted") {
                        recovered.attrs[kv.first] = kv.second;
                    }
                }
                (void)newdb::io::append_row(data_file_path, recovered);
            }
            return true;
        }
    } catch (...) {
        return false;
    }
    return false;
}

}  // namespace cli_txn_heap_undo
