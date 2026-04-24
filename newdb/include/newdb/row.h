#pragma once

#include <string>
#include <unordered_map>
#include <vector>

namespace newdb {

// One logical row: fixed int id + arbitrary string attributes.
struct Row {
    int id{0};
    std::unordered_map<std::string, std::string> attrs;
    // Column-oriented cache aligned to TableSchema::attrs order (filled by HeapTable::rebuild).
    std::vector<std::string> values;
};

} // namespace newdb
