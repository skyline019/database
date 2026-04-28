#pragma once

#include <newdb/error.h>

#include <string>
#include <vector>

namespace newdb {

// Filesystem catalog for database directories under a root path (no global singleton).
class Catalog {
public:
    explicit Catalog(std::string root_path);

    const std::string& root() const { return root_; }
    const std::string& current_database() const { return current_db_; }

    Status create_database(const std::string& name);
    Status drop_database(const std::string& name);
    Status use_database(const std::string& name);

    bool has_database(const std::string& name) const;
    std::vector<std::string> list_databases() const;
    std::string current_database_path() const;

    void clear_selection() { current_db_.clear(); }

private:
    std::string root_;
    std::string current_db_;
};

} // namespace newdb
