#pragma once

#include <string>
#include <vector>

// Logical schema names list (.schema_history under workspace) + table membership (SCHEMA: in .attr).
// `workspace_root` empty => use current working directory (legacy behavior).

bool create_schema(const std::string& workspace_root, const std::string& schema_name);
bool delete_schema(const std::string& workspace_root, const std::string& schema_name);
void list_schemas(const std::string& workspace_root, std::vector<std::string>& schemas);
void get_tables_in_schema(const std::string& workspace_root,
                          const std::string& schema_name,
                          std::vector<std::string>& tables);
