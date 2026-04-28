#pragma once

// Copy *.bin (+ optional .attr) from dir_path into dest_workspace.
// When dest_workspace is null or empty, use current working directory.
bool import_tables_from_directory(const char* dir_path, const char* dest_workspace, const char* log_file);
