#pragma once

#include <cstdint>
#include <string>

// Reads `demodb.wal_lsn` in the same directory as table data files. Used to bind
// sidecar files to a durable write-generation (high-water LSN) instead of mtime^size.
std::uint64_t read_wal_lsn_for_workspace(const std::string& workspace_dir);

void write_wal_lsn_for_workspace(const std::string& workspace_dir, std::uint64_t lsn);

// Workspace root for a `table.bin` path: parent of the file, or "" if not derivable.
std::string workspace_dir_for_data_file(const std::string& data_file);
