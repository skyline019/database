#include "logging.h"

#include "test_util.h"

#include <gtest/gtest.h>

TEST(DemoLogging, LoadPlainHistoryText) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_log_plain");
    std::filesystem::create_directories(dir);
    const std::string path = (dir / "plain.log").string();

    append_plain_log_line(path.c_str(), "hello");
    append_plain_log_line(path.c_str(), "world");

    const std::string text = load_log_file_text(path.c_str());
    EXPECT_NE(text.find("hello"), std::string::npos);
    EXPECT_NE(text.find("world"), std::string::npos);
}

TEST(DemoLogging, LoadEncryptedHistoryText) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_log_enc");
    std::filesystem::create_directories(dir);
    const std::string path = (dir / "enc.log").string();

    append_encrypted_log(path.c_str(), "line-a");
    append_encrypted_log(path.c_str(), "line-b");

    const std::string text = load_log_file_text(path.c_str());
    EXPECT_NE(text.find("line-a"), std::string::npos);
    EXPECT_NE(text.find("line-b"), std::string::npos);
}

