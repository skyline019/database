#include <newdb/error_format.h>

#include <gtest/gtest.h>

TEST(ErrorFormat, ProducesStableMachineParsableLine) {
    const std::string out =
        newdb::format_error_line("session", "load_failed", "cannot open file");
    EXPECT_EQ(out,
              "[ERR] domain=session code=load_failed message=\"cannot open file\"");
}

TEST(ErrorFormat, EscapesMessageSpecialChars) {
    const std::string out = newdb::format_error_line("cli", "arg_invalid", "bad \"x\"\nnext");
    EXPECT_NE(out.find("message=\"bad \\\"x\\\"\\nnext\""), std::string::npos);
}
