#include "utils.h"

#include <gtest/gtest.h>

#include <vector>

TEST(DemoUtils, TrimWhitespace) {
    EXPECT_EQ(trim(""), "");
    EXPECT_EQ(trim("   "), "");
    EXPECT_EQ(trim("  ab c  "), "ab c");
    EXPECT_EQ(trim("\t\nx\r"), "x");
}

TEST(DemoUtils, ParseCommaArgsValid) {
    std::vector<std::string> out;
    ASSERT_TRUE(parse_comma_args(" (a, b) ", out));
    ASSERT_EQ(out.size(), 2u);
    EXPECT_EQ(out[0], "a");
    EXPECT_EQ(out[1], "b");
}

TEST(DemoUtils, ParseCommaArgsQuotedComma) {
    std::vector<std::string> out;
    ASSERT_TRUE(parse_comma_args("( \"a,b\" , c )", out));
    ASSERT_EQ(out.size(), 2u);
    EXPECT_EQ(out[0], "a,b");
    EXPECT_EQ(out[1], "c");
}

TEST(DemoUtils, ParseCommaArgsInvalid) {
    std::vector<std::string> out;
    EXPECT_FALSE(parse_comma_args("no parens", out));
    EXPECT_FALSE(parse_comma_args("(", out));
    EXPECT_FALSE(parse_comma_args("()", out));
}

TEST(DemoUtils, ValidDateAndDateTime) {
    EXPECT_TRUE(is_valid_date_str("2024-01-15"));
    EXPECT_FALSE(is_valid_date_str("2024-1-15"));
    EXPECT_FALSE(is_valid_date_str("24-01-15"));
    EXPECT_TRUE(is_valid_datetime_str("2024-01-15 12:30:45"));
    EXPECT_FALSE(is_valid_datetime_str("2024-01-15"));
}

TEST(DemoUtils, CurrentDateStringsNonEmpty) {
    EXPECT_FALSE(get_current_date_str().empty());
    EXPECT_FALSE(get_current_datetime_str().empty());
}
