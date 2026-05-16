#include <gtest/gtest.h>

TEST(SampleMath, Add) {
    EXPECT_EQ(2 + 3, 5);
}

TEST(SampleString, Prefix) {
    const std::string v = "gtest_capi";
    EXPECT_TRUE(v.rfind("gtest", 0) == 0);
}
