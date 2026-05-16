////
// @file test_noncopyable.cc
// @brief
// 测试noncopyable.h
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <gtest/gtest.h>
#include <type_traits>
#include "noncopyable.h"

namespace wf::utils {

TEST(NoncopyableTest, BaseNotCopyable)
{
    EXPECT_FALSE(std::is_copy_constructible_v<noncopyable>);
    EXPECT_FALSE(std::is_copy_assignable_v<noncopyable>);
}

TEST(NoncopyableTest, DerivedBehavior)
{
    struct derived : noncopyable
    {};
    EXPECT_TRUE(std::is_default_constructible_v<derived>);
    EXPECT_FALSE(std::is_copy_constructible_v<derived>);
    EXPECT_FALSE(std::is_copy_assignable_v<derived>);
}

} // namespace wf::utils

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
