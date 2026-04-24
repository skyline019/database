#include <newdb/error.h>

#include <gtest/gtest.h>

using newdb::Status;

TEST(Status, OkDefault) {
    const Status s = Status::Ok();
    EXPECT_TRUE(s.ok);
    EXPECT_TRUE(static_cast<bool>(s));
    EXPECT_FALSE(s.failed());
    EXPECT_TRUE(s.message.empty());
}

TEST(Status, FailCarriesMessage) {
    const Status s = Status::Fail("oops");
    EXPECT_FALSE(s.ok);
    EXPECT_FALSE(static_cast<bool>(s));
    EXPECT_TRUE(s.failed());
    EXPECT_EQ(s.message, "oops");
}
