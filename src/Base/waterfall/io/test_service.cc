////
// @file test_service.cc
// @brief
// 测试服务
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <gtest/gtest.h>
#include <string>
#include "service.h"

namespace wf::io {

// Mock service class for testing
class mock_service : public service_base
{
  private:
    std::string name_;

  public:
    explicit mock_service(const std::string &name = "DefaultService")
        : name_(name)
    {}
    ~mock_service() override = default;

    const std::string &name() const { return name_; }

  private:
    void shutdown() override {}
};

// Test suite for service_registry
TEST(service, service_registry_use_service)
{
    service_registry registry;

    // Create a service with a custom name
    auto *service1 =
        registry.use_service<mock_service>(std::string("Service1"));
    ASSERT_NE(service1, nullptr);
    EXPECT_EQ(service1->name(), "Service1");

    // Reuse the same service
    auto *service2 =
        registry.use_service<mock_service>(std::string("Service1"));
    EXPECT_EQ(service1, service2);
}

TEST(service, service_registry_has_service)
{
    service_registry registry;

    // Initially, the service does not exist
    EXPECT_EQ(registry.has_service<mock_service>(), nullptr);

    // Create a service
    registry.use_service<mock_service>("Service1");

    // Now the service exists
    EXPECT_NE(registry.has_service<mock_service>(), nullptr);
}

TEST(service, service_registry_shutdown_and_destroy)
{
    service_registry registry;

    // Create a service
    registry.use_service<mock_service>("Service1");

    // Shutdown services
    registry.shutdown_services();

    // Destroy services
    registry.destroy_services();

    // After destruction, the service should not exist
    EXPECT_EQ(registry.has_service<mock_service>(), nullptr);
}

// Mock service class for testing move semantics
class mock_service_with_move : public service_base
{
  private:
    std::string name_;
    std::unique_ptr<std::vector<int>> data_;

  public:
    explicit mock_service_with_move(
        std::string name,
        std::unique_ptr<std::vector<int>> data)
        : name_(std::move(name))
        , data_(std::move(data))
    {}
    ~mock_service_with_move() override = default;

    const std::string &name() const { return name_; }
    const std::vector<int> &data() const { return *data_; }

  private:
    void shutdown() override {}
};

// Test suite for move semantics
TEST(service, service_registry_use_service_move_only)
{
    service_registry registry;

    // Create a service with move-only arguments
    auto data =
        std::make_unique<std::vector<int>>(std::initializer_list<int>{1, 2, 3});
    auto *service = registry.use_service<mock_service_with_move>(
        "Service1", std::move(data));
    ASSERT_NE(service, nullptr);
    EXPECT_EQ(service->name(), "Service1");
    EXPECT_EQ(service->data(), (std::vector<int>{1, 2, 3}));

    // Ensure the original unique_ptr is empty
    EXPECT_EQ(data, nullptr);
}

TEST(service, service_registry_use_service_lvalue)
{
    service_registry registry;

    // Pass an lvalue to the service
    std::string name = "Service2";
    auto *service = registry.use_service<mock_service_with_move>(
        name,
        std::make_unique<std::vector<int>>(
            std::initializer_list<int>{4, 5, 6}));
    ASSERT_NE(service, nullptr);
    EXPECT_EQ(service->name(), "Service2");
    EXPECT_EQ(service->data(), (std::vector<int>{4, 5, 6}));
}

TEST(service, service_registry_use_service_mixed_args)
{
    service_registry registry;

    // Pass a mix of lvalues and rvalues
    std::string name = "Service3";
    auto *service = registry.use_service<mock_service_with_move>(
        std::move(name),
        std::make_unique<std::vector<int>>(
            std::initializer_list<int>{7, 8, 9}));
    ASSERT_NE(service, nullptr);
    EXPECT_EQ(service->name(), "Service3");
    EXPECT_EQ(service->data(), (std::vector<int>{7, 8, 9}));

    // Ensure the original string is empty
    EXPECT_TRUE(name.empty());
}

TEST(service, service_registry_use_service_large_object)
{
    service_registry registry;

    // Pass a large object to the service
    std::vector<int> large_data(1000, 42); // 1000 elements initialized to 42
    auto *service = registry.use_service<mock_service_with_move>(
        "LargeService",
        std::make_unique<std::vector<int>>(std::move(large_data)));
    ASSERT_NE(service, nullptr);
    EXPECT_EQ(service->name(), "LargeService");
    EXPECT_EQ(service->data().size(), (size_t) 1000);
    EXPECT_EQ(service->data()[0], 42);

    // Ensure the original vector is empty
    EXPECT_TRUE(large_data.empty());
}

} // namespace wf::io

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}