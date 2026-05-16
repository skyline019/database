////
// @file test_call_stack.cc
// @brief
// 测试call_stack
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <gtest/gtest.h>
#include <string>
#include <thread>
#include "call_stack.h"

namespace wf::utils {

// 测试对象类
struct TestObject
{
    int id;
    std::string name;

    TestObject(int i, const std::string &n)
        : id(i)
        , name(n)
    {}
};

// 定义call_stack类型别名
using TestCallStack = call_stack<TestObject>;

// 测试call_stack的基本功能
TEST(CallStackTest, BasicFunctionality)
{
    TestObject obj1(1, "object1");
    TestObject obj2(2, "object2");

    // 初始状态下栈顶应为nullptr
    EXPECT_EQ(TestCallStack::top(), nullptr);

    {
        // 创建第一个tag
        TestCallStack::tag tag1(obj1);
        EXPECT_EQ(TestCallStack::top(), &obj1);

        {
            // 创建第二个tag（嵌套）
            TestCallStack::tag tag2(obj2);
            EXPECT_EQ(TestCallStack::top(), &obj2);
        }

        // 第二个tag销毁后，栈顶应回到第一个tag
        EXPECT_EQ(TestCallStack::top(), &obj1);
    }

    // 所有tag销毁后，栈顶应为nullptr
    EXPECT_EQ(TestCallStack::top(), nullptr);
}

// 测试空栈情况
TEST(CallStackTest, EmptyStack)
{
    EXPECT_EQ(TestCallStack::top(), nullptr);

    // 多次调用top()应该都返回nullptr
    EXPECT_EQ(TestCallStack::top(), nullptr);
    EXPECT_EQ(TestCallStack::top(), nullptr);
}

// 测试单层栈
TEST(CallStackTest, SingleLevelStack)
{
    TestObject obj(42, "single_object");

    {
        TestCallStack::tag tag(obj);
        EXPECT_EQ(TestCallStack::top(), &obj);
        EXPECT_EQ(TestCallStack::top()->id, 42);
        EXPECT_EQ(TestCallStack::top()->name, "single_object");
    }

    EXPECT_EQ(TestCallStack::top(), nullptr);
}

// 测试多层嵌套栈
TEST(CallStackTest, MultiLevelStack)
{
    TestObject obj1(1, "level1");
    TestObject obj2(2, "level2");
    TestObject obj3(3, "level3");

    {
        TestCallStack::tag tag1(obj1);
        EXPECT_EQ(TestCallStack::top(), &obj1);

        {
            TestCallStack::tag tag2(obj2);
            EXPECT_EQ(TestCallStack::top(), &obj2);

            {
                TestCallStack::tag tag3(obj3);
                EXPECT_EQ(TestCallStack::top(), &obj3);
            }

            EXPECT_EQ(TestCallStack::top(), &obj2);
        }

        EXPECT_EQ(TestCallStack::top(), &obj1);
    }

    EXPECT_EQ(TestCallStack::top(), nullptr);
}

// 测试栈的LIFO特性
TEST(CallStackTest, LIFOBehavior)
{
    TestObject obj1(1, "first");
    TestObject obj2(2, "second");
    TestObject obj3(3, "third");

    {
        TestCallStack::tag t1(obj1);
        EXPECT_EQ(TestCallStack::top(), &obj1);

        {
            TestCallStack::tag t2(obj2);
            EXPECT_EQ(TestCallStack::top(), &obj2);

            {
                TestCallStack::tag t3(obj3);
                EXPECT_EQ(TestCallStack::top(), &obj3);

                // 在tag对象仍然有效时验证栈的顺序
                // 编译选项保证可以访问私有成员prev_
                EXPECT_EQ(t3.prev_, &t2);     // t3的前一个应该是t2
                EXPECT_EQ(t2.prev_, &t1);     // t2的前一个应该是t1
                EXPECT_EQ(t1.prev_, nullptr); // t1应该是栈底，prev_为nullptr
            }

            // t3销毁后，栈顶应该回到t2
            EXPECT_EQ(TestCallStack::top(), &obj2);
        }

        // t2销毁后，栈顶应该回到t1
        EXPECT_EQ(TestCallStack::top(), &obj1);
    }

    // 所有tag销毁后，栈顶应为nullptr
    EXPECT_EQ(TestCallStack::top(), nullptr);
}

// 测试线程局部存储
TEST(CallStackTest, ThreadLocalStorage)
{
    TestObject main_obj(100, "main_thread");

    {
        TestCallStack::tag main_tag(main_obj);
        EXPECT_EQ(TestCallStack::top(), &main_obj);

        // 在另一个线程中测试
        std::thread worker([]() {
            // 新线程中栈顶应为nullptr
            EXPECT_EQ(TestCallStack::top(), nullptr);

            TestObject worker_obj(200, "worker_thread");
            TestCallStack::tag worker_tag(worker_obj);
            EXPECT_EQ(TestCallStack::top(), &worker_obj);
        });

        worker.join();

        // 主线程的栈顶应该保持不变
        EXPECT_EQ(TestCallStack::top(), &main_obj);
    }

    EXPECT_EQ(TestCallStack::top(), nullptr);
}

// 测试对象修改
TEST(CallStackTest, ObjectModification)
{
    TestObject obj(0, "initial");

    {
        TestCallStack::tag tag(obj);

        // 修改栈顶对象
        TestCallStack::top()->id = 999;
        TestCallStack::top()->name = "modified";

        EXPECT_EQ(obj.id, 999);
        EXPECT_EQ(obj.name, "modified");
    }
}

// 测试复杂对象类型
struct ComplexObject
{
    std::vector<int> data;
    std::map<std::string, int> mapping;

    ComplexObject(std::initializer_list<int> init)
        : data(init)
    {
        for (size_t i = 0; i < data.size(); ++i) {
            mapping["key" + std::to_string(i)] = data[i];
        }
    }
};

TEST(CallStackTest, ComplexObjectType)
{
    using ComplexCallStack = call_stack<ComplexObject>;

    ComplexObject obj({1, 2, 3, 4, 5});

    {
        ComplexCallStack::tag tag(obj);
        EXPECT_EQ(ComplexCallStack::top(), &obj);
        EXPECT_EQ(ComplexCallStack::top()->data.size(), 5);
        EXPECT_EQ(ComplexCallStack::top()->mapping.at("key2"), 3);
    }

    EXPECT_EQ(ComplexCallStack::top(), nullptr);
}

} // namespace wf::utils

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}