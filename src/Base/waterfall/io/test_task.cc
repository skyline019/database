////
// @file test_task.cc
// @brief
// 测试task.h
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <gtest/gtest.h>
#include <stdint.h>
#include "task.h"

namespace {

struct TestContext
{
    int calls = 0;
    int last_events = -1;
    void *last_owner_param = nullptr;
    wf::io::async_task *last_task = nullptr;
};

static void
cb_record(void *owner, wf::io::async_task *t, void *events); // forward

struct test_task : public wf::io::async_task
{
    TestContext *ctx_;
    explicit test_task(TestContext *c)
        : wf::io::async_task(cb_record)
        , ctx_(c)
    {}
};

static void cb_record(void *owner, wf::io::async_task *t, void *events)
{
    test_task *tt = static_cast<test_task *>(t);
    TestContext *c = tt->ctx_;
    c->calls++;
    c->last_events = static_cast<int>(reinterpret_cast<intptr_t>(events));
    c->last_owner_param = owner;
    c->last_task = t;
}

} // namespace

namespace wf::io {

TEST(AsyncTaskGTest, CompleteAndDestroy)
{
    TestContext ctx{};
    test_task t(&ctx);
    EXPECT_EQ(ctx.calls, 0);
    t.complete(reinterpret_cast<void *>(0x1), (void *) 5);
    EXPECT_EQ(ctx.calls, 1);
    EXPECT_EQ(ctx.last_events, 5);
    EXPECT_EQ(ctx.last_owner_param, reinterpret_cast<void *>(0x1));
    EXPECT_EQ(ctx.last_task, &t);

    t.destroy();
    EXPECT_EQ(ctx.calls, 2);
    EXPECT_EQ(ctx.last_events, 0);
    EXPECT_EQ(ctx.last_owner_param, nullptr);
    EXPECT_EQ(ctx.last_task, &t);
}

TEST(AsyncTaskGTest, EnqueueDequeueIsQueuedHeadEmpty)
{
    task_queue q;
    TestContext c1{}, c2{};
    test_task t1(&c1), t2(&c2);

    EXPECT_TRUE(q.empty());
    q.enqueue(t1);
    EXPECT_FALSE(q.empty());
    EXPECT_TRUE(q.is_queued(t1));
    q.enqueue(t2);
    EXPECT_TRUE(q.is_queued(t2));
    EXPECT_EQ(q.head(), &t1);

    q.dequeue();
    EXPECT_EQ(q.head(), &t2);
    EXPECT_FALSE(q.is_queued(t1));
    EXPECT_TRUE(q.is_queued(t2));

    q.dequeue();
    EXPECT_TRUE(q.empty());
    EXPECT_FALSE(q.is_queued(t2));
}

TEST(AsyncTaskGTest, MergeQueues)
{
    task_queue a, b;
    TestContext c3{}, c4{};
    test_task t3(&c3), t4(&c4);

    b.enqueue(t3);
    b.enqueue(t4);
    EXPECT_FALSE(b.empty());
    EXPECT_TRUE(a.empty());

    a.merge(b);
    EXPECT_FALSE(a.empty());
    EXPECT_TRUE(b.empty());
    EXPECT_EQ(a.head(), &t3);

    a.dequeue();
    a.dequeue();
    EXPECT_TRUE(a.empty());
}

TEST(AsyncTaskGTest, QueueDestructorInvokesDestroy)
{
    TestContext dc{};
    test_task dt(&dc);
    {
        task_queue dq;
        dq.enqueue(dt);
        EXPECT_EQ(dc.calls, 0);
    } // dq destructor should call destroy() on dt
    EXPECT_EQ(dc.calls, 1);
}

TEST(AsyncTaskGTest, MultipleEnqueueOrderPreserved)
{
    task_queue q;
    const int N = 5;
    TestContext ctx[N];
    test_task tasks[N] = {
        test_task(&ctx[0]),
        test_task(&ctx[1]),
        test_task(&ctx[2]),
        test_task(&ctx[3]),
        test_task(&ctx[4])};

    for (int i = 0; i < N; ++i)
        q.enqueue(tasks[i]);

    for (int i = 0; i < N; ++i) {
        EXPECT_EQ(q.head(), &tasks[i]);
        EXPECT_TRUE(q.is_queued(tasks[i]));
        q.dequeue();
        EXPECT_FALSE(q.is_queued(tasks[i]));
    }
    EXPECT_TRUE(q.empty());
}

TEST(AsyncTaskGTest, DequeueEmptyIsNoop)
{
    task_queue q;
    EXPECT_TRUE(q.empty());
    q.dequeue(); // should do nothing / not crash
    EXPECT_TRUE(q.empty());
}

TEST(AsyncTaskGTest, IsQueuedFalseForNonEnqueued)
{
    task_queue q;
    TestContext c{};
    test_task t(&c);
    EXPECT_FALSE(q.is_queued(t));
}

TEST(AsyncTaskGTest, MergeEmptyAndNonEmptyVariants)
{
    // merge empty into empty
    {
        task_queue a, b;
        a.merge(b);
        EXPECT_TRUE(a.empty());
        EXPECT_TRUE(b.empty());
    }

    // merge non-empty into empty (b <- a)
    {
        task_queue a, b;
        TestContext c1{}, c2{};
        test_task t1(&c1), t2(&c2);
        a.enqueue(t1);
        a.enqueue(t2);
        EXPECT_FALSE(a.empty());
        EXPECT_TRUE(b.empty());

        b.merge(a); // b should get a's elements
        EXPECT_FALSE(b.empty());
        EXPECT_TRUE(a.empty());
        EXPECT_EQ(b.head(), &t1);
        b.dequeue();
        b.dequeue();
        EXPECT_TRUE(b.empty());
    }

    // merge empty into non-empty (a <- b)
    {
        task_queue a, b;
        TestContext c1{}, c2{};
        test_task t1(&c1), t2(&c2);
        a.enqueue(t1);
        b.enqueue(t2);
        a.merge(b);
        EXPECT_FALSE(a.empty());
        EXPECT_TRUE(b.empty());
        EXPECT_EQ(a.head(), &t1);
        a.dequeue();
        EXPECT_EQ(a.head(), &t2);
        a.dequeue();
        EXPECT_TRUE(a.empty());
    }
}
} // namespace wf::io

namespace {
struct test_task2 : public wf::io::async_task
{
    int calls = 0;
    void *last_owner = nullptr;
    int last_events = -1;
    bool destroyed = false;

    test_task2()
        : wf::io::async_task(&test_task2::cb)
    {}

    static void cb(void *owner, async_task *at, void *events)
    {
        auto *self = static_cast<test_task2 *>(at);
        ++self->calls;
        self->last_owner = owner;
        self->last_events =
            static_cast<int>(reinterpret_cast<intptr_t>(events));
        if (owner == nullptr) self->destroyed = true;
    }
};

} // namespace

namespace wf::io {

TEST(AsyncTaskTest, reinterpret_cast)
{
    int x = 5;
    void *ptr = reinterpret_cast<void *>(x);
    int y = reinterpret_cast<intptr_t>(ptr);
    EXPECT_EQ(y, x);

    error_info err = reinterpret_cast<error_info>(ptr);
    int z = reinterpret_cast<intptr_t>(err);
    EXPECT_EQ(z, x);
}

// enqueue / dequeue / is_queued / head / empty basic behavior
TEST(AsyncTaskTest, EnqueueDequeueHeadIsQueuedEmpty)
{
    task_queue q;
    test_task2 a, b, c;

    EXPECT_TRUE(q.empty());
    EXPECT_FALSE(q.is_queued(a));

    q.enqueue(a);
    EXPECT_FALSE(q.empty());
    EXPECT_EQ(q.head(), &a);
    EXPECT_TRUE(q.is_queued(a));

    q.enqueue(b);
    EXPECT_EQ(q.head(), &a);
    EXPECT_TRUE(q.is_queued(b));

    q.enqueue(c);
    // dequeue first (a)
    q.dequeue();
    EXPECT_EQ(q.head(), &b);
    EXPECT_FALSE(q.is_queued(a));
    EXPECT_TRUE(q.is_queued(b));

    // dequeue b and c
    q.dequeue();
    q.dequeue();
    EXPECT_TRUE(q.empty());
    EXPECT_FALSE(q.is_queued(c));
}

// merge behavior (q1 non-empty + q2 non-empty)
TEST(AsyncTaskTest, MergeNonEmptyQueuesPreserveOrder)
{
    task_queue q1, q2;
    test_task2 t1, t2, t3;

    q1.enqueue(t1);
    q2.enqueue(t2);
    q2.enqueue(t3);

    q1.merge(q2);
    EXPECT_TRUE(q2.empty());
    // order should be t1, t2, t3
    EXPECT_EQ(q1.head(), &t1);
    q1.dequeue(); // removes t1
    EXPECT_EQ(q1.head(), &t2);
    q1.dequeue(); // removes t2
    EXPECT_EQ(q1.head(), &t3);
    q1.dequeue(); // removes t3
    EXPECT_TRUE(q1.empty());
}

// merge behavior (q1 empty + q2 non-empty)
TEST(AsyncTaskTest, MergeEmptyIntoNonEmptyQueue)
{
    task_queue q3, q4;
    test_task2 m1, m2;
    q4.enqueue(m1);
    q4.enqueue(m2);
    q3.merge(q4);
    EXPECT_FALSE(q3.empty());
    EXPECT_TRUE(q4.empty());
    // drain q3
    q3.dequeue();
    q3.dequeue();
    EXPECT_TRUE(q3.empty());
}

// complete and destroy callbacks
TEST(AsyncTaskTest, CompleteAndDestroyCallbacks)
{
    test_task2 cb;

    // Test complete callback
    cb.complete(reinterpret_cast<void *>(0x1234), (void *) 7);
    EXPECT_EQ(cb.calls, 1);
    EXPECT_EQ(cb.last_owner, reinterpret_cast<void *>(0x1234));
    EXPECT_EQ(cb.last_events, 7);

    // Test destroy callback
    cb.destroy();
    EXPECT_EQ(cb.calls, 2);
    EXPECT_EQ(cb.last_owner, nullptr);
    EXPECT_EQ(cb.last_events, 0);
    EXPECT_TRUE(cb.destroyed);
}

// queue destructor should call destroy on remaining tasks
TEST(AsyncTaskTest, QueueDestructorCallsDestroy)
{
    test_task2 d1, d2;
    {
        task_queue q;
        q.enqueue(d1);
        q.enqueue(d2);
        // q goes out of scope and its destructor must call destroy on d1 and d2
    }
    EXPECT_TRUE(d1.destroyed);
    EXPECT_TRUE(d2.destroyed);
}

TEST(AsyncTaskTest, ResultFieldInitialization)
{
    test_task2 task;
    // 验证result_字段正确初始化为OK
    EXPECT_EQ(task.result_, utils::OK);
}

TEST(AsyncTaskTest, ResultFieldAccessibility)
{
    test_task2 task;
    // 验证result_字段可以被访问和修改
    error_info original_result = task.result_;
    EXPECT_EQ(original_result, nullptr);

    // 可以测试result_字段的修改（如果需要）
    // task.result_ = &some_other_error;
}
} // namespace wf::io

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}