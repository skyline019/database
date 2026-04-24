#!
# @file Makefile
# @brief
# io模块Makefile
#
# @author niexw
# @email niexiaowen@uestc.edu.cn
#
INCLUDE := $(SOURCE_DIR)

STATIC_LIBS := libio.a

libio.a_SOURCES := error.cc service.cc thread.cc thread_pool.cc scheduler.cc \
	reactor.cc channel.cc timer.cc strand.cc uring.cc coro.cc

PROGRAMS := test_task test_service test_thread test_thread_pool test_scheduler \
	test_reactor test_timer test_strand test_uring test_coro

test_task_SOURCES := test_task.cc
test_task_LOCAL_LIBS := -lio

test_service_SOURCES := test_service.cc
test_service_LOCAL_LIBS := -lio

test_thread_SOURCES := test_thread.cc
test_thread_LOCAL_LIBS := -lio

test_thread_pool_SOURCES := test_thread_pool.cc
test_thread_pool_LOCAL_LIBS := -lio

test_scheduler_SOURCES := test_scheduler.cc
test_scheduler_LOCAL_LIBS := -lio -lutils

test_reactor_SOURCES := test_reactor.cc
test_reactor_LOCAL_LIBS := -lio -lutils

test_timer_SOURCES := test_timer.cc
test_timer_LOCAL_LIBS := -lio -lutils

test_strand_SOURCES := test_strand.cc
test_strand_LOCAL_LIBS := -lio -lutils

test_uring_SOURCES := test_uring.cc
test_uring_LOCAL_LIBS := -lio -lutils
test_uring_SYSTEM_LIBS := -luring

test_coro_SOURCES := test_coro.cc
test_coro_LOCAL_LIBS := -lio -lutils


include $(SOURCE_DIR)/rules/program.mk
