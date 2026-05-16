#!
# @file Makefile
# @brief
# utils模块
#
# @author niexw
# @email niexiaowen@uestc.edu.cn
#
STATIC_LIBS := libutils.a
PROGRAMS := test_leb128 test_arena test_slice test_crc32c test_roaring \
	test_noncopyable test_call_stack

libutils.a_SOURCES := error.cc arena.cc system_errors.cc

test_leb128_SOURCES := test_leb128.cc
test_leb128_LOCAL_LIBS := -lutils

test_arena_SOURCES := test_arena.cc
test_arena_LOCAL_LIBS := -lutils

test_slice_SOURCES := test_slice.cc
test_slice_LOCAL_LIBS := -lutils

test_crc32c_SOURCES := test_crc32c.cc
test_crc32c_SYSTEM_LIBS := -lcrc32c

test_roaring_SOURCES := test_roaring.cc
test_roaring_SYSTEM_LIBS := -lroaring

test_noncopyable_SOURCES := test_noncopyable.cc

test_call_stack_SOURCES := test_call_stack.cc


include $(SOURCE_DIR)/rules/program.mk