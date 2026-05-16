#!
# @file Makefile
# @brief
# 存储模块
#
# @author niexw
# @email niexiaowen@uestc.edu.cn
#
INCLUDE := $(SOURCE_DIR)

STATIC_LIBS := libstorage.a
PROGRAMS := test_tuple test_record

libstorage.a_SOURCES := error.cc record.cc

test_tuple_SOURCES := test_tuple.cc
test_tuple_LOCAL_LIBS := -lutils

test_record_SOURCES := test_record.cc
test_record_LOCAL_LIBS := -lstorage -lutils

include $(SOURCE_DIR)/rules/program.mk
