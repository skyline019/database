#!
# @file Makefile
# @brief
# 项目根Makefile
#
# @author niexw
# @email niexiaowen@uestc.edu.cn
#

# 工程名
export PROJECT_NAME := waterfall
export VERSION := 0.1

# 编译类型：DEBUG|RELEASE
export BUILD_TYPE := DEBUG

# 子目录
SUBDIRS := docs/ waterfall/

# 三方库
# export FMT_DIR = $(SOURCE_DIR)/third/fmt-12.1.0
# export FMT_LIB_DIR = $(SOURCE_DIR)/third/fmt-12.1.0/build
# export UNIFEX_DIR = $(SOURCE_DIR)/third/libunifex-0.4.0
# export UNIFEX_LIB_DIR = $(SOURCE_DIR)/third/libunifex-0.4.0/build/source

# 编译目录与源目录分离
include $(realpath $(dir $(lastword $(MAKEFILE_LIST))))/rules/seperate.mk
