#!
# @file target.mk
# @brief
# 定义缺省目标
#
# @author niexw
# @email niexiaowen@uestc.edu.cn
#
ifndef __TARGET_MK__
__TARGET_MK__ := 1

include $(SOURCE_DIR)/rules/variable.mk

# 定义缺省目标变量
.DEFAULT_GOAL := all

# 定义伪目标
.PHONY: all clean doc dist lint test

# 抑制缺省输出目标
_supress_output:
	@true

# 缺省目标
all: _supress_output
clean: _supress_output
doc: _supress_output
test: _supress_output
dist: _supress_output
lint: _supress_output

# 包含clean规则
include $(SOURCE_DIR)/rules/clean.mk
# 保护dist规则
include $(SOURCE_DIR)/rules/dist.mk
# 包含subdir规则
include $(SOURCE_DIR)/rules/subdir.mk

# 一些方便函数
_add_prefix = $(addprefix $(1),$(2))
_corresponding_source_dir = $(subst $(BUILD_DIR),$(SOURCE_DIR),$(CURDIR))
_add_source_dir_prefix = $(call _add_prefix,$(_corresponding_source_dir)/,$(1))
_add_build_dir_prefix = $(call _add_prefix,$(CURDIR)/,$(1))

# SOURCES列表到对象文件的转换函数
OBJ_EXT_PAIRS = .proto:.pb.o .c:.o .cc:.o
_source_to_obj = $(strip $(foreach pair,$(OBJ_EXT_PAIRS), \
       $(patsubst %$(firstword $(subst :, ,$(pair))), \
               %$(lastword $(subst :, ,$(pair))), \
               $(filter %$(firstword $(subst :, ,$(pair))),$(1)))))

# SOURCES列表到其它中间文件的转换函数
MID_EXT_PAIRS = .proto:.pb.h .proto:.pb.cc .proto:.pb.cc.d .c:.c.d .cc:.cc.d
_source_to_mid = $(strip $(foreach pair,$(MID_EXT_PAIRS), \
       $(patsubst %$(firstword $(subst :, ,$(pair))), \
               %$(lastword $(subst :, ,$(pair))), \
               $(filter %$(firstword $(subst :, ,$(pair))),$(1)))))

# SOURCES列表到源文件的转换函数
_source_to_src = $(call _add_source_dir_prefix,$(1))

endif