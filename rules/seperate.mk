#!
# @file seperate.mk
# @brief
# 编译目录
#
# @author niexw
# @email niexiaowen@uestc.edu.cn
#
ifndef __SEPERATE_MK__
export __SEPERATE_MK__ := 1	# 全局编译目录，包括递归编译

# 全局变量
export BUILD_DIR := $(realpath $(CURDIR))
export SOURCE_DIR := $(realpath $(dir $(lastword $(MAKEFILE_LIST)))/..)
export BINARY_DIR := $(BUILD_DIR)/bin

include $(SOURCE_DIR)/rules/variable.mk
include $(SOURCE_DIR)/rules/project.mk

# 判断编译目录与源码目录是否相同
ifeq "$(SOURCE_DIR)" "$(BUILD_DIR)"

# 禁止在源码目录下编译
WARNING1 = $(shell printf $(RED)"===禁止在源码目录下编译==="$(RESET))
WARNING2 = $(shell printf $(RED)"> cd build"$(RESET))
WARNING3 = $(shell printf $(RED)"> make -f ../Makefile"$(RESET))
$(info $(WARNING1))
$(info $(WARNING2))
$(info $(WARNING3))
$(error )

else

# 所有目标依赖于build-dir目标
$(MAKECMDGOALS) all: _seperate_build

# 在编译目录下建立一个Makefile文件
_seperate_build:
	@if [ ! -f $(BUILD_DIR)/Makefile ]; then \
		$(ECHO) "include $(SOURCE_DIR)/Makefile" > \
			$(BUILD_DIR)/Makefile; \
	fi
	@if [ ! -e $(BUILD_DIR)/bin ]; then \
		$(MKDIR) $(BUILD_DIR)/bin; \
	fi

# 指定缺省目标
include $(SOURCE_DIR)/rules/target.mk

endif	# "$(SOURCE_DIR)" "$(BUILD_DIR)"

endif
