#!
# @file debug.mk
# @brief
# 调试模式相关规则
#
# @author niexw
# @email niexiaowen@uestc.edu.cn
#
ifndef __DEBUG_MK__
__DEBUG_MK__ := 1	# 全局debug规则

include $(SOURCE_DIR)/rules/sanitize.mk

# 检查BUILD_TYPE合法性
ifneq ($(BUILD_TYPE),DEBUG)
ifneq ($(BUILD_TYPE),RELEASE)
$(error BUILD_TYPE must be DEBUG or RELEASE, but got '$(BUILD_TYPE)')
endif
endif

# -Os是-O2的子集，-Os会打开-foptimize-sibling-calls尾折叠选项
ifeq "$(BUILD_TYPE)" "DEBUG"
CFLAGS += -O0 -g -DDEBUG -fno-omit-frame-pointer -foptimize-sibling-calls $(SANITIZE_COMPILE_FLAGS)
CXXFLAGS += -O0 -g -DDEBUG -fno-omit-frame-pointer -foptimize-sibling-calls $(SANITIZE_COMPILE_FLAGS)
# 添加链接选项
LDFLAGS += -fno-omit-frame-pointer $(SANITIZE_COMPILE_FLAGS) $(SANITIZE_LINK_FLAGS) -L$(BUILD_DIR)/bin
endif
ifeq "$(BUILD_TYPE)" "RELEASE"
CFLAGS += -O2
CXXFLAGS += -O2
LDFLAGS += -Wno-unused-function -flto -L$(BUILD_DIR)/bin
endif

endif	# __DEBUG_MK__