#!
# @file sanitize.mk
# @brief
# sanitizer相关规则
#
# @author niexw
# @email niexiaowen@uestc.edu.cn
#
ifndef __SANITIZE_MK__
__SANITIZE_MK__ := 1	# 全局sanitize规则

# 仅在调试模式下添加sanitizer选项
ifeq "$(BUILD_TYPE)" "DEBUG"

# 检查地址、内存泄漏、未定义行为
SANITIZE_COMPILE_FLAGS := -fsanitize=address,leak,undefined
SANITIZE_LINK_FLAGS := -lubsan

endif

endif	# __SANITIZE_MK__