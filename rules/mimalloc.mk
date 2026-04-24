#!
# @file mimalloc.mk
# @brief
# mimalloc替代c库中的内存分配函数
#
# @author niexw
# @email niexiaowen@uestc.edu.cn
#
ifndef __MIMALLOC_MK__
__MIMALLOC_MK__ := 1	# 全局mimalloc规则

CFLAGS += -fno-builtin-malloc
CXXFLAGS += -fno-builtin-malloc

# mimalloc库
LDFLAGS += -lmimalloc

endif	# __MIMALLOC_MK__
