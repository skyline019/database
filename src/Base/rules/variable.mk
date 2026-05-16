#!
# @file variable.mk
# @brief
# 定义shell变量
#
# @author niexw
# @email niexiaowen@uestc.edu.cn
#
ifndef __VARIABLE_MK__
__VARIABLE_MK__ := 1

# 定义shell变量
CD := cd
MV := mv
CP := cp
RM := rm -fr
MKDIR := mkdir -p
GREP := grep
ECHO := echo
AWK := awk
SED := sed
PRINTF := printf
SHELL := sh
MAKE := make
CHMOD := chmod
PATCH := patch -tb
PWD := pwd
TOUCH := touch
NM := nm
SORT := sort
OBJCOPY := objcopy
XARGS := xargs
TOUCH := touch
PROTOC := protoc

# 据说，clang++对协程的支持更好
CC := gcc #clang
CXX := g++ #clang++
LINT := clang-tidy
# 编译器会封装ld链接器，为便于移植，不要使用ld
LD := g++ # clang++ # -v
AR := ar

# 定义颜色
BLACK := "\e[30m"
RED := "\e[31m"
GREEN := "\e[32m"
YELLOW := "\e[33m"
BLUE := "\e[34m"
PURPLE := "\e[35m"
CYAN := "\e[36m"
WHITE := "\e[37m"
RESET := "\e[0m"

# 按照MAKELEVEL输出空格
define SPACES
$(shell printf "%*s" $$(($(MAKELEVEL) * 2)) "")
endef

# 在当前make中导出shell变量
export CD MV CP RM MKDIR GREP ECHO AWK SED PRINTF SHELL CHMOD PATCH TOUCH CC CXX LD AR LINT

# 提取完整路径
define _get_path_name
$(realpath $(dir $(1)))
endef

# 提取文件名
define _get_file_name
$(notdir $(1))
endef

# 从BUILD_DIR得到SOURCE_DIR目录下对应的文件或目录
define _corresponding_in_source_dir
$(subst $(BUILD_DIR),$(SOURCE_DIR),$(1))
endef

# 从SOURCE_DIR得到BUILD_DIR目录下对应的文件或目录
define _corresponding_in_build_dir
$(subst $(SOURCE_DIR),$(BUILD_DIR),$(1))
endef

endif
