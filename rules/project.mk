#!
# @file project.mk
# @brief
# 输出工程信息
#
# @author niexw
# @email niexiaowen@uestc.edu.cn
#
ifndef __PROJECT_MK__
export __PROJECT_MK__ := 1

ifndef PROJECT_NAME
$(error $(RED)"PROJECT_NAME must be defined in $(SOURCE_DIR)/Makefile"$(RESET))
endif
ifndef VERSION
$(error $(RED)"VERSION must be defined in $(SOURCE_DIR)/Makefile"$(RESET))
endif
ifndef BUILD_TYPE
$(error $(RED)"BUILD_TYPE must be defined in $(SOURCE_DIR)/Makefile"$(RESET))
endif

export PROJECT_NAME	# 工程名称
export VERSION		# 版本号
export BUILD_DIR	# 编译目录
export SOURCE_DIR	# 源码目录
export BINARY_DIR	# 输出目录
export BUILD_TYPE	# 编译类型，缺省为DEBUG，可以是RELEASE

MAKEFLAGS += -rR	# 禁用隐式规则

# 输出工程名
$(MAKECMDGOALS) all: _project_name

_project_name:
	@$(PRINTF) $(GREEN)"---------------------------------------------\n"${RESET}
	@$(PRINTF) ${GREEN}"project: \t\t$(PROJECT_NAME)\n"${RESET}
	@$(PRINTF) ${GREEN}"version: \t\t$(VERSION)\n"${RESET}
	@$(PRINTF) ${GREEN}"build targets: \t$(MAKECMDGOALS)\n"${RESET}
	@$(PRINTF) ${GREEN}"build type: \t\t$(BUILD_TYPE)\n"${RESET}
	@$(PRINTF) ${GREEN}"source directory: \t$(SOURCE_DIR)\n"${RESET}
	@$(PRINTF) ${GREEN}"build directory: \t$(BUILD_DIR)\n"${RESET}
	@$(PRINTF) ${GREEN}"output directory: \t$(BINARY_DIR)\n"${RESET}
	@$(PRINTF) ${GREEN}"c compiler: \t\t$(CC)\n"${RESET}
	@$(PRINTF) ${GREEN}"c++ compiler: \t\t$(CXX)\n"${RESET}
	@$(PRINTF) ${GREEN}"linker: \t\t$(LD)\n"${RESET}
	@$(PRINTF) ${GREEN}"archives: \t\t$(AR)\n"${RESET}
	@$(PRINTF) $(GREEN)"---------------------------------------------\n"${RESET}

endif