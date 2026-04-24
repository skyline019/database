#!
# @file config_h.mk
# @brief
# 项目配置头文件
#
# @author niexw
# @email niexiaowen@uestc.edu.cn
#
ifndef __CONFIG_H_MK__
__CONFIG_H_MK__ := 1	# 全局配置头文件规则

include $(SOURCE_DIR)/rules/project.mk

CONFIG_H := $(SOURCE_DIR)/$(PROJECT_NAME)/config.h

CFLAGS += -include $(CONFIG_H)
CXXFLAGS += -include $(CONFIG_H)

endif	# __CONFIG_H_MK__