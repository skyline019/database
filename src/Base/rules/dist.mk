#!
# @file dist.mk
# @brief
# 发布打包
#
# @author niexw
# @email niexiaowen@uestc.edu.cn
#
ifndef __DIST_MK__
export __DIST_MK__ := 1

# 打包的文件和目录
EXCLUDE_DIRS := build
ALL_FILE_AND_DIR := $(patsubst $(SOURCE_DIR)/%,%,$(wildcard $(SOURCE_DIR)/*))
FILTERED_FILE_AND_DIR := $(filter-out $(EXCLUDE_DIRS), $(ALL_FILE_AND_DIR))

dist:
	@$(ECHO) "making distribution ..."
	tar zcfv $(BUILD_DIR)/$(PROJECT_NAME)-$(VERSION).tar.gz \
		-C $(SOURCE_DIR) $(FILTERED_FILE_AND_DIR)

CLEAN_FILES += $(BUILD_DIR)/$(PROJECT_NAME)-$(VERSION).tar.gz

endif