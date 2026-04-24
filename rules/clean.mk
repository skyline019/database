#!
# @file clean.mk
# @brief
# 清除规则
#
# @author niexw
# @email niexiaowen@uestc.edu.cn
#
ifndef __CLEAN_MK__
__CLEAN_MK__ := 1

# 所有清除文件定义在CLEAN_FILES变量中，先去重，然后非空执行rm
export CLEAN_FILES
clean:
	@$(ECHO) "cleaning ..."
	$(eval CLEAN_FILES := $(sort $(CLEAN_FILES)))
	@if [ -n "$(strip $(sort $(CLEAN_FILES)))" ]; then \
		$(ECHO) $(RM) $(CLEAN_FILES); \
		$(RM) $(CLEAN_FILES); \
    fi

endif