#!
# @file subdir.mk
# @brief
# 子目录规则
#
# @author niexw
# @email niexiaowen@uestc.edu.cn
#
ifndef __SUBDIR_MK__
__SUBDIR_MK__ := 1

include $(SOURCE_DIR)/rules/target.mk

# all目标依赖于subdir目标
all: $(SUBDIRS)
# doc目标依赖于subdir目标
doc: $(SUBDIRS)
# clean目标依赖于subdir目标
clean: $(SUBDIRS)
# lint目标依赖于subdir目标
lint: $(SUBDIRS)
# test目标依赖于subdir
test: $(SUBDIRS)

include $(SOURCE_DIR)/rules/variable.mk

# 生成子目录编译规则
$(SUBDIRS):
	@$(MKDIR) $(CURDIR)/$(patsubst %/,%,$@)
	@if [ ! -f $(CURDIR)/$(patsubst %/,%,$@)/Makefile ]; then \
			$(ECHO) "include $(subst $(BUILD_DIR),$(SOURCE_DIR),$(CURDIR)/$(patsubst %/,%,$@))/Makefile" > \
			$(CURDIR)/$(patsubst %/,%,$@)/Makefile; \
		fi
	@$(PRINTF) $(CYAN)"$(SPACES)entering subdirectory: $(CURDIR)/$(patsubst %/,%,$@)\n"$(RESET)
	@$(MAKE) --no-print-directory -C $(CURDIR)/$(patsubst %/,%,$@) $(subst $@,,$(MAKECMDGOALS))

.PHONY: $(SUBDIRS)	# 必须是PHONY规则

endif
