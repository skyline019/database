#!
# @file lint.mk
# @brief
# lint规则
#
# @author niexw
# @email niexiaowen@uestc.edu.cn
#
ifndef __LINT_MK__
__LINT_MK__ := 1

include $(SOURCE_DIR)/rules/target.mk

# lint选项
export C_LINTFLAGS
export CXX_LINTFLAGS += -std=c++20

# 编译规则
# .c -> .lint
%.lint:%.c $(MAKEFILE_LIST)
	@$(if $(dir $@),$(shell $(MKDIR) $(dir $@)))
	$(LINT) $< -- $(foreach i,$(INCLUDE),-I $(i)) $(C_LINTFLAGS)

# .cc -> .lint
%.lint:%.cc $(MAKEFILE_LIST)
	@$(if $(dir $@),$(shell $(MKDIR) $(dir $@)))
	$(LINT) $< -- $(foreach i,$(INCLUDE),-I $(i)) $(CXX_LINTFLAGS)

endif	# __LINT_MK__