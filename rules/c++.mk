##
# @file c++.mk
# @brief
# c/c++编译
#
# @author niexw
# @email niexiaowen@uestc.edu.cn
#
ifndef __C++_MK_
__C++_MK_ := 1

# 编译选项，强制打开尾递归优化，禁止内置优化malloc
C_STANDARD := c11
CXX_STANDARD := c++20
CFLAGS += -std=$(C_STANDARD) -fPIC -Wall -Werror -fchar8_t
CXXFLAGS += -std=$(CXX_STANDARD) -fPIC -Wall -Werror -fchar8_t -fno-rtti -fno-exceptions

include $(SOURCE_DIR)/rules/config_h.mk
include $(SOURCE_DIR)/rules/mimalloc.mk
include $(SOURCE_DIR)/rules/debug.mk

# 编译规则
# .c -> .o
%.o:%.c $(MAKEFILE_LIST)
	@$(if $(dir $@),$(shell $(MKDIR) $(dir $@)))
	@$(PRINTF) $(CYAN)"$(SPACES)compiling c source file: $<\n"$(RESET)
	$(CC) $(if $($*.c_CFLAGS),$($*.c_CFLAGS),$(CFLAGS)) $(if $($*.c_CPPFLAGS),$($*.c_CPPFLAGS),$(CPPFLAGS)) -MT $@ -MMD -MP -MF $*.c.d $(foreach i,$(INCLUDE),-I $(i)) -c $< -o $@

# .cc -> .o
%.o:%.cc $(MAKEFILE_LIST)
	@$(if $(dir $@),$(shell $(MKDIR) $(dir $@)))
	@$(PRINTF) $(CYAN)"$(SPACES)compiling c++ source file: $<\n"$(RESET)
	$(CXX) $(if $($*.cc_CXXFLAGS),$($*.cc_CXXFLAGS),$(CXXFLAGS)) $(if $($*.cc_CPPFLAGS),$($*.cc_CPPFLAGS),$(CPPFLAGS)) -MT $@ -MMD -MP -MF $*.cc.d $(foreach i,$(INCLUDE),-I $(i)) -c $< -o $@

# 包含依赖文件
define _include_depend
	-include $(1)
endef

endif # __C++_MK_
