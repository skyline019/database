##
# @file staticlib.mk
# @brief
# 静态库规则
#
# @author niexw
# @email niexiaowen@uestc.edu.cn
#
ifndef __STATICLIB_MK__
__STATICLIB_MK__ := 1

include $(SOURCE_DIR)/rules/target.mk
include $(SOURCE_DIR)/rules/c++.mk
include $(SOURCE_DIR)/rules/proto.mk
include $(SOURCE_DIR)/rules/lint.mk
include $(SOURCE_DIR)/rules/debug.mk
include $(SOURCE_DIR)/rules/mimalloc.mk

# 静态库编译选项
ARFLAGS := -rcs

# 源代码搜索路径，设定为对应的源文件路径
VPATH := $(_corresponding_source_dir)

# 更新对象列表，.o不要加编译路径，隐式规则要求路径完全匹配
$(foreach l,$(STATIC_LIBS),$(eval $(l)_OBJECTS += $(call _source_to_obj,$($(l)_SOURCES))))

# 缺省目标添加依赖
all: $(addprefix $(BINARY_DIR)/,$(STATIC_LIBS))

# lint
lint: $(foreach l,$(STATIC_LIBS),$($(l)_SOURCES:.cc=.lint)) $(foreach l,$(STATIC_LIBS),$($(l)_SOURCES:.c=.lint))

# 设定目标与对象之间的依赖
define _static_lib_to_obj_dependency
$(BINARY_DIR)/$(1): $($(1)_OBJECTS) $(MAKEFILE_LIST)
	@$(MKDIR) $(BINARY_DIR)
	@$(PRINTF) $(CYAN)"$(SPACES)linking static library: $@\n"$(RESET)
	@$(RM) $(BINARY_DIR)/$(1)
	$(AR) $(if $($(1)_ARFLAGS),$($(1)_ARFLAGS),$(ARFLAGS)) $(BINARY_DIR)/$(1) $($(1)_LIBS) $($(1)_OBJECTS)
endef
$(foreach s,$(STATIC_LIBS),$(eval $(call _static_lib_to_obj_dependency,$(s))))

# 包含依赖文件.d
$(eval $(foreach l,$(STATIC_LIBS), \
	$(foreach s,$($(l)_SOURCES),$(call _include_depend,$(s).d))))

# 清理文件列表
$(foreach l,$(STATIC_LIBS),$(eval CLEAN_FILES += $(BINARY_DIR)/$(l) \
	$(foreach s,$($(l)_SOURCES),$(CURDIR)/$(call _source_to_obj,$(s))) \
	$(foreach s,$($(l)_SOURCES),$(CURDIR)/$(call _source_to_mid,$(s)))))


endif # __STATICLIB_MK__