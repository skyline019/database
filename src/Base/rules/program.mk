##
# @file program.mk
# @brief
# 生成可执行程序规则
#
# @author niexw
# @email niexiaowen@uestc.edu.cn
#
ifndef __PROGRAM_MK__
__PROGRAM_MK__ := 1

include $(SOURCE_DIR)/rules/target.mk
include $(SOURCE_DIR)/rules/c++.mk
include $(SOURCE_DIR)/rules/proto.mk
include $(SOURCE_DIR)/rules/lint.mk
include $(SOURCE_DIR)/rules/debug.mk
include $(SOURCE_DIR)/rules/mimalloc.mk
include $(SOURCE_DIR)/rules/sharedlib.mk
include $(SOURCE_DIR)/rules/staticlib.mk

# 源代码搜索路径，设定为对应的源文件路径
VPATH := $(_corresponding_source_dir)
# 公共测试库
GTEST_LIBS := -lgtest

# 更新对象列表，.o不要加编译路径，隐式规则要求路径完全匹配
$(foreach l,$(PROGRAMS),$(eval $(l)_OBJECTS += $(call _source_to_obj,$($(l)_SOURCES))))

# 分辨出普通程序和测试程序
TEST_PROGRAMS := $(filter test_%,$(PROGRAMS))
ORDINARY_PROGRAMS := $(filter-out $(TEST_PROGRAMS),$(PROGRAMS))

# 设定测试程序的链接库
$(foreach t,$(TEST_PROGRAMS),$(eval $(notdir $(t))_SYSTEM_LIBS += $(GTEST_LIBS)))
# 设定测试程序的编译选项
$(foreach t,$(TEST_PROGRAMS),$(foreach s,$($(t)_SOURCES),$(eval $(if $($(s)_CXXFLAGS),$(s)_CXXFLAGS += -fno-access-control,$(s)_CXXFLAGS := $(CXXFLAGS) -fno-access-control))))

# 缺省目标添加依赖
all: $(addprefix $(BINARY_DIR)/,$(ORDINARY_PROGRAMS)) $(addprefix $(CURDIR)/,$(TEST_PROGRAMS))

# 只lint普通程序
lint: $(foreach l,$(ORDINARY_PROGRAMS),$($(l)_SOURCES:.cc=.lint)) $(foreach l,$(ORDINARY_PROGRAMS),$($(l)_SOURCES:.c=.lint))

# 测试目标
test: $(addprefix $(CURDIR)/,$(TEST_PROGRAMS))
	@for t in $(addprefix $(CURDIR)/,$(TEST_PROGRAMS)); do \
		$(PRINTF) $(CYAN)"$(SPACES)running test: $$t\n"$(RESET); \
		$$t || exit 1; \
	done

# 程序对本地库的依赖
define _local_lib_dependency
$(1)_LOCAL_LIB_DEPS := $(foreach lib,$(patsubst -l%,%,$(filter -l%,$($(1)_LOCAL_LIBS))),\
    $(firstword $(wildcard $(BINARY_DIR)/lib$(lib).so $(BINARY_DIR)/lib$(lib).a)))
endef

# 为每个普通程序创建本地库依赖目标
$(foreach s,$(ORDINARY_PROGRAMS),$(eval $(call _local_lib_dependency,$(s))))

# 设定普通程序目标与对象之间的依赖
define _ordinary_program_to_obj_dependency
$(BINARY_DIR)/$(1): $($(1)_OBJECTS) $(MAKEFILE_LIST) $($(1)_LOCAL_LIB_DEPS)
	@$(MKDIR) $(BINARY_DIR)
	@$(PRINTF) $(CYAN)"$(SPACES)linking executable: $(BINARY_DIR)/$(1)\n"$(RESET)
	$(LD) -Wl,--no-undefined $(if $($(1)_LDFLAGS),$($(1)_LDFLAGS),$(LDFLAGS)) $($(1)_OBJECTS) -Wl,-rpath,$(BINARY_DIR) $($(1)_LOCAL_LIBS) $($(1)_SYSTEM_LIBS) -o $(BINARY_DIR)/$(1)
	$(CHMOD) u+x $(BINARY_DIR)/$(1)
endef
$(foreach s,$(ORDINARY_PROGRAMS),$(eval $(call _ordinary_program_to_obj_dependency,$(s))))

# 包含普通程序依赖文件.d
$(eval $(foreach l,$(ORDINARY_PROGRAMS), \
	$(foreach s,$($(l)_SOURCES),$(call _include_depend,$(s).d))))

# 清理普通程序文件列表
$(foreach l,$(ORDINARY_PROGRAMS),$(eval CLEAN_FILES += $(BINARY_DIR)/$(l) \
	$(foreach s,$($(l)_SOURCES),$(CURDIR)/$(call _source_to_obj,$(s))) \
	$(foreach s,$($(l)_SOURCES),$(CURDIR)/$(call _source_to_mid,$(s)))))

# 为每个测试程序创建本地库依赖目标
$(foreach s,$(TEST_PROGRAMS),$(eval $(call _local_lib_dependency,$(s))))

# 设定测试程序目标与对象之间的依赖
define _test_program_to_obj_dependency
$(CURDIR)/$(1): $($(1)_OBJECTS) $(MAKEFILE_LIST) $($(1)_LOCAL_LIB_DEPS)
	@$(PRINTF) $(CYAN)"$(SPACES)linking test: $(CURDIR)/$(1)\n"$(RESET)
	$(LD) -Wl,--no-undefined $(if $($(1)_LDFLAGS),$($(1)_LDFLAGS),$(LDFLAGS)) $($(1)_OBJECTS) -Wl,-rpath,$(CURDIR) $($(1)_LOCAL_LIBS) $($(1)_SYSTEM_LIBS) -o $(CURDIR)/$(1)
	$(CHMOD) u+x $(CURDIR)/$(1)
endef
$(foreach s,$(TEST_PROGRAMS),$(eval $(call _test_program_to_obj_dependency,$(s))))

# 包含测试程序依赖文件.d
$(eval $(foreach l,$(TEST_PROGRAMS), \
	$(foreach s,$($(l)_SOURCES),$(call _include_depend,$(s).d))))

# 清理测试程序文件列表
$(foreach l,$(TEST_PROGRAMS),$(eval CLEAN_FILES += $(CURDIR)/$(l) \
	$(foreach s,$($(l)_SOURCES),$(CURDIR)/$(call _source_to_obj,$(s))) \
	$(foreach s,$($(l)_SOURCES),$(CURDIR)/$(call _source_to_mid,$(s)))))

endif # __PROGRAM_MK__