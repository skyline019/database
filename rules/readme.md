# rules

why Makefile? why not cmake?

项目使用Makefile来组织管理，不使用cmake，原因是Makefile直接使用shell命令，对编译过程掌控更精确。Makefile当然有它的问题，它出现的比较早，最初定义Makefile语法时，没想过项目组织会进化到相当复杂的程度。

Makefile最初的语法只是简单的依赖、命令，根据要编译的目标，调用shell命令即可。它的语法甚至不包括函数、分支、循环这些基本的控制结构，从语言的发展来看，这都是非常致命的。另一个例子是latex，高德纳也没想到过印一本书会有这么多的需求。

实际上lisp就足够简单足够好，lisp扩展性极佳，当然它的括号魔法容易招致批评，但它的语法设计很优秀。

rules目录放置各种Makefile规则，这种设计模仿了FreeBSD的规则组织方式。

## 根目录Makefile

根目录的Makefile如下，要暴露几个全局变量，指定编译子目录，然后要求源码与编译目录分离。

```Makefile
# 工程名
export PROJECT_NAME := coir
export VERSION := 0.1

# 编译类型：DEBUG|RELEASE
export BUILD_TYPE := DEBUG

# 子目录
SUBDIRS := coir/

# 编译目录与源目录分离
include $(realpath $(dir $(lastword $(MAKEFILE_LIST))))/rules/seperate.mk
```

注意，为了包含seperate.mk，使用realpath获得路径，不能直接使用../来替代，因为在分离规则中，源码根目录的Makefile在编译目录被包含。

## seperate.mk

分离编译规则，将源码目录与编译目录分开，目的是防止中间文件污染目录。

```Makefile
ifndef __SEPERATE_MK__
export __SEPERATE_MK__ := 1	# 全局编译目录，包括递归编译
...
endif
```

Makefile规则文件都使用类似c头文件方式保护。__SEPERATE_MK__变量被export，意思是这个文件只能被包含一次。大多数规则都是在子目录编译时递归使用，不要export。

```Makefile
export BUILD_DIR := $(realpath $(CURDIR))
export SOURCE_DIR := $(realpath $(dir $(lastword $(MAKEFILE_LIST)))/..)
export BINARY_DIR := $(BUILD_DIR)/bin

include $(SOURCE_DIR)/rules/variable.mk
include $(SOURCE_DIR)/rules/project.mk
```

seperate.mk先抽取项目全局变量，分别是编译目录、源码目录、二进制制品输出目录。然后包含variable.mk和project.mk规则，variable.mk中定义了CC、AR这样的变量，project.mk输出项目编译信息。

```Makefile
# 判断编译目录与源码目录是否相同
ifeq "$(SOURCE_DIR)" "$(BUILD_DIR)"

# 禁止在源码目录下编译
WARNING1 = $(shell printf $(RED)"===禁止在源码目录下编译==="$(RESET))
WARNING2 = $(shell printf $(RED)"> cd build"$(RESET))
WARNING3 = $(shell printf $(RED)"> make -f ../Makefile"$(RESET))
$(info $(WARNING1))
$(info $(WARNING2))
$(info $(WARNING3))
$(error )

else

# 所有目标依赖于build-dir目标
$(MAKECMDGOALS) all: _seperate_build

# 在编译目录下建立一个Makefile文件
_seperate_build:
	@if [ ! -f $(BUILD_DIR)/Makefile ]; then \
		$(ECHO) "include $(SOURCE_DIR)/Makefile" > \
			$(BUILD_DIR)/Makefile; \
	fi
	@if [ ! -e $(BUILD_DIR)/bin ]; then \
		$(MKDIR) $(BUILD_DIR)/bin; \
	fi

# 指定缺省目标
include $(SOURCE_DIR)/rules/target.mk

endif	# "$(SOURCE_DIR)" "$(BUILD_DIR)"
```

seperate.mk通过比较SOURCE_DIR与BUILD_DIR两个变量是否相同来判断是否是分离编译。如果相同，就输出警告信息，提示用户不要在源码目录下编译。

在分离编译情况下，会在编译目录下创建一个Makefile，让它include源码根目录的Makefile。

## target.mk

target.mk定义了一些PHONY目标，如all、clean、test、doc等。

```Makefile
# 一些方便函数
_add_prefix = $(addprefix $(1),$(2))
_corresponding_source_dir = $(subst $(BUILD_DIR),$(SOURCE_DIR),$(CURDIR))
_add_source_dir_prefix = $(call _add_prefix,$(_corresponding_source_dir)/,$(1))
_add_build_dir_prefix = $(call _add_prefix,$(CURDIR)/,$(1))

# SOURCES列表到对象文件的转换函数
OBJ_EXT_PAIRS = .proto:.pb.o .c:.o .cc:.o
_source_to_obj = $(strip $(foreach pair,$(OBJ_EXT_PAIRS), \
       $(patsubst %$(firstword $(subst :, ,$(pair))), \
               %$(lastword $(subst :, ,$(pair))), \
               $(filter %$(firstword $(subst :, ,$(pair))),$(1)))))

# SOURCES列表到其它中间文件的转换函数
MID_EXT_PAIRS = .proto:.pb.h .proto:.pb.cc .proto:.pb.cc.d .c:.c.d .cc:.cc.d
_source_to_mid = $(strip $(foreach pair,$(MID_EXT_PAIRS), \
       $(patsubst %$(firstword $(subst :, ,$(pair))), \
               %$(lastword $(subst :, ,$(pair))), \
               $(filter %$(firstword $(subst :, ,$(pair))),$(1)))))

# SOURCES列表到源文件的转换函数
_source_to_src = $(call _add_source_dir_prefix,$(1))
```

target.mk里定义了其它规则会使用的一些宏，添加源码目录前缀、添加编译目录前缀、将SOURCES列表转换为对象文件列表、将SOURCES列表转换为其它中间文件列表、将SOURCES列表转换为源文件列表。

这些规则放在target.mk是找不到地方放了。

## subdir.mk

subdir.mk定义了递归编译子目录的规则，SUBDIRS变量存放当前目录需要递归编译的子目录名字，注意子目录名后必须带上/，例如src/、doc/等。

子目录之间可以相互依赖，例如test/可以依赖src/，因为测试目录可能会链接src/目录下生成的静态链接库或动态链接库。

```Makefile
# 生成子目录编译规则
$(SUBDIRS):
	@$(MKDIR) $(CURDIR)/$(patsubst %/,%,$@)
	@if [ ! -f $(CURDIR)/$(patsubst %/,%,$@)/Makefile ]; then \
			$(ECHO) "include $(subst $(BUILD_DIR),$(SOURCE_DIR),$(CURDIR)/$(patsubst %/,%,$@))/Makefile" > \
			$(CURDIR)/$(patsubst %/,%,$@)/Makefile; \
		fi
	@$(PRINTF) $(CYAN)"$(SPACES)entering subdirectory: $(CURDIR)/$(patsubst %/,%,$@)\n"$(RESET)
	@$(MAKE) --no-print-directory -C $(CURDIR)/$(patsubst %/,%,$@) $(subst $@,,$(MAKECMDGOALS))
```

subdir.mk会在编译目录下对等构建编译目录，然后创建一个Makefile包含源码侧的Makefile，最后沿着编译目录递归构建。

## 编译输出

编译过程中，有一些信息要输出，为了好看在variable.mk中定义了一些格式相关的变量。SPACES变量控制缩进空格数目，每次进入一个子目录会缩进2个空格。

```Makefile
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
```

## debug.mk

debug.mk控制BUILD_TYPE在DEBUG|RELEASE两种编译类型下的编译，它包含了sanitize.mk用于DEBUG编译模式。在DEBUG编译模式，为了使用尾折叠优化，使用-Os选项。

-flto选项用于RELEASE模式，用于优化模块间的调用。

```Makefile
# -Os是-O2的子集，-Os会打开-foptimize-sibling-calls尾折叠选项
ifeq "$(BUILD_TYPE)" "DEBUG"
CFLAGS += -Os -g -DDEBUG -fno-omit-frame-pointer $(SANITIZE_COMPILE_FLAGS)
CXXFLAGS += -Os -g -DDEBUG -fno-omit-frame-pointer $(SANITIZE_COMPILE_FLAGS)
# 添加链接选项
LDFLAGS += -fno-omit-frame-pointer $(SANITIZE_COMPILE_FLAGS) $(SANITIZE_LINK_FLAGS)
endif
ifeq "$(BUILD_TYPE)" "RELEASE"
CFLAGS += -O2
CXXFLAGS += -O2
LDFLAGS += -Wno-unused-function -flto
endif
```

## mimalloc.mk

mimalloc是一个内存分配库，性能非常优异，并且库很小，项目强制使用mimalloc替代了c库中的内存分配函数。

```Makefile
CFLAGS += -fno-builtin-malloc
CXXFLAGS += -fno-builtin-malloc

# mimalloc库
LDFLAGS += -lmimalloc
```

要求源码不使用内置malloc编译，并且链接到mimalloc库。

## config_h.mk

config_h.mk用于包含项目配置头文件，例如coir/config.h，config.h中定义了一些项目相关的常量，例如项目名称、版本号等。

注意，**config_h.mk规定源码根目录下必须有一个PROJECT_NAME子目录，config.h放在该子目录下**。

```Makefile
CONFIG_H := $(SOURCE_DIR)/$(PROJECT_NAME)/config.h

CFLAGS += -include $(CONFIG_H)
CXXFLAGS += -include $(CONFIG_H)
```

## program.mk

编译可执行程序使用program.mk规则，它首先指定LDFLAGS链接规则，然后指定源码搜索路径VPATH。

```Makefile
# 编译选项
LDFLAGS := -Wl,--no-undefined

# 源代码搜索路径，设定为对应的源文件路径
VPATH := $(_corresponding_source_dir)
```

可执行程序名称放在PROGRAMS变量中，每个可执行程序由一个或多个源码编译，这需要用户在Makefile中指定，例如一个test_add可执行文件，它的源码列表放在test_add_SOURCES变量中，下面的宏生成test_add_OBJECTS对象文件列表。

```Makefile
# 更新对象列表，.o不要加编译路径，隐式规则要求路径完全匹配
$(foreach l,$(PROGRAMS),$(eval $(l)_OBJECTS += $(call _source_to_obj,$($(l)_SOURCES))))
```

可执行程序只是一个名称，编译出的二进制制品放在BINARY_DIR目录下。all目标依赖于所有可执行程序，lint目标则依赖于所有目标的源文件。

```Makefile
# 缺省目标添加依赖
all: $(addprefix $(BINARY_DIR)/,$(PROGRAMS))

# lint
lint: $(foreach l,$(PROGRAMS),$($(l)_SOURCES:.cc=.lint)) $(foreach l,$(PROGRAMS),$($(l)_SOURCES:.c=.lint))
```

每个BINARY_DIR目录下的可执行文件依赖于该文件的对象文件，以及Makefile列表。要编译可执行文件，需要先编译出对象文件。可执行文件只需要链接即可，LD变量并非ld，而是gcc，直接使用ld很麻烦，gcc集成了前端和后端，要方便许多。

```Makefile
# 设定目标与对象之间的依赖
define _program_to_obj_dependency
$(BINARY_DIR)/$(1): $($(1)_OBJECTS) $(MAKEFILE_LIST)
	@$(MKDIR) $(BINARY_DIR)
	@$(PRINTF) $(CYAN)"$(SPACES)building executable: $(BINARY_DIR)/$(1)\n"$(RESET)
	$(LD) $(if $($(1)_LDFLAGS),$($(1)_LDFLAGS),$(LDFLAGS)) $($(1)_OBJECTS) -Wl,-rpath,$(BINARY_DIR) $($(1)_LIBS) -o $(BINARY_DIR)/$(1)
	$(CHMOD) u+x $(BINARY_DIR)/$(1)
endef
$(foreach s,$(PROGRAMS),$(eval $(call _program_to_obj_dependency,$(s))))

# 包含依赖文件.d
$(eval $(foreach l,$(PROGRAMS), \
	$(foreach s,$($(l)_SOURCES),$(call _include_depend,$(s).d))))
```

下面的代码将对象文件，可执行文件制品都放入CLEAN_FILES变量中。

```Makefile
# 清理文件列表
$(foreach l,$(PROGRAMS),$(eval CLEAN_FILES += $(BINARY_DIR)/$(l) \
	$(foreach s,$($(l)_SOURCES),$(CURDIR)/$(call _source_to_obj,$(s))) \
	$(foreach s,$($(l)_SOURCES),$(CURDIR)/$(call _source_to_mid,$(s)))))
```

## sharedlib.mk

与program.mk类似，LDFLAGS链接选项定义为shared。

```Makefile
# 编译器会封装ld链接器，为便于移植，不要使用ld
LDFLAGS := -shared
```

## staticlib.mk

静态库并不需要链接，而是使用AR工具打包，规则与program.mk类似。

## c++.mk

c++.mk定义了C++编译选项，文件的开头主要定义CFLAGS和CXXFLAGS变量。

```Makefile
# 编译选项，强制打开尾递归优化，禁止内置优化malloc
C_STANDARD := c11
CXX_STANDARD := c++20
CFLAGS += -std=$(C_STANDARD) -fPIC -Wall -Werror -fchar8_t
CXXFLAGS += -std=$(CXX_STANDARD) -fPIC -Wall -Werror -fchar8_t -fno-rtti -fno-exceptions

include $(SOURCE_DIR)/rules/config_h.mk
include $(SOURCE_DIR)/rules/mimalloc.mk
include $(SOURCE_DIR)/rules/debug.mk
```

然后是c++文件的编译规则，创建目录，将对象文件添加到CLEAN_FILES变量中，然后编译。CXXFLAGS是编译选项，CPPFLAGS是预处理选项，INCLUDE是头文件搜索路径。

```Makefile
# 编译规则
# .c -> .o
%.o:%.c $(MAKEFILE_LIST)
	@$(if $(dir $@),$(shell $(MKDIR) $(dir $@)))
	$(eval CLEAN_FILES += $@)
	$(CC) $(if $($*.c_CFLAGS),$($*.c_CFLAGS),$(CFLAGS)) $(if $($*.c_CPPFLAGS),$($*.c_CPPFLAGS),$(CPPFLAGS)) -MT $@ -MMD -MP -MF $*.c.d $(foreach i,$(INCLUDE),-I $(i)) -c $< -o $@

# .cc -> .o
%.o:%.cc $(MAKEFILE_LIST)
	@$(if $(dir $@),$(shell $(MKDIR) $(dir $@)))
	$(CXX) $(if $($*.cc_CXXFLAGS),$($*.cc_CXXFLAGS),$(CXXFLAGS)) $(if $($*.cc_CPPFLAGS),$($*.cc_CPPFLAGS),$(CPPFLAGS)) -MT $@ -MMD -MP -MF $*.cc.d $(foreach i,$(INCLUDE),-I $(i)) -c $< -o $@
```

## proto.mk

proto.mk定义了protobuf编译选项，proto文件编译后会生成.pb.h和.pb.cc文件。

```Makefile
# proto规则
# protoc不接受绝对路径，这里用$(dir $<)包含
%.pb.cc %.pb.h:%.proto $(MAKEFILE_LIST)
	@$(if $(dir $@),$(shell $(MKDIR) $(dir $@)))
	$(PROTOC) -I=. -I=$(dir $<) $($*.proto_PROTOC_FLAGS) --cpp_out=. $<
```

.pb.h文件生成在编译目录下，编译的时候需要包含头文件搜索路径，或者先将.pb.h文件拷贝到include目录下。

```Makefile
# 自动拷贝devlog.pb.h到include目录
$(OUTPUT_DIR)/libdevlog.a: $(SOURCE_DIR)/include/cane/devlog/devlog.pb.h

$(SOURCE_DIR)/include/cane/devlog/devlog.pb.h: devlog.proto
	@$(PRINTF) "copying devlog.pb.h to $(SOURCE_DIR)/include/cane/devlog/\n"
	@$(CP) -f devlog.pb.h $@
```

## latex.mk

latex.mk编译pdf文档，一般在Windows的cygwin环境中编译，在Linux下编译需要先安装texlive，然后安装各种字库。

