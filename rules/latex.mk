#!
# @file latex.mk
# @brief
# latex编译规则
#
# @author niexw
# @email niexiaowen@uestc.edu.cn
#
ifndef __LATEX_MK__
__LATEX_MK__ := 1

include $(SOURCE_DIR)/rules/target.mk

# latex程序与参数
LATEX := xelatex
BIBER := biber
DVIPDF := xdvipdfmx
LATEXMK := latexmk

# 输出目录与源文件目录
PDF_OUTPUT_DIR := $(CURDIR)
LATEX_SOURCE_DIR := $(call _corresponding_in_source_dir,$(CURDIR))
LATEX_OPTS := -synctex=1 -interaction=nonstopmode -file-line-error -recorder -output-directory="$(PDF_OUTPUT_DIR)"
BIBER_OPTS := --input-directory "$(LATEX_SOURCE_DIR)"
DVIPDF_OPTS := -E -o

# latexmk 配置选项
LATEXMK_OPTS := \
    -xelatex \
    -output-directory="$(PDF_OUTPUT_DIR)" \
    -synctex=1 \
    -interaction=nonstopmode \
    -file-line-error \
    -recorder \
    -use-make

# 输出PDF文件列表
OUTPUT_PDFS := $(call _add_build_dir_prefix,$(PDFS))

# 根据.fls文件生成PDF的输入文件依赖
define _pdf_input_dependencies
$(shell grep '^INPUT ' $(patsubst %.pdf,%.fls,$1) | cut -d' ' -f2 | \
    grep -E '\.(tex|bib|png|jpg|jpeg|pdf|eps|svg|bbl|bst|cls|sty|cfg)$$' | \
    sed 's|^\./||' | \
    while read file; do \
        if [ -f "$(LATEX_SOURCE_DIR)/$$file" ]; then \
            echo "$(LATEX_SOURCE_DIR)/$$file"; \
        elif [ -f "$$file" ]; then \
            echo "$$file"; \
        fi; \
    done)
endef

# 设定目标与对象之间的依赖
define _pdf_to_latex_dependency
$1:$(call _corresponding_in_source_dir,$(patsubst %.pdf,%.tex,$1)) $(call _pdf_input_dependencies,$1)
	@$(MKDIR) $(BUILD_DIR)/pdfs
	@$(CD) $(LATEX_SOURCE_DIR) && $(LATEXMK) $(LATEXMK_OPTS) "$(LATEX_SOURCE_DIR)/$(patsubst %.pdf,%.tex,$(notdir $1))"
	@$(CP) -f $(patsubst %.tex,%.pdf,$1) $(BUILD_DIR)/pdfs/$(notdir $(patsubst %.tex,%.pdf,$1))
endef
$(foreach s,$(OUTPUT_PDFS),$(eval $(call _pdf_to_latex_dependency,$(s))))

# 缺省目标添加依赖
all: $(OUTPUT_PDFS)
doc: $(OUTPUT_PDFS)

CLEAN_FILES += $(filter-out $(addprefix $(CURDIR)/,Makefile),$(wildcard $(CURDIR)/*)) \
    $(addprefix $(BUILD_DIR)/pdfs/,$(notdir $(OUTPUT_PDFS)))

endif