#!
# @file proto.mk
# @brief
# protobuf编译规则
#
# @author niexw
# @email niexiaowen@uestc.edu.cn
#
ifndef __PROTO_MK__
__PROTO_MK__ := 1

# proto规则
# protoc不接受绝对路径，这里用$(dir $<)包含
%.pb.cc %.pb.h:%.proto $(MAKEFILE_LIST)
	@$(if $(dir $@),$(shell $(MKDIR) $(dir $@)))
	@$(PRINTF) $(CYAN)"$(SPACES)compiling proto file: $<\n"$(RESET)
	$(PROTOC) -I=. -I=$(dir $<) $($*.proto_PROTOC_FLAGS) --cpp_out=. $<

endif # __PROTO_MK__