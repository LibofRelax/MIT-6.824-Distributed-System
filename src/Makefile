.PHONY: all clean lab1 mrapps mrmain


NO_CLEAN_FILE_EXT := go txt sh
NO_CLEAN_FILE := Makefile


all:


CLEAN_SUBDIRS := $(wildcard */)
clean: $(CLEAN_SUBDIRS)


WILDCARD_FILENAME := $(addprefix *., $(NO_CLEAN_FILE_EXT))
FIND_NAME_ARG := $(addprefix ! -name , $(patsubst %, '%' , $(WILDCARD_FILENAME))) $(addprefix ! -name , $(NO_CLEAN_FILE))

.PHONY: $(CLEAN_SUBDIRS)
$(CLEAN_SUBDIRS):
	@cd $@
	@find . -type f $(FIND_NAME_ARG) -delete
	@cd ..


lab1: mrapps mrmain

mrapps:
	@$(MAKE) -C mrapps

mrmain:
	@$(MAKE) -C main

test-lab1: lab1
	@cd main && ./test-mr.sh