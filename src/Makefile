.PHONY: all clean lab1


NO_CLEAN_FILE_EXT := go txt sh


all:


CLEAN_SUBDIRS := $(wildcard */)
clean: $(CLEAN_SUBDIRS)


WILDCARD_FILENAME := $(addprefix *., $(NO_CLEAN_FILE_EXT))
FIND_NAME_ARG := $(addprefix ! -name , $(patsubst %, '%' , $(WILDCARD_FILENAME)))

.PHONY: $(CLEAN_SUBDIRS)
$(CLEAN_SUBDIRS):
	@cd $@; find . -type f $(FIND_NAME_ARG) -delete; cd ..;


lab1: mrapps mrmain

mrapps:
	@$(MAKE) -C mrapps

mrmain:
	@$(MAKE) -C main

test-lab1: lab1
	@cd main && ./test-mr.sh