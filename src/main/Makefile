TARGETS = mrmaster mrworker mrsequential

.PHONY: all build

all: build

build: $(TARGETS)

mrfiles = $(wildcard ../mr/*.go)

%: %.go $(mrfiles)
	@echo ---- building $< ----
	@go build $<
