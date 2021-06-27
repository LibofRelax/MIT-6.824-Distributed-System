SRC := $(wildcard *.go)
TARGETS := $(subst go,so,$(SRC))

.PHONY: all build

all: build

build: $(TARGETS)

mrfiles = $(wildcard ../mr/*.go)

%.so: %.go $(mrfiles)
	@echo ---- building $< ----
	@go build -buildmode=plugin $<

