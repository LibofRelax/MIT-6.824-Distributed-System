SRC := $(wildcard *.go)
TARGETS := $(subst go,so,$(SRC))

.PHONY: all build

all: build

build: $(TARGETS)

%.so: %.go
	@echo ---- building $^ ----
	@go build -buildmode=plugin $^

