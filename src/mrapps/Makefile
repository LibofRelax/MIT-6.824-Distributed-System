SRC := $(wildcard *.go)
TARGETS := $(subst go,so,$(SRC))

.PHONY: all build

all: build

build: $(TARGETS)

%.so: %.go
	@go build -v -buildmode=plugin $^

