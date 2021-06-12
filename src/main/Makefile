TARGETS := mrmaster mrworker mrsequential
SRC := $(addsuffix .go, $(TARGETS))

.PHONY: all build

all: build

build: $(TARGETS)

%: %.go
	@go build -v $^;
