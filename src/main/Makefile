TARGETS = mrmaster mrworker mrsequential

.PHONY: all build

all: build

build: $(TARGETS)

%: %.go
	@echo ---- building $^ ----
	@go build $^
