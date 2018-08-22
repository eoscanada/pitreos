
# Git magic!
GIT_VERSION := $(shell git --no-pager describe --tags --always)
GIT_COMMIT  := $(shell git rev-parse --verify HEAD)
GIT_DATE    := $(firstword $(shell git --no-pager show --date=iso-strict --format="%ad" --name-only))
BUILD_DATE  := $(shell date -u '+%Y-%m-%d_%I:%M:%S%p')
# If working tree is dirty, append dirty flag
ifneq ($(strip $(shell git status --porcelain 2>/dev/null)),)
 GIT_VERSION := $(GIT_VERSION)-dirty
endif

# Go parameters
GOCMD=go
GOPKG=github.com/eoscanada/pitreos
LDFLAGS=-X $(GOPKG)/cmd.Version=$(GIT_VERSION) -X $(GOPKG)/cmd.BuildTime=$(BUILD_DATE) -X $(GOPKG)/cmd.BuildHash=$(GIT_COMMIT)  
GOBUILD=$(GOCMD) build -ldflags "$(LDFLAGS)"
GOINSTALL=$(GOCMD) install -ldflags "$(LDFLAGS)"
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
BINARY_NAME=pitreos

all: test install

install:
	$(GOINSTALL) -v
build:
	$(GOBUILD) -v
test:
	$(GOTEST) -v ./...
run:
	$(GOBUILD) -v
	./$(BINARY_NAME)
deps:
	$(GOGET) ./...

