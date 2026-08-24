GO      ?= go
BIN     := bin/gateway
PKG     := ./cmd/gateway
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
DATE    := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -s -w \
	-X main.version=$(VERSION) \
	-X main.commit=$(COMMIT) \
	-X main.date=$(DATE)

.PHONY: all build build-linux run check test race bench cover lint fmt vet tidy clean docker e2e e2e-ark

all: build

build:
	@mkdir -p bin
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN) $(PKG)
	@echo "built $(BIN) ($(VERSION))"

## 交叉编译 Linux amd64/arm64
build-linux:
	@mkdir -p bin
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o bin/gateway-linux-amd64 $(PKG)
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o bin/gateway-linux-arm64 $(PKG)

run: build
	$(BIN)

## 仅校验配置
check: build
	$(BIN) -check

test:
	@$(GO) test ./...

race:
	$(GO) test -race -count=1 ./...

bench:
	$(GO) test -run '^$$' -bench . -benchmem ./internal/...

cover:
	$(GO) test -coverprofile=coverage.out -covermode=atomic ./...
	$(GO) tool cover -func=coverage.out | tail -1
	@echo "HTML 报告: go tool cover -html=coverage.out"

## 端到端：本地 mock 上游，无需密钥
e2e: build
	./scripts/e2e.sh

## 端到端：真实火山方舟上游，需要 ARK_KEY
e2e-ark: build
	ARK_KEY=$(ARK_KEY) ./scripts/e2e_ark.sh

fmt:
	gofmt -w ./cmd ./internal

vet:
	$(GO) vet ./...

tidy:
	$(GO) mod tidy

lint: fmt vet

docker:
	docker build -t remap-gateway:$(VERSION) .

clean:
	rm -rf bin coverage.out
