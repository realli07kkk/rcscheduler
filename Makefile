GO ?= go

.PHONY: build test integration vet fmt linux

build:
	mkdir -p bin
	CGO_ENABLED=0 $(GO) build -trimpath -o bin/rcscheduler ./cmd/rcscheduler

test:
	$(GO) test -race ./...

integration:
	test -n "$(RCSCHEDULER_TEST_RCLONE)"
	RCSCHEDULER_TEST_RCLONE="$(RCSCHEDULER_TEST_RCLONE)" $(GO) test -race ./... -count=1

vet:
	$(GO) vet ./...

fmt:
	gofmt -w cmd internal

linux:
	mkdir -p bin
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -trimpath -o bin/rcscheduler-linux-amd64 ./cmd/rcscheduler
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 $(GO) build -trimpath -o bin/rcscheduler-linux-arm64 ./cmd/rcscheduler
