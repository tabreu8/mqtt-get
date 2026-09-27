VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X github.com/tabreu8/mqtt-get/internal/api.Version=$(VERSION)

.PHONY: build test bench docker run
build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/mqtt-get ./cmd/mqtt-get
test:
	go vet ./... && go test -race ./...
bench:
	go test -run xxx -bench . -benchmem ./...
docker:
	docker build --build-arg VERSION=$(VERSION) -t mqtt-get:$(VERSION) .
run: build
	./bin/mqtt-get
