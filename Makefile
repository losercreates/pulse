.PHONY: proto up down test bench build run clean

GO ?= go
PROTOC ?= protoc

proto:
	@mkdir -p proto/v1
	$(PROTOC) --proto_path=specs/001-telemetry-ingestion/contracts \
		--go_out=proto/v1 --go_opt=paths=source_relative \
		--go-grpc_out=proto/v1 --go-grpc_opt=paths=source_relative \
		specs/001-telemetry-ingestion/contracts/telemetry.proto
	@echo "Protobuf code generation complete in proto/v1"

up:
	docker compose -f deployments/docker-compose.yml up -d

down:
	docker compose -f deployments/docker-compose.yml down

build: proto
	$(GO) build -o bin/gateway ./cmd/gateway

run: build
	./bin/gateway

test:
	$(GO) test -v -race ./internal/...

bench:
	$(GO) test -bench=. -benchmem ./tests/bench/...

clean:
	rm -rf bin/
