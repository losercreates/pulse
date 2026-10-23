.PHONY: proto up down test bench build run clean

GO ?= go
PROTOC ?= protoc

proto:
	@mkdir -p proto/v1
	$(PROTOC) --proto_path=specs/001-telemetry-ingestion/contracts \
		--go_out=proto/v1 --go_opt=paths=source_relative \
		--go-grpc_out=proto/v1 --go-grpc_opt=paths=source_relative \
		specs/001-telemetry-ingestion/contracts/telemetry.proto
	$(PROTOC) --proto_path=specs/001-telemetry-ingestion/contracts \
		--proto_path=specs/002-stream-processor-core/contracts \
		--go_out=proto/v1 --go_opt=paths=source_relative \
		specs/002-stream-processor-core/contracts/aggregates.proto
	$(PROTOC) --proto_path=specs/001-telemetry-ingestion/contracts \
		--proto_path=specs/003-anomaly-detection-alerts/contracts \
		--go_out=proto/v1 --go_opt=paths=source_relative \
		specs/003-anomaly-detection-alerts/contracts/alerts.proto
	$(PROTOC) --proto_path=specs/001-telemetry-ingestion/contracts \
		--proto_path=specs/004-chronodb-storage-query/contracts \
		--go_out=proto/v1 --go_opt=paths=source_relative \
		--go-grpc_out=proto/v1 --go-grpc_opt=paths=source_relative \
		specs/004-chronodb-storage-query/contracts/chronodb.proto
	@echo "Protobuf code generation complete in proto/v1"

up:
	docker compose -f deployments/docker-compose.yml up -d

down:
	docker compose -f deployments/docker-compose.yml down

build: proto
	$(GO) build -o bin/gateway ./cmd/gateway
	$(GO) build -o bin/processor ./cmd/processor
	$(GO) build -o bin/alertd ./cmd/alertd
	$(GO) build -o bin/chronod ./cmd/chronod

run: build
	./bin/gateway

test:
	$(GO) test -v -race ./internal/...

bench:
	$(GO) test -bench=. -benchmem ./tests/bench/...

clean:
	rm -rf bin/
