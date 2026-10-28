.PHONY: proto up down obs-up obs-down dashboards test bench build run clean e2e sim docker-build helm-lint helm-template k8s-verify

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

obs-up:
	docker compose -f deployments/docker-compose.yml up -d prometheus grafana alertmanager redpanda redpanda-init redpanda-console

obs-down:
	docker compose -f deployments/docker-compose.yml stop prometheus grafana alertmanager

dashboards:
	@echo "=================================================================="
	@echo "Pulse Observability Stack & Grafana Dashboards"
	@echo "=================================================================="
	@echo "Grafana UI:             http://localhost:3000 (admin/admin)"
	@echo "Prometheus UI:          http://localhost:9090"
	@echo "Alertmanager UI:        http://localhost:9093"
	@echo "Redpanda Console:       http://localhost:8088"
	@echo "------------------------------------------------------------------"
	@echo "Executive Overview:     http://localhost:3000/d/pulse-executive-overview"
	@echo "Gateway & Broker Lag:   http://localhost:3000/d/pulse-ingest-broker-lag"
	@echo "Stream Processor:       http://localhost:3000/d/pulse-stream-processor"
	@echo "Anomaly Alerts:         http://localhost:3000/d/pulse-anomaly-alerts"
	@echo "ChronoDB Storage:       http://localhost:3000/d/pulse-chronodb-storage"
	@echo "=================================================================="

build: proto
	$(GO) build -o bin/gateway ./cmd/gateway
	$(GO) build -o bin/processor ./cmd/processor
	$(GO) build -o bin/alertd ./cmd/alertd
	$(GO) build -o bin/chronod ./cmd/chronod
	$(GO) build -o bin/simulator ./cmd/simulator

run: build
	./bin/gateway

sim: build
	./bin/simulator

docker-build:
	docker build -t pulse-gateway:latest -f deployments/docker/Dockerfile.gateway .
	docker build -t pulse-processor:latest -f deployments/docker/Dockerfile.processor .
	docker build -t pulse-alertd:latest -f deployments/docker/Dockerfile.alertd .
	docker build -t pulse-chronod:latest -f deployments/docker/Dockerfile.chronod .
	docker build -t pulse-simulator:latest -f deployments/docker/Dockerfile.simulator .

helm-lint:
	helm lint deployments/helm/pulse

helm-template:
	helm template pulse deployments/helm/pulse

k8s-verify:
	./scripts/verify_packaging.sh

test:
	$(GO) test -v -race ./internal/...

storage-test:
	$(GO) test -v -race ./internal/storage/... ./tests/integration/durable_storage_test.go

bench:
	$(GO) test -bench=. -benchmem ./tests/bench/...

e2e: build
	./scripts/e2e_pipeline.sh

clean:
	rm -rf bin/ data/
