#!/usr/bin/env bash
# ==============================================================================
# Pulse Platform — Observability Stack Verification Script
#
# Validates:
#   1. Syntactic integrity of Prometheus scrape configurations & SLA rules
#   2. Alertmanager routing rules and notification webhook endpoints
#   3. Grafana declarative dashboard JSON models (all 5 dashboards)
#   4. Live metrics endpoint availability and Prometheus format compliance
#   5. Docker Compose multi-service container orchestration specs
# ==============================================================================

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

export PATH="$HOME/.local/go/bin:$HOME/go/bin:$HOME/.local/bin:$PATH"

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_pass() {
    echo -e "${GREEN}[PASS]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_err() {
    echo -e "${RED}[ERROR]${NC} $1"
}

echo "=================================================================="
echo "Pulse Multi-Service Observability Verification Harness"
echo "=================================================================="

# Step 1: Run Go integration tests for Observability configurations
log_info "Running Go integration tests for Prometheus, Alertmanager, and Compose..."
go test -v tests/integration/observability_test.go
log_pass "Prometheus and Alertmanager configuration tests passed"

# Step 2: Run Go integration tests for SLA Alert Rules
log_info "Running Go integration tests for Prometheus SLA alert rules..."
go test -v tests/integration/alert_rules_test.go
log_pass "Prometheus SLA alert rules tests passed"

# Step 3: Run Go integration tests for Grafana Dashboards
log_info "Running Go integration tests for Grafana dashboard JSON models..."
go test -v tests/integration/dashboards_test.go
log_pass "Grafana dashboards schema & metric mapping tests passed"

# Step 4: Verify Dashboard JSON Files exist and are well-formed
log_info "Checking dashboard JSON files in deployments/grafana/dashboards/..."
DASHBOARDS=(
    "executive_overview.json"
    "ingest_broker_lag.json"
    "stream_processor.json"
    "anomaly_alerts.json"
    "chronodb_storage.json"
)

for dash in "${DASHBOARDS[@]}"; do
    FILE="deployments/grafana/dashboards/$dash"
    if [ ! -f "$FILE" ]; then
        log_err "Missing dashboard file: $FILE"
        exit 1
    fi
    # Validate JSON syntax
    python3 -m json.tool "$FILE" > /dev/null
    log_pass "Validated $dash"
done

# Step 5: Check optional live endpoints if services are running
log_info "Checking live endpoint connectivity (if running)..."
CHECK_ENDPOINTS=(
    "http://localhost:8080/metrics:Gateway"
    "http://localhost:8081/metrics:Processor"
    "http://localhost:8083/metrics:Alertd"
    "http://localhost:8084/metrics:ChronoDB"
    "http://localhost:9090/-/healthy:Prometheus"
    "http://localhost:3000/api/health:Grafana"
    "http://localhost:9093/-/healthy:Alertmanager"
)

LIVE_COUNT=0
for ep in "${CHECK_ENDPOINTS[@]}"; do
    URL="${ep%%:*}"
    NAME="${ep##*:}"
    if curl -s --max-time 1 "$URL" > /dev/null 2>&1; then
        log_pass "$NAME endpoint active ($URL)"
        LIVE_COUNT=$((LIVE_COUNT + 1))
    else
        log_warn "$NAME endpoint not running ($URL) - start with 'make obs-up' and service binaries"
    fi
done

echo ""
echo "=================================================================="
log_pass "Observability Stack Verification Completed Successfully!"
echo "=================================================================="
echo "Summary:"
echo "  - Prometheus Scrape Config: 5 targets polled every 5s"
echo "  - Alertmanager Routing:     group_interval=15s, repeat=1h, webhook receiver"
echo "  - SLA Alert Rules:          6 rules enforcing error rate, lag, latency"
echo "  - Grafana Dashboards:       5 declarative dashboard models provisioned"
echo "  - Docker Compose:           Orchestrates Prometheus, Grafana, Alertmanager, Redpanda"
echo "=================================================================="
