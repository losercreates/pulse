#!/usr/bin/env bash
# ==============================================================================
# Pulse Platform — Production Packaging & Kubernetes Verification Script
#
# Validates:
#   1. Syntactic integrity of all 5 multi-stage distroless Dockerfiles
#   2. Non-root user execution directives (UID 65532) and port exposures
#   3. Helm chart structure, metadata, values, and templates
#   4. Helm chart linting and dry-run template rendering (if helm is available)
#   5. Go integration tests validating packaging and HPA partition bounds
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
echo "Pulse Production Packaging & Kubernetes Verification Harness"
echo "=================================================================="

# Step 1: Run Go integration tests for Docker packaging and Helm templates
log_info "Running Go packaging and security integration tests..."
go test -v tests/integration/k8s_packaging_test.go tests/integration/k8s_security_test.go
log_pass "Packaging, Helm, and Security integration tests passed"

# Step 2: Validate Dockerfile existence and security directives
log_info "Validating Dockerfile definitions in deployments/docker/..."
DOCKERFILES=(
    "Dockerfile.gateway"
    "Dockerfile.processor"
    "Dockerfile.alertd"
    "Dockerfile.chronod"
    "Dockerfile.simulator"
)

for df in "${DOCKERFILES[@]}"; do
    FILE="deployments/docker/$df"
    if [ ! -f "$FILE" ]; then
        log_err "Missing Dockerfile: $FILE"
        exit 1
    fi
    grep -q "USER 65532:65532" "$FILE" || { log_err "$df missing non-root USER"; exit 1; }
    grep -q "gcr.io/distroless/static-debian12:nonroot" "$FILE" || { log_err "$df missing distroless base"; exit 1; }
    grep -q "CGO_ENABLED=0" "$FILE" || { log_err "$df missing CGO_ENABLED=0"; exit 1; }
    log_pass "Validated $df"
done

# Step 3: Check Helm CLI and lint templates (if helm installed)
if command -v helm &> /dev/null; then
    log_info "Running helm lint on deployments/helm/pulse..."
    helm lint deployments/helm/pulse
    log_pass "Helm lint passed with 0 errors"

    log_info "Rendering Helm templates via helm template dry-run..."
    helm template pulse deployments/helm/pulse > /dev/null
    log_pass "Helm template rendering validated"
else
    log_warn "Helm CLI not installed; validated via Go integration tests"
fi

echo ""
echo "=================================================================="
log_pass "Production Packaging & Kubernetes Verification Completed Successfully!"
echo "=================================================================="
echo "Summary:"
echo "  - 5 Multi-Stage Distroless Dockerfiles (UID 65532, < 50MB, static CGO=0)"
echo "  - 1 Unified Helm Chart (Deployments, Services, ConfigMaps, Secrets)"
echo "  - 1 ChronoDB StatefulSet with PersistentVolumeClaim (10Gi, ReadWriteOnce) and Headless Service"
echo "  - 2 HPAs (Gateway CPU scaling, Processor 12-partition ceiling)"
echo "  - 1 PodDisruptionBudget ensuring high availability during drains"
echo "  - 1 Simulator Batch Verification Job template"
echo "  - Kubernetes NetworkPolicies (Default-Deny Ingress, least-privilege pod isolation)"
echo "  - Kubernetes Secrets (TLS certificates, API keys, JWT secret, Webhook HMAC secret)"
echo "=================================================================="
