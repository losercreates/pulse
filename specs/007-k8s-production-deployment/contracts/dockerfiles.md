# Contract: Multi-Stage Distroless Container Packaging

**Feature**: `007-k8s-production-deployment`  
**Date**: 2026-10-27  
**Status**: Completed  

---

## 1. Container Build Specifications

All container images are built using a unified multi-stage build design or individual service Dockerfiles located in `deployments/docker/`.

### 1.1 Base Build Image Contract
- **Builder Base**: `golang:1.23-alpine`
- **Builder Dependencies**: `ca-certificates`, `tzdata`, `git`
- **Build Flags**:
  ```bash
  CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w -extldflags '-static'" \
    -o /bin/<service> ./cmd/<service>
  ```
- **Runtime Base**: `gcr.io/distroless/static-debian12:nonroot`
- **User Context**: `USER 65532:65532` (`nonroot:nonroot`)
- **Working Directory**: `/`
- **Root Filesystem**: Read-only compatible (`readOnlyRootFilesystem: true` with `/tmp` emptyDir)

---

## 2. Microservice Container Inventory

| Binary | Dockerfile Path | Exposed Ports | Health Check Endpoint | Target Size |
| :--- | :--- | :--- | :--- | :---: |
| `gateway` | `deployments/docker/Dockerfile.gateway` | `8080/TCP` (HTTP), `50051/TCP` (gRPC) | `http://:8080/healthz` | $< 35\text{ MB}$ |
| `processor` | `deployments/docker/Dockerfile.processor` | `8081/TCP` (HTTP/Metrics) | `http://:8081/healthz` | $< 35\text{ MB}$ |
| `alertd` | `deployments/docker/Dockerfile.alertd` | `8083/TCP` (HTTP/Metrics) | `http://:8083/healthz` | $< 35\text{ MB}$ |
| `chronod` | `deployments/docker/Dockerfile.chronod` | `8084/TCP` (HTTP), `50054/TCP` (gRPC) | `http://:8084/healthz` | $< 35\text{ MB}$ |
| `simulator` | `deployments/docker/Dockerfile.simulator` | — | CLI execution | $< 35\text{ MB}$ |

---

## 3. Template Multi-Stage Dockerfile Contract

```dockerfile
# Stage 1: Build static binary
FROM golang:1.23-alpine AS builder

WORKDIR /src

RUN apk add --no-cache ca-certificates tzdata

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG SERVICE=gateway
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w -extldflags '-static'" \
    -o /bin/service ./cmd/${SERVICE}

# Stage 2: Minimal distroless nonroot runtime
FROM gcr.io/distroless/static-debian12:nonroot

USER 65532:65532
WORKDIR /

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /bin/service /usr/local/bin/service

ENTRYPOINT ["/usr/local/bin/service"]
```
