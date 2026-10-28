# Quickstart: Milestone 008 — ChronoDB Storage Durability & Persistent Volume Orchestration

This guide demonstrates how to configure, run, and verify ChronoDB's embedded LSM durable storage engine, Write-Ahead Logging (WAL), crash recovery protocol, and Kubernetes StatefulSet deployment.

---

## 1. Running ChronoDB Locally with Durable Storage

### Environment Configuration
| Variable | Description | Default |
|---|---|---|
| `STORAGE_DATA_DIR` | Filesystem path for on-disk WAL and SSTable segments | `/var/lib/pulse/chronodb` (or `./data/chronodb` locally) |
| `STORAGE_SHARDS` | Number of deterministic device hash shards on disk | `16` |
| `STORAGE_RETENTION_DAYS` | Retention window TTL before background compaction eviction | `14` |
| `STORAGE_COMPACTION_INTERVAL` | Cadence for background SSTable compaction | `5m` |

### Starting ChronoDB Locally
```bash
# Build all platform binaries
make build

# Start ChronoDB with local data persistence
STORAGE_DATA_DIR="./data/chronodb" ./bin/chronod
```

---

## 2. Testing Crash Recovery & Data Survivability

Pulse includes an automated crash & recovery integration test asserting 100% data survivability:

```bash
# Execute crash and replay invariance verification
go test -v -run TestDurableStorage_CrashAndRecoveryInvariance ./tests/integration/...
```

### Manual Crash Test Verification
1. Start ChronoDB with a designated data directory:
   ```bash
   STORAGE_DATA_DIR="/tmp/pulse-test" ./bin/chronod &
   PID=$!
   ```
2. Ingest window aggregates via the simulator or HTTP sink.
3. Simulate an ungraceful hardware / power failure:
   ```bash
   kill -9 $PID
   ```
4. Restart ChronoDB against the identical directory:
   ```bash
   STORAGE_DATA_DIR="/tmp/pulse-test" ./bin/chronod &
   ```
5. Query the REST query endpoint to confirm all pre-crash records are present:
   ```bash
   curl -s "http://localhost:8084/api/v1/query?device_id=dev-000001&metric=cpu_usage" | jq .
   ```

---

## 3. Deploying ChronoDB StatefulSet with Helm

### Deploy to Kubernetes with Persistent Volumes
```bash
helm upgrade --install pulse ./deployments/helm/pulse \
  --set chronod.persistence.enabled=true \
  --set chronod.persistence.size=10Gi
```

### Inspect StatefulSet & PersistentVolumeClaim
```bash
# Verify the StatefulSet is running
kubectl get statefulset pulse-chronod

# Verify the dedicated PVC is bound
kubectl get pvc -l app.kubernetes.io/component=chronod

# Check pod storage mount
kubectl exec -it pulse-chronod-0 -- df -h /var/lib/pulse/chronodb
```
