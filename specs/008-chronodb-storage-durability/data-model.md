# Data Model: Milestone 008 — ChronoDB Storage Durability & Persistent Storage

**Feature**: `008-chronodb-storage-durability`  
**Date**: 2026-10-28  
**Status**: Completed  

---

## 1. On-Disk Directory Hierarchy

```text
/var/lib/pulse/chronodb/
├── LOCK                               # Advisory flock preventing concurrent process execution
├── catalog.bin                        # Serialized global device catalog metadata
├── MANIFEST                           # Storage engine manifest tracking active shards & versions
└── shard-{00..15}/                    # 16 deterministic device hash partitions
    ├── wal-000001.log                 # Active Write-Ahead Log receiving synchronous writes
    ├── wal-000000.log.checkpointed    # Flushed WAL segment pending rotation
    ├── segment-1730000000-001.sst     # Flushed immutable SSTable segment
    ├── segment-1730000000-002.sst     # Flushed immutable SSTable segment
    └── segment-compacted-001.sst      # Result of background multi-way merge compaction
```

---

## 2. Binary Write-Ahead Log (WAL) Record Framing

Every batch write appended to `wal-*.log` conforms to the following fixed-overhead binary framing:

```text
+-------------------+-------------------+-------------------+-------------------+-----------------------+
| Magic (4 bytes)   | Length (4 bytes)  | CRC32 (4 bytes)   | OpType (1 byte)   | Payload (N bytes)     |
| uint32 (0x50554C53)| uint32 big-endian | uint32 IEEE-CRC32 | 0x01 (Put)        | StoredAggregate bytes |
+-------------------+-------------------+-------------------+-------------------+-----------------------+
```

### Fields
| Field | Type | Size | Description |
|---|---|---|---|
| `Magic` | `uint32` | 4 bytes | Constant sentinel `0x50554C53` ("PULS") identifying valid frames. |
| `Length` | `uint32` | 4 bytes | Big-endian byte count of the serialized payload. |
| `CRC32` | `uint32` | 4 bytes | IEEE 32-bit cyclic redundancy check over the payload bytes. |
| `OpType` | `uint8` | 1 byte | Operation type (`0x01` = PutAggregate, `0x02` = Tombstone, `0x03` = Checkpoint). |
| `Payload` | `[]byte` | $N$ bytes | Protobuf-encoded `telemetryv1.StoredAggregate`. |

---

## 3. SSTable On-Disk Format

When a shard's in-memory memtable reaches capacity ($4\text{MB}$ or 5,000 aggregates), it is immutably serialized into an `.sst` file:

```text
+---------------------------------------------------------------------------------+
| Data Block 0: Sorted StoredAggregate Records (Length-Prefixed Protobuf)         |
+---------------------------------------------------------------------------------+
| Data Block 1: Sorted StoredAggregate Records (Length-Prefixed Protobuf)         |
+---------------------------------------------------------------------------------+
| ...                                                                             |
+---------------------------------------------------------------------------------+
| Sparse Index Block: Array of [CompositeKey, BlockOffset, BlockLength]           |
+---------------------------------------------------------------------------------+
| Bloom Filter Block: 10 bits/key Murmur3 filter for zero-disk-read misses        |
+---------------------------------------------------------------------------------+
| Trailer Block (Fixed 32 Bytes):                                                 |
|   IndexOffset (uint64) | IndexLength (uint32)                                   |
|   BloomOffset (uint64) | BloomLength (uint32)                                   |
|   RecordCount (uint32) | Magic (uint32: 0x53535442)                             |
+---------------------------------------------------------------------------------+
```

### Key Encoding Schema
Composite keys are stored in strictly sorted lexicographical order:
```text
{device_id}#{metric_name}#{window_start_ns:019d}#{window_end_ns:019d}
```
*Note*: Timestamp components are zero-padded to 19 digits to guarantee chronological byte-sorting.

---

## 4. Persistent Device Catalog Index

Stored in `catalog.bin` with CRC32 integrity verification:

```text
+--------------------+--------------------+-----------------------+
| Version (2 bytes)  | Count (4 bytes)    | CRC32 (4 bytes)       |
+--------------------+--------------------+-----------------------+
| Entry 1: DeviceID (string) | MetricCount (uint16) | Metrics ([]string) |
|          FirstSeenNs (int64) | LastSeenNs (int64)                      |
+-------------------------------------------------------------------------+
| Entry 2: ...                                                            |
+-------------------------------------------------------------------------+
```

---

## 5. Kubernetes StatefulSet Volume Claim Model

```yaml
volumeClaimTemplates:
  - metadata:
      name: data
      labels:
        app.kubernetes.io/name: pulse
        app.kubernetes.io/component: chronod
    spec:
      accessModes:
        - ReadWriteOnce
      storageClassName: "" # Defaults to cluster default storage class
      resources:
        requests:
          storage: 10Gi
```

### Mount Path & Security Context
- **Container Mount**: `/var/lib/pulse/chronodb`
- **Filesystem Permissions**: Owner `UID 65532` (`nonroot`), `fsGroup: 65532`.
- **Readiness Probe**: Checks write access to `/var/lib/pulse/chronodb/LOCK` and validates free disk space $> 5\%$.
