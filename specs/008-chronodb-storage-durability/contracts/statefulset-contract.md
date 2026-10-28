# Contract: Kubernetes StatefulSet & Storage Provisioning

**Feature**: `008-chronodb-storage-durability`  
**Date**: 2026-10-28  

---

## 1. Workload Specification: StatefulSet

Replaces `chronod-deployment.yaml` to ensure deterministic pod identities and dedicated volume mounting across rescheduling.

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: pulse-chronod
  labels:
    app.kubernetes.io/name: pulse
    app.kubernetes.io/component: chronod
spec:
  serviceName: pulse-chronod-headless
  replicas: 1
  podManagementPolicy: OrderedReady
  updateStrategy:
    type: RollingUpdate
  selector:
    matchLabels:
      app.kubernetes.io/name: pulse
      app.kubernetes.io/component: chronod
  template:
    metadata:
      labels:
        app.kubernetes.io/name: pulse
        app.kubernetes.io/component: chronod
    spec:
      terminationGracePeriodSeconds: 30
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        runAsGroup: 65532
        fsGroup: 65532
      containers:
        - name: chronod
          image: "pulse-chronod:latest"
          securityContext:
            readOnlyRootFilesystem: true
            allowPrivilegeEscalation: false
            capabilities:
              drop: [ "ALL" ]
          volumeMounts:
            - name: data
              mountPath: /var/lib/pulse/chronodb
            - name: tmp
              mountPath: /tmp
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: [ "ReadWriteOnce" ]
        resources:
          requests:
            storage: 10Gi
```

---

## 2. Headless Service Specification

Provides deterministic DNS resolution for StatefulSet pod ordinals (`pulse-chronod-0.pulse-chronod-headless`).

```yaml
apiVersion: v1
kind: Service
metadata:
  name: pulse-chronod-headless
  labels:
    app.kubernetes.io/name: pulse
    app.kubernetes.io/component: chronod
spec:
  clusterIP: None
  selector:
    app.kubernetes.io/name: pulse
    app.kubernetes.io/component: chronod
  ports:
    - name: http
      port: 8084
      targetPort: 8084
    - name: grpc
      port: 50054
      targetPort: 50054
```
