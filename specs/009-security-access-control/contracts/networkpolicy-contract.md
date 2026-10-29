# Contract: Kubernetes NetworkPolicy Specification

**Feature**: `009-security-access-control`  
**Date**: 2026-10-29  

---

## 1. Default-Deny Ingress NetworkPolicy

Enforces default-deny on all pods within the `pulse` namespace:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: pulse-default-deny-ingress
  labels:
    app.kubernetes.io/name: pulse
    app.kubernetes.io/part-of: pulse
spec:
  podSelector: {}
  policyTypes:
    - Ingress
```

---

## 2. Ingestion Gateway NetworkPolicy

Allows external ingress on `:8080` (HTTP) and `:50051` (gRPC), with egress strictly restricted to Redpanda broker ports and DNS:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: pulse-gateway-netpol
  labels:
    app.kubernetes.io/component: gateway
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/component: gateway
  policyTypes:
    - Ingress
    - Egress
  ingress:
    - ports:
        - protocol: TCP
          port: 8080
        - protocol: TCP
          port: 50051
  egress:
    # Egress to Redpanda message broker
    - to:
        - podSelector:
            matchLabels:
              app.kubernetes.io/component: redpanda
      ports:
        - protocol: TCP
          port: 9092
    # Egress to CoreDNS for name resolution
    - to:
        - namespaceSelector: {}
      ports:
        - protocol: UDP
          port: 53
        - protocol: TCP
          port: 53
```

---

## 3. ChronoDB Storage & Query NetworkPolicy

Restricts ingress on `:8084` and `:50054` strictly to authorized query clients and Grafana dashboards, with egress restricted to Redpanda:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: pulse-chronod-netpol
  labels:
    app.kubernetes.io/component: chronod
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/component: chronod
  policyTypes:
    - Ingress
    - Egress
  ingress:
    # Allow authorized query clients or dashboards
    - ports:
        - protocol: TCP
          port: 8084
        - protocol: TCP
          port: 50054
  egress:
    - to:
        - podSelector:
            matchLabels:
              app.kubernetes.io/component: redpanda
      ports:
        - protocol: TCP
          port: 9092
    - to:
        - namespaceSelector: {}
      ports:
        - protocol: UDP
          port: 53
        - protocol: TCP
          port: 53
```

---

## 4. Decoupled Alert Dispatcher NetworkPolicy

Restricts ingress strictly to Prometheus metrics scraping (`:8083`), with egress permitted to Redpanda and external webhook endpoints (HTTP/HTTPS):

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: pulse-alertd-netpol
  labels:
    app.kubernetes.io/component: alertd
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/component: alertd
  policyTypes:
    - Ingress
    - Egress
  ingress:
    - ports:
        - protocol: TCP
          port: 8083
  egress:
    - to:
        - podSelector:
            matchLabels:
              app.kubernetes.io/component: redpanda
      ports:
        - protocol: TCP
          port: 9092
    # Allow egress to external webhook notification destinations
    - ports:
        - protocol: TCP
          port: 443
        - protocol: TCP
          port: 80
    - to:
        - namespaceSelector: {}
      ports:
        - protocol: UDP
          port: 53
        - protocol: TCP
          port: 53
```

