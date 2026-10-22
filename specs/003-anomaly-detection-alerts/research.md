# Phase 0 Research & Technical Decisions: Real-Time Anomaly Detection & Decoupled Alerting

**Feature**: `003-anomaly-detection-alerts`
**Date**: 2026-10-22
**Status**: Complete

---

## 1. Online Statistical Baseline & Anomaly Detection Algorithm

### Decision: Exponentially Weighted Moving Average (EWMA) with Exponential Moving Variance
For each stream slice `(device_id, metric_name)`, we maintain an incremental, single-pass statistical baseline updated on every arriving event:

1. **Deviation**:
   $$\delta_t = x_t - \mu_{t-1}$$
2. **Mean Update (EWMA)**:
   $$\mu_t = \mu_{t-1} + \alpha \cdot \delta_t$$
3. **Variance Update (Exponential Moving Variance)**:
   $$\sigma^2_t = (1 - \alpha) \cdot (\sigma^2_{t-1} + \alpha \cdot \delta_t^2)$$
4. **Defensive Variance Floor Guardrail**:
   $$\sigma_t = \sqrt{\max(\sigma^2_t, \epsilon)}$$
   where default $\epsilon = 10^{-4}$ (equivalent to $\sigma_{\text{min}} = 0.01$).
5. **Standardized Deviation ($z$-score)**:
   $$z_t = \frac{|x_t - \mu_t|}{\sigma_t}$$

### Anomaly Classification Thresholds
- **Nominal**: $z_t < 2.5$
- **INFO**: $2.5 \le z_t < 3.0$
- **WARNING**: $3.0 \le z_t < 5.0$
- **CRITICAL**: $z_t \ge 5.0$

### Rationale
- **$O(1)$ Time & $O(1)$ Memory**: The update requires only 4 floating-point arithmetic operations and stores only 40 bytes per metric series (`mean`, `variance`, `sample_count`, `last_update_ns`, `cooldown_until_ns`, `consecutive_nominal`).
- **Zero Cross-Goroutine Contention**: Stored strictly within partition-local memory inside each partition worker goroutine, guaranteeing lock-free execution on the hot ingestion path (Constitution Principle I & IV).
- **Dynamic Adaptability**: Adapts to shifting operating regimes (e.g. turbine spooling up) via decay factor $\alpha$ (default $\alpha = 0.05$, corresponding to an effective window of $2/\alpha - 1 \approx 39$ samples) without unbounded memory growth.

### Alternatives Considered
- **Fixed-Window Rolling Standard Deviation (Welford's over ring buffer)**: Requires storing $W$ samples in memory. For 10,000 devices $\times$ 5 metrics $\times$ 100 samples, requires $> 40\text{ MB}$ of buffer allocations and pointer overhead per partition.
- **Holt-Winters / Seasonal ARIMA**: High computational complexity ($> 10\,\mu\text{s}$ per evaluation), unsuitable for $50,000\text{ events/sec}$ real-time stream ingestion.

---

## 2. Guardrails Against False-Positive Alert Storms

### Decision: Warmup Sample Threshold & Minimum Variance Floor
- **Warmup Phase ($N_{\text{warmup}} = 30$)**: When a new `(device_id, metric_name)` begins streaming, sample count $N < 30$ updates the baseline statistics but suppresses anomaly alerts until the baseline converges.
- **Variance Floor ($\epsilon = 10^{-4}$)**: If a sensor flatlines (e.g. constant 50.0000), sample variance collapses to 0. Without a floor, any minor digital noise (e.g. 50.0001) would yield an astronomical $z$-score ($z = \frac{0.0001}{10^{-10}} = 1,000,000$) and trigger false-positive alert storms. Capping $\sigma^2 \ge \epsilon$ ensures stability.

---

## 3. Decoupled Asynchronous Alert Dispatch Architecture

### Decision: Redpanda Topic `telemetry.alerts` with Independent Dispatcher Worker Pool
- **Architecture**:
  ```mermaid
  flowchart LR
      SW[Stream Worker (Partition N)] -->|Non-blocking publish| TA[(Redpanda: telemetry.alerts)]
      TA --> DW1[Alert Dispatcher Worker 1]
      TA --> DW2[Alert Dispatcher Worker 2]
      DW1 --> WH1[HTTP Webhook Sink]
      DW1 --> SL[Slack / PagerDuty Mock]
      DW2 --> DLQ[(Dead-Letter Queue)]
  ```
- **Constitution Principle IV Adherence**: Stream workers do NOT make external HTTP network requests. When an anomaly is detected, the worker produces an `AlertEvent` to Redpanda topic `telemetry.alerts` asynchronously.
- **Resilience**: Even if downstream webhooks hang for 30 seconds or crash, the stream processing engine and ingestion gateway continue operating at full throughput with zero latency degradation or consumer lag.

---

## 4. Alert Deduplication, Cooldown & Auto-Resolution Hysteresis

### Decision: In-Memory Incident State Machine per Series
1. **Incident State**:
   - `IDLE`: Normal operation.
   - `FIRING`: Anomaly active.
2. **Cooldown Suppression (default: 5 minutes)**:
   - On transition `IDLE` $\rightarrow$ `FIRING`, an `AlertEvent` with status `STATUS_FIRING` is immediately emitted and dispatched.
   - Subsequent anomalies for the same `(device_id, metric_name)` within 5 minutes update incident telemetry but suppress external notification calls.
   - If the anomaly persists after 5 minutes, an escalation reminder alert is dispatched.
3. **Auto-Resolution Hysteresis ($N = 10$ consecutive nominal samples)**:
   - To prevent alert flapping around the boundary, the incident is only marked `RESOLVED` after 10 consecutive readings remain within $1.5\sigma$ of the mean.
   - Emits an `AlertEvent` with status `STATUS_RESOLVED`.

---

## 5. Resilient Dispatching: Circuit Breaker & Exponential Backoff

### Decision: Client-Side Circuit Breaker with Exponential Jittered Backoff
- **HTTP Client Settings**:
  - `Timeout`: 5 seconds per request.
  - `MaxConnsPerHost`: 50.
  - `IdleConnTimeout`: 90 seconds.
- **Backoff Schedule**:
  - Base delay: 100ms, Multiplier: 2.0, Max delay: 2s, Jitter: $\pm 25\%$.
  - Max retries: 3 attempts.
- **Circuit Breaker States**:
  - `CLOSED`: Normal operation; all requests pass through.
  - `OPEN`: 5 consecutive failures trips circuit to `OPEN` for 30 seconds. In `OPEN` state, requests fail fast and divert to dead-letter storage without blocking dispatcher threads.
  - `HALF-OPEN`: After 30 seconds, allows a probe request. Success transitions back to `CLOSED`; failure resets the 30-second `OPEN` timer.
