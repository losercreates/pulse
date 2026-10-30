# Contract: Complex Event Processing (CEP) Engine Interfaces

**Feature**: `010-edge-bridge-cep`  
**Date**: 2026-10-30  

---

## 1. CEPEngine (Partition-Local State Machine Interface)

```go
package cep

import (
    telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

// CEPEngine manages stateful pattern matching and cross-metric evaluations inside a single
// partition worker goroutine. Zero-lock hot-path compliance with Constitution Principle I.
type CEPEngine interface {
    // ProcessReading evaluates a single incoming telemetry reading against active rules.
    // Returns any completed composite incidents triggered by this reading.
    ProcessReading(reading *telemetryv1.TelemetryReading, zScore float64) []*telemetryv1.AlertEvent

    // AdvanceWatermark purges expired sequence candidates older than (watermark - allowedLateness).
    AdvanceWatermark(watermarkNs int64)

    // ReloadRules atomically swaps the active compiled rule registry.
    ReloadRules(rules []CEPRule) error

    // ActiveCandidatesCount returns current in-flight partial sequence matches.
    ActiveCandidatesCount() int
}
```

---

## 2. RuleCompiler & ExpressionEvaluator

```go
package cep

// RuleCompiler parses declarative YAML/JSON rule files into optimized runtime execution trees.
type RuleCompiler interface {
    // Compile validates syntax, compiles math expressions, and builds NFA transition tables.
    Compile(rawConfig []byte) ([]CEPRule, error)

    // CompileFile loads and validates rule definitions from a file path.
    CompileFile(filePath string) ([]CEPRule, error)
}

// ExpressionEvaluator parses and executes arithmetic formulas across concurrent device metrics.
type ExpressionEvaluator interface {
    // Evaluate executes the compiled formula using the provided metric values map.
    // Example: Evaluate("power_mw / fuel_flow", {"power_mw": 85.0, "fuel_flow": 120.0}) -> 0.7083
    Evaluate(expression string, values map[string]float64) (float64, error)
}
```

