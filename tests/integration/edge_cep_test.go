package integration_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/bridge"
	"github.com/losercreates/pulse/internal/bridge/adapter"
	"github.com/losercreates/pulse/internal/bridge/config"
	"github.com/losercreates/pulse/internal/bridge/spooler"
	"github.com/losercreates/pulse/internal/processor/cep"
	"github.com/losercreates/pulse/internal/processor/consumer"
	"github.com/losercreates/pulse/internal/processor/server"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

type mockGateway struct {
	mu        sync.Mutex
	available bool
	received  []*telemetryv1.TelemetryReading
}

func (m *mockGateway) ForwardBatch(ctx context.Context, batch []*telemetryv1.TelemetryReading) (*telemetryv1.IngestionReceipt, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.available {
		return nil, fmt.Errorf("gateway offline / network partition")
	}
	m.received = append(m.received, batch...)
	return &telemetryv1.IngestionReceipt{Status: telemetryv1.IngestionReceipt_STATUS_ACCEPTED, ProcessedCount: int32(len(batch))}, nil
}

func (m *mockGateway) IsConnected() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.available
}

func (m *mockGateway) Close() error {
	return nil
}

type mockAlertPublisher struct {
	mu     sync.Mutex
	alerts []*telemetryv1.AlertEvent
}

func (p *mockAlertPublisher) PublishAlert(ctx context.Context, alert *telemetryv1.AlertEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.alerts = append(p.alerts, alert)
	return nil
}

func TestEdgeBridgeAndCEPEndToEnd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tmpDir := t.TempDir()

	// 1. Setup Bridge with Offline Spooler and Mock Gateway
	gw := &mockGateway{available: true}
	sp, err := spooler.NewDiskSpooler(filepath.Join(tmpDir, "spool"), 50*1024*1024)
	require.NoError(t, err)

	bridgeCfg := &config.EdgeBridgeConfig{
		BridgeID:        "plant-01-bridge",
		BatchLinger:     5 * time.Millisecond,
		BatchMaxRecords: 10,
	}

	mqttAdapter, err := adapter.NewMQTTAdapter(config.MQTTAdapterConfig{
		Name:         "plant-mqtt",
		Topics:       []string{"factory/+/+/telemetry"},
		Format:       "json",
		TopicPattern: "factory/{area}/{device_id}/telemetry",
	})
	require.NoError(t, err)

	coord := bridge.NewCoordinator(bridgeCfg, []adapter.ProtocolAdapter{mqttAdapter}, sp, gw, nil)
	require.NoError(t, coord.Start(ctx))
	defer func() { _ = coord.Stop() }()

	// 2. Ingest MQTT JSON telemetry via Bridge
	baseTime := time.Date(2026, 10, 31, 14, 0, 0, 0, time.UTC).UnixNano()

	payload1 := []byte(`{"vibration": 5.8, "ambient_temp": 28.0}`)
	readings1, err := mqttAdapter.DecodeMessage("factory/line1/turbine-01/telemetry", payload1, baseTime)
	require.NoError(t, err)
	for _, r := range readings1 {
		coord.IngestChannel() <- r
	}

	// Wait for bridge to forward to gateway
	time.Sleep(30 * time.Millisecond)

	gw.mu.Lock()
	assert.NotEmpty(t, gw.received)
	assert.Equal(t, int64(0), sp.BufferedCount())
	gw.mu.Unlock()

	// 3. Simulate Network Partition: Sever Gateway Connection
	gw.mu.Lock()
	gw.available = false
	gw.mu.Unlock()

	// Ingest Modbus reading during network partition
	modbusPayload := []byte(`{"bearing_temp": 98.4}`)
	readingsPartition, err := mqttAdapter.DecodeMessage("factory/line1/turbine-01/telemetry", modbusPayload, baseTime+int64(15*time.Second))
	require.NoError(t, err)
	for _, r := range readingsPartition {
		coord.IngestChannel() <- r
	}

	time.Sleep(30 * time.Millisecond)
	// Must have spooled to disk
	assert.Greater(t, sp.BufferedCount(), int64(0))

	// Reconnect Gateway
	gw.mu.Lock()
	gw.available = true
	gw.mu.Unlock()

	// Wait for backfill
	time.Sleep(40 * time.Millisecond)
	assert.Equal(t, int64(0), sp.BufferedCount())

	// 4. Setup Processor with CEP Engine evaluating the sequence
	rulesFile := filepath.Join(tmpDir, "cep_rules.yaml")
	rulesContent := `
rules:
  - rule_id: "RULE-BEARING-RUNAWAY"
    name: "Bearing Thermal Runaway Cascade"
    device_pattern: "turbine-*"
    window_duration: 30s
    pattern_type: "SEQUENCE"
    sequence_steps:
      - step_index: 0
        metric_name: "vibration"
        condition: ">="
        threshold: 5.0
      - step_index: 1
        metric_name: "bearing_temp"
        condition: ">="
        threshold: 90.0
    severity: "CRITICAL"
    cooldown: 1m
  - rule_id: "RULE-EFFICIENCY"
    name: "Efficiency Drop"
    device_pattern: "*"
    window_duration: 1m
    pattern_type: "EXPRESSION"
    expression: "power_mw / fuel_flow < 0.70"
    severity: "WARNING"
`
	require.NoError(t, os.WriteFile(rulesFile, []byte(rulesContent), 0600))

	compiler := cep.NewRuleCompiler()
	compiledRules, err := compiler.CompileFile(rulesFile)
	require.NoError(t, err)
	require.Len(t, compiledRules, 2)

	alertPublisher := &mockAlertPublisher{}
	partWorker := consumer.NewPartitionWorker(
		"telemetry.raw",
		0,
		5*time.Second,
		10*time.Second,
		10*time.Second,
		60*time.Second,
		10*time.Second,
		nil,
		nil,
		nil,
		1024,
	)

	cepEngine := cep.NewEngine(0)
	require.NoError(t, cepEngine.ReloadRules(compiledRules))
	partWorker.SetCEPEngine(cepEngine)
	partWorker.SetAlertPipeline(nil, alertPublisher)

	go partWorker.Run(ctx)

	// Feed all readings from gateway into partition worker
	gw.mu.Lock()
	allReadings := make([]*telemetryv1.TelemetryReading, len(gw.received))
	copy(allReadings, gw.received)
	gw.mu.Unlock()

	for _, rd := range allReadings {
		payload, _ := proto.Marshal(rd)
		partWorker.Inbound() <- &kgo.Record{
			Key:   []byte(rd.DeviceId),
			Value: payload,
		}
	}

	// 5. Verify CEP synthesized the composite incident alert
	time.Sleep(50 * time.Millisecond)

	alertPublisher.mu.Lock()
	require.NotEmpty(t, alertPublisher.alerts)
	cascadeAlert := alertPublisher.alerts[0]
	assert.Equal(t, "turbine-01", cascadeAlert.DeviceId)
	assert.Equal(t, telemetryv1.AlertSeverity_SEVERITY_CRITICAL, cascadeAlert.Severity)
	assert.Contains(t, cascadeAlert.AlertId, "RULE-BEARING-RUNAWAY")
	alertPublisher.mu.Unlock()

	// 6. Test Live Rule Reload via HTTPServer
	reloader := cep.NewRuleReloader(rulesFile, nil)
	httpServer := server.NewHTTPServer(server.WithRuleReloader(reloader))

	reloadReq := httptest.NewRequest("POST", "/api/v1/rules/reload", nil)
	reloadRec := httptest.NewRecorder()
	httpServer.Handler().ServeHTTP(reloadRec, reloadReq)
	assert.Equal(t, http.StatusOK, reloadRec.Code)
	assert.Contains(t, reloadRec.Body.String(), `"rules_loaded":2`)
}

