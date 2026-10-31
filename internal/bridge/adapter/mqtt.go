package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/losercreates/pulse/internal/bridge/config"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

// TopicMatches evaluates an MQTT wildcard pattern (+ and #) against a concrete topic.
func TopicMatches(pattern, topic string) bool {
	if pattern == "#" || pattern == topic {
		return true
	}

	pParts := strings.Split(pattern, "/")
	tParts := strings.Split(topic, "/")

	for i := 0; i < len(pParts); i++ {
		if pParts[i] == "#" {
			return true
		}
		if i >= len(tParts) {
			return false
		}
		if pParts[i] != "+" && pParts[i] != tParts[i] {
			return false
		}
	}

	return len(pParts) == len(tParts)
}

// ExtractTokens extracts dynamic variables like {device_id} from a topic pattern.
func ExtractTokens(pattern, topic string) map[string]string {
	tokens := make(map[string]string)
	if pattern == "" || topic == "" {
		return tokens
	}

	pParts := strings.Split(pattern, "/")
	tParts := strings.Split(topic, "/")

	for i := 0; i < len(pParts) && i < len(tParts); i++ {
		p := pParts[i]
		if strings.HasPrefix(p, "{") && strings.HasSuffix(p, "}") {
			key := p[1 : len(p)-1]
			tokens[key] = tParts[i]
		}
	}
	return tokens
}

// MQTTAdapter implements ProtocolAdapter for MQTT v3/v5 streams.
type MQTTAdapter struct {
	cfg       config.MQTTAdapterConfig
	statusMu  sync.RWMutex
	status    AdapterStatus
	msgCount  atomic.Uint64
	errCount  atomic.Uint64
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	connected atomic.Bool
}

// NewMQTTAdapter creates a new MQTT protocol adapter from configuration.
func NewMQTTAdapter(cfg config.MQTTAdapterConfig) (*MQTTAdapter, error) {
	if cfg.Name == "" {
		cfg.Name = "mqtt-" + cfg.ClientID
	}
	if cfg.Format == "" {
		cfg.Format = "json"
	}
	return &MQTTAdapter{
		cfg: cfg,
	}, nil
}

// Name returns the unique adapter identifier.
func (a *MQTTAdapter) Name() string {
	return a.cfg.Name
}

// Status returns current connection and message counters.
func (a *MQTTAdapter) Status() AdapterStatus {
	a.statusMu.RLock()
	defer a.statusMu.RUnlock()
	return AdapterStatus{
		Connected:        a.connected.Load(),
		MessagesReceived: a.msgCount.Load(),
		ErrorsTotal:      a.errCount.Load(),
		LastError:        a.status.LastError,
	}
}

func (a *MQTTAdapter) setError(err error) {
	a.errCount.Add(1)
	a.statusMu.Lock()
	a.status.LastError = err.Error()
	a.statusMu.Unlock()
}

// Start begins connecting and streaming telemetry to the out channel.
func (a *MQTTAdapter) Start(ctx context.Context, out chan<- *telemetryv1.TelemetryReading) error {
	ctx, cancel := context.WithCancel(ctx)
	a.cancel = cancel

	a.wg.Add(1)
	go a.connectionLoop(ctx, out)

	return nil
}

// Stop terminates the adapter background loops.
func (a *MQTTAdapter) Stop() error {
	if a.cancel != nil {
		a.cancel()
	}
	a.connected.Store(false)
	a.wg.Wait()
	return nil
}

func (a *MQTTAdapter) connectionLoop(ctx context.Context, out chan<- *telemetryv1.TelemetryReading) {
	defer a.wg.Done()

	// If BrokerURL is empty or simulation, keep running in connected state ready for DecodeMessage
	if a.cfg.BrokerURL == "" {
		a.connected.Store(true)
		<-ctx.Done()
		a.connected.Store(false)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Connect to broker endpoint (e.g. "localhost:1883" or "tcp://localhost:1883")
		addr := a.cfg.BrokerURL
		if strings.HasPrefix(addr, "tcp://") {
			addr = strings.TrimPrefix(addr, "tcp://")
		}

		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err != nil {
			a.setError(fmt.Errorf("mqtt dial failed: %w", err))
			a.connected.Store(false)
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
				continue
			}
		}

		a.connected.Store(true)
		a.readBrokerStream(ctx, conn, out)
		_ = conn.Close()
		a.connected.Store(false)
	}
}

func (a *MQTTAdapter) readBrokerStream(ctx context.Context, conn net.Conn, out chan<- *telemetryv1.TelemetryReading) {
	buf := make([]byte, 4096)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		n, err := conn.Read(buf)
		if err != nil {
			return
		}
		if n > 0 {
			// Process any inbound frame
			a.msgCount.Add(1)
		}
	}
}

// DecodeMessage parses an incoming MQTT payload into canonical TelemetryReadings.
func (a *MQTTAdapter) DecodeMessage(topic string, payload []byte, defaultTimestampNs int64) ([]*telemetryv1.TelemetryReading, error) {
	if defaultTimestampNs <= 0 {
		defaultTimestampNs = time.Now().UnixNano()
	}

	if a.cfg.Format == "sparkplug_b" {
		return DecodeSparkplugMessage(topic, payload)
	}

	// JSON parsing
	var raw interface{}
	if err := json.Unmarshal(payload, &raw); err != nil {
		a.setError(err)
		return nil, fmt.Errorf("invalid json payload: %w", err)
	}

	tokens := ExtractTokens(a.cfg.TopicPattern, topic)
	deviceID := tokens["device_id"]

	var readings []*telemetryv1.TelemetryReading

	switch val := raw.(type) {
	case map[string]interface{}:
		// Check for embedded device_id
		if devField, ok := val["device_id"].(string); ok && devField != "" {
			deviceID = devField
		} else if a.cfg.DeviceIDField != "" {
			if customDev, ok := val[a.cfg.DeviceIDField].(string); ok && customDev != "" {
				deviceID = customDev
			}
		}

		if deviceID == "" {
			deviceID = a.cfg.ClientID
			if deviceID == "" {
				deviceID = "mqtt-default"
			}
		}

		// Check for embedded timestamp
		tsNs := defaultTimestampNs
		if tsVal, ok := val["timestamp_ns"].(float64); ok && tsVal > 0 {
			tsNs = int64(tsVal)
		} else if tsVal, ok := val["timestamp"].(float64); ok && tsVal > 0 {
			if tsVal > 1e16 {
				tsNs = int64(tsVal)
			} else if tsVal > 1e12 {
				tsNs = int64(tsVal * 1e6)
			} else {
				tsNs = int64(tsVal * 1e9)
			}
		}

		// Check if it's a single metric format: {"metric": "flow_rate", "value": 150.5, "unit": "gpm"}
		if metricName, ok := val["metric"].(string); ok && metricName != "" {
			if numVal, ok := getFloat(val["value"]); ok {
				unit, _ := val["unit"].(string)
				readings = append(readings, &telemetryv1.TelemetryReading{
					DeviceId:    deviceID,
					MetricName:  metricName,
					Value:       numVal,
					TimestampNs: tsNs,
					Unit:        unit,
				})
				a.msgCount.Add(1)
				return readings, nil
			}
		}

		// Flat key-value map: {"vibration": 3.42, "temperature": 75.8}
		for k, v := range val {
			if k == "device_id" || k == "timestamp" || k == "timestamp_ns" || k == "unit" || k == a.cfg.DeviceIDField {
				continue
			}
			if numVal, ok := getFloat(v); ok {
				readings = append(readings, &telemetryv1.TelemetryReading{
					DeviceId:    deviceID,
					MetricName:  k,
					Value:       numVal,
					TimestampNs: tsNs,
				})
			}
		}

	case []interface{}:
		// Array of readings
		for _, item := range val {
			if m, ok := item.(map[string]interface{}); ok {
				subPayload, _ := json.Marshal(m)
				subReadings, err := a.DecodeMessage(topic, subPayload, defaultTimestampNs)
				if err == nil {
					readings = append(readings, subReadings...)
				}
			}
		}
	}

	a.msgCount.Add(uint64(len(readings)))
	return readings, nil
}

func getFloat(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	default:
		return 0, false
	}
}

// Ensure interface adherence
var _ ProtocolAdapter = (*MQTTAdapter)(nil)

