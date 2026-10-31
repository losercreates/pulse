package adapter

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/losercreates/pulse/internal/bridge/config"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

// ModbusAdapter implements ProtocolAdapter for cyclic polling of Modbus TCP PLCs.
type ModbusAdapter struct {
	cfg       config.ModbusAdapterConfig
	statusMu  sync.RWMutex
	status    AdapterStatus
	msgCount  atomic.Uint64
	errCount  atomic.Uint64
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	connected atomic.Bool
	transID   atomic.Uint32
}

// NewModbusAdapter constructs a Modbus TCP adapter from configuration.
func NewModbusAdapter(cfg config.ModbusAdapterConfig) (*ModbusAdapter, error) {
	if cfg.Name == "" {
		cfg.Name = "modbus-" + cfg.Endpoint
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 1 * time.Second
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 2 * time.Second
	}
	return &ModbusAdapter{
		cfg: cfg,
	}, nil
}

// Name returns the adapter name.
func (a *ModbusAdapter) Name() string {
	return a.cfg.Name
}

// Status returns runtime connection and metrics.
func (a *ModbusAdapter) Status() AdapterStatus {
	a.statusMu.RLock()
	defer a.statusMu.RUnlock()
	return AdapterStatus{
		Connected:        a.connected.Load(),
		MessagesReceived: a.msgCount.Load(),
		ErrorsTotal:      a.errCount.Load(),
		LastError:        a.status.LastError,
	}
}

func (a *ModbusAdapter) setError(err error) {
	a.errCount.Add(1)
	a.statusMu.Lock()
	a.status.LastError = err.Error()
	a.statusMu.Unlock()
}

// Start launches the cyclic polling goroutine.
func (a *ModbusAdapter) Start(ctx context.Context, out chan<- *telemetryv1.TelemetryReading) error {
	ctx, cancel := context.WithCancel(ctx)
	a.cancel = cancel

	a.wg.Add(1)
	go a.pollLoop(ctx, out)

	return nil
}

// Stop terminates the polling goroutine.
func (a *ModbusAdapter) Stop() error {
	if a.cancel != nil {
		a.cancel()
	}
	a.connected.Store(false)
	a.wg.Wait()
	return nil
}

func (a *ModbusAdapter) pollLoop(ctx context.Context, out chan<- *telemetryv1.TelemetryReading) {
	defer a.wg.Done()

	// If Endpoint is empty, run in idle state (e.g. simulated unit tests)
	if a.cfg.Endpoint == "" {
		a.connected.Store(true)
		<-ctx.Done()
		a.connected.Store(false)
		return
	}

	ticker := time.NewTicker(a.cfg.PollInterval)
	defer ticker.Stop()

	var conn net.Conn
	defer func() {
		if conn != nil {
			_ = conn.Close()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if conn == nil {
				var err error
				conn, err = net.DialTimeout("tcp", a.cfg.Endpoint, a.cfg.Timeout)
				if err != nil {
					a.setError(fmt.Errorf("failed to connect to modbus plc %s: %w", a.cfg.Endpoint, err))
					a.connected.Store(false)
					continue
				}
				a.connected.Store(true)
			}

			// Poll each configured register
			for _, reg := range a.cfg.Registers {
				reading, err := a.pollRegister(conn, reg)
				if err != nil {
					a.setError(err)
					_ = conn.Close()
					conn = nil
					a.connected.Store(false)
					break
				}
				if reading != nil {
					a.msgCount.Add(1)
					select {
					case out <- reading:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}
}

func (a *ModbusAdapter) pollRegister(conn net.Conn, reg config.RegisterDefinition) (*telemetryv1.TelemetryReading, error) {
	fc := reg.FunctionCode
	if fc == 0 {
		fc = 3 // Default FC03 (Holding Registers)
	}

	qty := uint16(1)
	if reg.DataType == "int32" || reg.DataType == "uint32" || reg.DataType == "float32_be" || reg.DataType == "float32_le" || reg.DataType == "float32_swap" {
		qty = 2
	}

	tid := uint16(a.transID.Add(1))
	reqBytes := BuildModbusReadRequest(tid, a.cfg.SlaveID, fc, reg.StartAddress, qty)

	_ = conn.SetDeadline(time.Now().Add(a.cfg.Timeout))
	if _, err := conn.Write(reqBytes); err != nil {
		return nil, fmt.Errorf("modbus write error: %w", err)
	}

	// Read MBAP Header (7 bytes)
	var mbap [7]byte
	if _, err := io.ReadFull(conn, mbap[:]); err != nil {
		return nil, fmt.Errorf("modbus read mbap error: %w", err)
	}

	respLen := binary.BigEndian.Uint16(mbap[4:6])
	if respLen < 2 {
		return nil, fmt.Errorf("invalid modbus response length: %d", respLen)
	}

	// Read PDU
	pdu := make([]byte, respLen-1)
	if _, err := io.ReadFull(conn, pdu); err != nil {
		return nil, fmt.Errorf("modbus read pdu error: %w", err)
	}

	respFC := pdu[0]
	if respFC&0x80 != 0 {
		return nil, fmt.Errorf("modbus exception received: 0x%02x", pdu[1])
	}

	byteCount := int(pdu[1])
	data := pdu[2 : 2+byteCount]

	val, err := DecodeRegisterValue(reg, data)
	if err != nil {
		return nil, err
	}

	return &telemetryv1.TelemetryReading{
		DeviceId:    reg.DeviceID,
		MetricName:  reg.MetricName,
		Value:       val,
		Unit:        reg.Unit,
		TimestampNs: time.Now().UnixNano(),
	}, nil
}

// BuildModbusReadRequest creates a standard Modbus TCP frame (MBAP + PDU).
func BuildModbusReadRequest(transID uint16, slaveID uint8, functionCode uint8, startAddress uint16, quantity uint16) []byte {
	buf := make([]byte, 12)
	// MBAP Header (7 bytes)
	binary.BigEndian.PutUint16(buf[0:2], transID)
	binary.BigEndian.PutUint16(buf[2:4], 0) // Protocol ID (Modbus = 0)
	binary.BigEndian.PutUint16(buf[4:6], 6) // Following bytes (UnitID + PDU = 1 + 5 = 6)
	buf[6] = slaveID

	// PDU (5 bytes)
	buf[7] = functionCode
	binary.BigEndian.PutUint16(buf[8:10], startAddress)
	binary.BigEndian.PutUint16(buf[10:12], quantity)

	return buf
}

// DecodeRegisterValue converts raw bytes into a scaled float64 value based on register definition.
func DecodeRegisterValue(reg config.RegisterDefinition, raw []byte) (float64, error) {
	var rawVal float64

	switch reg.DataType {
	case "int16":
		if len(raw) < 2 {
			return 0, fmt.Errorf("insufficient bytes for int16: %d", len(raw))
		}
		rawVal = float64(int16(binary.BigEndian.Uint16(raw[0:2])))

	case "uint16":
		if len(raw) < 2 {
			return 0, fmt.Errorf("insufficient bytes for uint16: %d", len(raw))
		}
		rawVal = float64(binary.BigEndian.Uint16(raw[0:2]))

	case "int32":
		if len(raw) < 4 {
			return 0, fmt.Errorf("insufficient bytes for int32: %d", len(raw))
		}
		rawVal = float64(int32(binary.BigEndian.Uint32(raw[0:4])))

	case "uint32":
		if len(raw) < 4 {
			return 0, fmt.Errorf("insufficient bytes for uint32: %d", len(raw))
		}
		rawVal = float64(binary.BigEndian.Uint32(raw[0:4]))

	case "float32_be":
		if len(raw) < 4 {
			return 0, fmt.Errorf("insufficient bytes for float32_be: %d", len(raw))
		}
		bits := binary.BigEndian.Uint32(raw[0:4])
		rawVal = float64(math.Float32frombits(bits))

	case "float32_le":
		if len(raw) < 4 {
			return 0, fmt.Errorf("insufficient bytes for float32_le: %d", len(raw))
		}
		bits := binary.LittleEndian.Uint32(raw[0:4])
		rawVal = float64(math.Float32frombits(bits))

	case "float32_swap":
		if len(raw) < 4 {
			return 0, fmt.Errorf("insufficient bytes for float32_swap: %d", len(raw))
		}
		// Mid-Big word swap (CDAB)
		swapBytes := []byte{raw[2], raw[3], raw[0], raw[1]}
		bits := binary.BigEndian.Uint32(swapBytes)
		rawVal = float64(math.Float32frombits(bits))

	default:
		// Default to uint16
		if len(raw) < 2 {
			return 0, fmt.Errorf("insufficient bytes: %d", len(raw))
		}
		rawVal = float64(binary.BigEndian.Uint16(raw[0:2]))
	}

	multiplier := reg.Multiplier
	if multiplier == 0 {
		multiplier = 1.0
	}

	return rawVal*multiplier + reg.Offset, nil
}

// Ensure interface adherence
var _ ProtocolAdapter = (*ModbusAdapter)(nil)

