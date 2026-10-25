package fleet

import (
	"fmt"
	"sync"
	"time"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

// Device represents an individual simulated IoT sensor node.
type Device struct {
	mu              sync.Mutex
	ID              string
	Archetype       Archetype
	Metrics         []MetricDef
	lastTimestampNs int64
	offline         bool
	offlineBuffer   []*telemetryv1.TelemetryReading
}

// NextReading generates a telemetry reading for the device at the given timestamp.
func (d *Device) NextReading(timestampNs int64, elapsed time.Duration, metricIdx int) *telemetryv1.TelemetryReading {
	d.mu.Lock()
	defer d.mu.Unlock()

	if len(d.Metrics) == 0 {
		return nil
	}

	m := d.Metrics[metricIdx%len(d.Metrics)]
	val := m.Generator.Next(elapsed)
	d.lastTimestampNs = timestampNs

	reading := &telemetryv1.TelemetryReading{
		DeviceId:    d.ID,
		TimestampNs: timestampNs,
		MetricName:  m.Name,
		Value:       val,
		Unit:        m.Unit,
	}

	if d.offline {
		// Buffer reading if offline
		if len(d.offlineBuffer) < 5000 {
			d.offlineBuffer = append(d.offlineBuffer, reading)
		}
		return nil
	}

	return reading
}

// SetOffline toggles offline state.
func (d *Device) SetOffline(offline bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.offline = offline
}

// IsOffline checks if the device is disconnected.
func (d *Device) IsOffline() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.offline
}

// DrainOfflineBuffer returns and clears buffered readings accumulated during disconnect.
func (d *Device) DrainOfflineBuffer() []*telemetryv1.TelemetryReading {
	d.mu.Lock()
	defer d.mu.Unlock()

	if len(d.offlineBuffer) == 0 {
		return nil
	}

	buf := d.offlineBuffer
	d.offlineBuffer = nil
	return buf
}

// Freeze halts signal oscillations across device metrics and locks value to constantVal.
func (d *Device) Freeze(constantVal float64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, m := range d.Metrics {
		if m.Generator != nil {
			m.Generator.Freeze(constantVal)
		}
	}
}

// Unfreeze resumes dynamic signal oscillations for the device.
func (d *Device) Unfreeze() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, m := range d.Metrics {
		if m.Generator != nil {
			m.Generator.Unfreeze()
		}
	}
}

// SetDrift applies a persistent baseline drift rate across device metric generators.
func (d *Device) SetDrift(driftRate float64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, m := range d.Metrics {
		if m.Generator != nil {
			m.Generator.SetDrift(driftRate)
		}
	}
}

// Fleet manages the entire topology of virtual devices.
type Fleet struct {
	devices []*Device
}

// NewFleet initializes a fleet of numDevices partitioned across 4 archetypes.
func NewFleet(numDevices int) *Fleet {
	if numDevices <= 0 {
		numDevices = 1000
	}

	devices := make([]*Device, numDevices)
	archetypes := []Archetype{ArchetypeTurbine, ArchetypeMotor, ArchetypePump, ArchetypeGenerator}

	for i := 0; i < numDevices; i++ {
		arch := archetypes[i%len(archetypes)]
		id := fmt.Sprintf("%s-%05d", arch, i)
		devices[i] = &Device{
			ID:        id,
			Archetype: arch,
			Metrics:   GetArchetypeMetrics(arch),
		}
	}

	return &Fleet{
		devices: devices,
	}
}

// TotalDevices returns total count of devices in the fleet.
func (f *Fleet) TotalDevices() int {
	return len(f.devices)
}

// Devices returns the slice of all devices.
func (f *Fleet) Devices() []*Device {
	return f.devices
}

// WorkerPartitions partitions the device fleet uniformly across numWorkers.
func (f *Fleet) WorkerPartitions(numWorkers int) [][]*Device {
	if numWorkers <= 0 {
		numWorkers = 1
	}

	partitions := make([][]*Device, numWorkers)
	for i := 0; i < numWorkers; i++ {
		partitions[i] = make([]*Device, 0, len(f.devices)/numWorkers+1)
	}

	for i, dev := range f.devices {
		w := i % numWorkers
		partitions[w] = append(partitions[w], dev)
	}

	return partitions
}
