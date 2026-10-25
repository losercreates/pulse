package chaos

import (
	"context"
	"sync"
	"time"

	"github.com/losercreates/pulse/internal/simulator/fleet"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

// OfflineController manages network disconnect chaos and out-of-order replay flushes.
type OfflineController struct {
	fleet          *fleet.Fleet
	offlinePct     float64
	interval       time.Duration
	duration       time.Duration
	mu             sync.RWMutex
	offlineDevices []*fleet.Device
}

// NewOfflineController creates a network disconnect chaos manager.
func NewOfflineController(f *fleet.Fleet, offlinePct float64, interval, duration time.Duration) *OfflineController {
	if offlinePct <= 0 {
		offlinePct = 0.05
	}
	if interval <= 0 {
		interval = 30 * time.Second
	}
	if duration <= 0 {
		duration = 15 * time.Second
	}

	return &OfflineController{
		fleet:      f,
		offlinePct: offlinePct,
		interval:   interval,
		duration:   duration,
	}
}

// ActiveOfflineCount returns the number of currently disconnected devices.
func (c *OfflineController) ActiveOfflineCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.offlineDevices)
}

// DrainAllBuffers collects and drains buffered telemetry from all fleet devices.
func (c *OfflineController) DrainAllBuffers() []*telemetryv1.TelemetryReading {
	var backfill []*telemetryv1.TelemetryReading
	for _, dev := range c.fleet.Devices() {
		buf := dev.DrainOfflineBuffer()
		if len(buf) > 0 {
			backfill = append(backfill, buf...)
		}
	}
	return backfill
}

// Run periodically triggers network disconnects across a fraction of the fleet.
func (c *OfflineController) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			c.reconnectAll()
			return
		case <-time.After(c.interval):
			c.disconnectFraction()

			select {
			case <-ctx.Done():
				c.reconnectAll()
				return
			case <-time.After(c.duration):
				c.reconnectAll()
			}
		}
	}
}

func (c *OfflineController) disconnectFraction() {
	c.mu.Lock()
	defer c.mu.Unlock()

	all := c.fleet.Devices()
	targetCount := int(float64(len(all)) * c.offlinePct)
	if targetCount < 1 && len(all) > 0 {
		targetCount = 1
	}

	c.offlineDevices = make([]*fleet.Device, 0, targetCount)
	for i := 0; i < targetCount && i < len(all); i++ {
		dev := all[i]
		dev.SetOffline(true)
		c.offlineDevices = append(c.offlineDevices, dev)
	}
}

func (c *OfflineController) reconnectAll() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, dev := range c.offlineDevices {
		dev.SetOffline(false)
	}
	c.offlineDevices = nil
}
