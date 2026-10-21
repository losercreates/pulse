package window

import (
	"time"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

// WindowManager coordinates partition-local window lifecycle, record accumulation,
// and deterministic watermark-triggered emission with zero cross-thread locking.
type WindowManager struct {
	partitionID int32
	boundary    *WindowBoundaryCalculator
	windows     map[WindowKey]*WindowAccumulator
	pq          *WindowPriorityQueue
}

// NewWindowManager creates an in-memory window manager for a single partition.
func NewWindowManager(partitionID int32, boundary *WindowBoundaryCalculator) *WindowManager {
	return &WindowManager{
		partitionID: partitionID,
		boundary:    boundary,
		windows:     make(map[WindowKey]*WindowAccumulator),
		pq:          NewWindowPriorityQueue(),
	}
}

// ProcessReading routes a single sensor measurement to all applicable open windows.
func (m *WindowManager) ProcessReading(eventTimeNs int64, deviceID, metricName string, value float64) {
	keys := m.boundary.AssignAllWindows(eventTimeNs, deviceID, metricName)
	for _, key := range keys {
		acc, exists := m.windows[key]
		if !exists {
			acc = NewAccumulator(key)
			m.windows[key] = acc
			m.pq.Push(acc)
		}
		acc.Add(value, deviceID)
	}
}

// AdvanceWatermark evaluates open windows against the advanced event-time watermark threshold.
// Any window whose WindowEndNs <= watermarkNs is closed, evicted from memory,
// and converted into a WindowAggregate record.
func (m *WindowManager) AdvanceWatermark(watermarkNs int64) []*telemetryv1.WindowAggregate {
	nowNs := time.Now().UnixNano()
	var emitted []*telemetryv1.WindowAggregate

	for m.pq.Len() > 0 {
		earliest := m.pq.Peek()
		if earliest == nil || earliest.Key.WindowEndNs > watermarkNs {
			break
		}

		// Window end has been crossed by the event-time watermark -> Close and evict!
		acc := m.pq.Pop()
		delete(m.windows, acc.Key)

		agg := &telemetryv1.WindowAggregate{
			DeviceId:            acc.Key.DeviceID,
			MetricName:          acc.Key.MetricName,
			WindowStartNs:       acc.Key.WindowStartNs,
			WindowEndNs:         acc.Key.WindowEndNs,
			Count:               acc.Count,
			Sum:                 acc.Sum,
			Min:                 acc.Min,
			Max:                 acc.Max,
			Mean:                acc.Mean(),
			ApproxDistinctCount: acc.ApproxDistinct(),
			EmittedAtNs:         nowNs,
			PartitionId:         m.partitionID,
		}
		emitted = append(emitted, agg)
	}

	return emitted
}

// FlushAll forcibly closes and emits all open windows regardless of watermark.
// Typically used during graceful shutdown or partition revocation.
func (m *WindowManager) FlushAll() []*telemetryv1.WindowAggregate {
	nowNs := time.Now().UnixNano()
	var emitted []*telemetryv1.WindowAggregate

	for m.pq.Len() > 0 {
		acc := m.pq.Pop()
		delete(m.windows, acc.Key)

		agg := &telemetryv1.WindowAggregate{
			DeviceId:            acc.Key.DeviceID,
			MetricName:          acc.Key.MetricName,
			WindowStartNs:       acc.Key.WindowStartNs,
			WindowEndNs:         acc.Key.WindowEndNs,
			Count:               acc.Count,
			Sum:                 acc.Sum,
			Min:                 acc.Min,
			Max:                 acc.Max,
			Mean:                acc.Mean(),
			ApproxDistinctCount: acc.ApproxDistinct(),
			EmittedAtNs:         nowNs,
			PartitionId:         m.partitionID,
		}
		emitted = append(emitted, agg)
	}

	return emitted
}

// OpenWindowsCount returns the current count of open in-memory windows.
func (m *WindowManager) OpenWindowsCount() int {
	return len(m.windows)
}
