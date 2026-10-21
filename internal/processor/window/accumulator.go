package window

import (
	"hash/fnv"
	"math"
	"math/bits"
)

const (
	// HLLRegistersCount is 64 registers (p=6 bits).
	HLLRegistersCount = 64
	// HLLAlpha for m=64 is 0.709.
	HLLAlpha = 0.709
)

// WindowAccumulator maintains single-pass running statistical aggregates
// and a 64-register HyperLogLog sketch for an open window in partition-local memory.
type WindowAccumulator struct {
	Key          WindowKey
	Count        uint64
	Sum          float64
	Min          float64
	Max          float64
	HLLRegisters [HLLRegistersCount]uint8
	HeapIndex    int // Internal index in the partition min-heap priority queue
}

// NewAccumulator initializes a new WindowAccumulator for the given WindowKey.
func NewAccumulator(key WindowKey) *WindowAccumulator {
	return &WindowAccumulator{
		Key:       key,
		HeapIndex: -1,
	}
}

// Add updates the accumulator with a new measurement reading.
func (a *WindowAccumulator) Add(value float64, distinctDeviceID string) {
	if a.Count == 0 {
		a.Min = value
		a.Max = value
	} else {
		if value < a.Min {
			a.Min = value
		}
		if value > a.Max {
			a.Max = value
		}
	}

	a.Count++
	a.Sum += value

	// Update HyperLogLog sketch
	if distinctDeviceID != "" {
		a.addHLL(distinctDeviceID)
	}
}

// Mean returns the computed arithmetic mean (sum / count).
func (a *WindowAccumulator) Mean() float64 {
	if a.Count == 0 {
		return 0
	}
	return a.Sum / float64(a.Count)
}

func (a *WindowAccumulator) addHLL(key string) {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	hashVal := h.Sum64()

	// 6 bits for register index (2^6 = 64)
	regIdx := hashVal & 0x3F

	// Remaining 58 bits for leading zeros count
	w := hashVal >> 6
	var rank uint8
	if w == 0 {
		rank = 59
	} else {
		// bits.LeadingZeros64(w) has at least 6 leading zeros because w is 58 bits
		lz := bits.LeadingZeros64(w) - 6
		rank = uint8(lz + 1)
	}

	if rank > a.HLLRegisters[regIdx] {
		a.HLLRegisters[regIdx] = rank
	}
}

// ApproxDistinct returns the estimated distinct element count from the 64-register HLL sketch.
func (a *WindowAccumulator) ApproxDistinct() uint64 {
	if a.Count == 0 {
		return 0
	}

	var sum float64
	zeros := 0
	for _, reg := range a.HLLRegisters {
		sum += 1.0 / math.Pow(2.0, float64(reg))
		if reg == 0 {
			zeros++
		}
	}

	m := float64(HLLRegistersCount)
	rawEstimate := (HLLAlpha * m * m) / sum

	// Linear counting for small cardinalities (E <= 2.5 * m = 160)
	if rawEstimate <= 2.5*m && zeros > 0 {
		return uint64(math.Round(m * math.Log(m/float64(zeros))))
	}

	return uint64(math.Round(rawEstimate))
}
