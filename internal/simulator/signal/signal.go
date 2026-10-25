package signal

import (
	"math"
	"math/rand/v2"
	"sync"
	"time"
)

// Generator synthesizes continuous physical sensor signals combining sinusoidal oscillations,
// Gaussian white noise, and baseline drift.
type Generator struct {
	mu           sync.Mutex
	base         float64
	amplitude    float64
	frequency    float64 // in Hz
	noiseSigma   float64
	phase        float64
	driftRate    float64
	totalDrift   float64
	frozen       bool
	frozenVal    float64
}

// NewGenerator creates a physical sensor signal generator.
func NewGenerator(base, amplitude, frequency, noiseSigma float64) *Generator {
	return &Generator{
		base:       base,
		amplitude:  amplitude,
		frequency:  frequency,
		noiseSigma: noiseSigma,
		phase:      rand.Float64() * 2 * math.Pi,
	}
}

// Next calculates the next sensor reading value at the specified elapsed time offset.
func (g *Generator) Next(elapsed time.Duration) float64 {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.frozen {
		return g.frozenVal
	}

	tSec := elapsed.Seconds()
	// Sinusoidal wave component
	sineComponent := g.amplitude * math.Sin(2*math.Pi*g.frequency*tSec+g.phase)

	// Gaussian white noise component (Box-Muller or math/rand NormFloat64)
	noise := rand.NormFloat64() * g.noiseSigma

	// Drift component
	g.totalDrift += g.driftRate

	val := g.base + sineComponent + noise + g.totalDrift
	return val
}

// SetDrift sets a continuous drift rate added per reading.
func (g *Generator) SetDrift(driftRate float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.driftRate = driftRate
}

// Freeze freezes the generator to output an unvarying constant (zero variance flatline).
func (g *Generator) Freeze(constantVal float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.frozen = true
	g.frozenVal = constantVal
}

// Unfreeze resumes dynamic signal generation.
func (g *Generator) Unfreeze() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.frozen = false
}

// BaseValue returns the nominal central operating value.
func (g *Generator) BaseValue() float64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.base
}

// NoiseSigma returns the noise standard deviation.
func (g *Generator) NoiseSigma() float64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.noiseSigma
}
