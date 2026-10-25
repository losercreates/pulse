package fleet

import "github.com/losercreates/pulse/internal/simulator/signal"

// Archetype defines a physical industrial device category.
type Archetype string

const (
	ArchetypeTurbine   Archetype = "turbine"
	ArchetypeMotor     Archetype = "motor"
	ArchetypePump      Archetype = "pump"
	ArchetypeGenerator Archetype = "generator"
)

// MetricDef defines a sensor measurement channel.
type MetricDef struct {
	Name      string
	Unit      string
	Generator *signal.Generator
}

// GetArchetypeMetrics instantiates the sensor metric definitions for a given device archetype.
func GetArchetypeMetrics(arch Archetype) []MetricDef {
	switch arch {
	case ArchetypeTurbine:
		return []MetricDef{
			{Name: "vibration", Unit: "mm/s", Generator: signal.NewGenerator(4.5, 0.5, 0.05, 0.25)},
			{Name: "bearing_temp", Unit: "celsius", Generator: signal.NewGenerator(65.0, 3.0, 0.01, 0.5)},
			{Name: "rotor_rpm", Unit: "rpm", Generator: signal.NewGenerator(15.0, 2.0, 0.02, 0.1)},
		}
	case ArchetypeMotor:
		return []MetricDef{
			{Name: "vibration", Unit: "mm/s", Generator: signal.NewGenerator(2.8, 0.3, 0.08, 0.15)},
			{Name: "current_amp", Unit: "ampere", Generator: signal.NewGenerator(45.0, 4.0, 0.02, 0.8)},
			{Name: "power_mw", Unit: "mw", Generator: signal.NewGenerator(1.2, 0.1, 0.02, 0.02)},
		}
	case ArchetypePump:
		return []MetricDef{
			{Name: "oil_pressure", Unit: "psi", Generator: signal.NewGenerator(120.0, 8.0, 0.05, 1.5)},
			{Name: "temp", Unit: "celsius", Generator: signal.NewGenerator(55.0, 2.5, 0.01, 0.4)},
		}
	case ArchetypeGenerator:
		fallthrough
	default:
		return []MetricDef{
			{Name: "voltage_kv", Unit: "kv", Generator: signal.NewGenerator(13.8, 0.2, 0.01, 0.05)},
			{Name: "frequency_hz", Unit: "hz", Generator: signal.NewGenerator(60.0, 0.05, 0.1, 0.01)},
			{Name: "bearing_temp", Unit: "celsius", Generator: signal.NewGenerator(70.0, 3.5, 0.01, 0.5)},
		}
	}
}
