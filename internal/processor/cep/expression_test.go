package cep_test

import (
	"testing"

	"github.com/losercreates/pulse/internal/processor/cep"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExpressionEvaluator_Arithmetic(t *testing.T) {
	eval := cep.NewExpressionEvaluator()

	values := map[string]float64{
		"power_mw":     85.0,
		"fuel_flow":    100.0,
		"ambient_temp": 25.0,
		"multiplier":   2.0,
	}

	// Simple division
	val, err := eval.Evaluate("power_mw / fuel_flow", values)
	require.NoError(t, err)
	assert.InDelta(t, 0.85, val, 1e-4)

	// Precedence: 85 + 25 * 2 = 85 + 50 = 135
	val2, err := eval.Evaluate("power_mw + ambient_temp * multiplier", values)
	require.NoError(t, err)
	assert.InDelta(t, 135.0, val2, 1e-4)

	// Parentheses: (85 + 25) * 2 = 110 * 2 = 220
	val3, err := eval.Evaluate("(power_mw + ambient_temp) * multiplier", values)
	require.NoError(t, err)
	assert.InDelta(t, 220.0, val3, 1e-4)
}

func TestExpressionEvaluator_Condition(t *testing.T) {
	eval := cep.NewExpressionEvaluator()

	values := map[string]float64{
		"power_mw":  60.0,
		"fuel_flow": 100.0, // ratio = 0.60
	}

	// 0.60 < 0.70 should be true
	match, err := eval.EvaluateCondition("power_mw / fuel_flow < 0.70", values)
	require.NoError(t, err)
	assert.True(t, match)

	// 0.60 > 0.70 should be false
	matchFalse, err := eval.EvaluateCondition("power_mw / fuel_flow > 0.70", values)
	require.NoError(t, err)
	assert.False(t, matchFalse)

	// Missing variable
	_, errMissing := eval.EvaluateCondition("unknown_metric > 10", values)
	assert.ErrorIs(t, errMissing, cep.ErrMissingVariable)

	// Division by zero
	zeroValues := map[string]float64{"a": 10.0, "b": 0.0}
	_, errDivZero := eval.EvaluateCondition("a / b > 1.0", zeroValues)
	assert.ErrorIs(t, errDivZero, cep.ErrDivisionByZero)
}

