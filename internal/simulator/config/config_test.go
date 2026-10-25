package config_test

import (
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/simulator/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_DefaultsAndParsing(t *testing.T) {
	cfg, err := config.Parse([]string{})
	require.NoError(t, err)

	assert.Equal(t, 50000, cfg.TargetRate)
	assert.Equal(t, 1000, cfg.NumDevices)
	assert.Equal(t, 60*time.Second, cfg.Duration)
	assert.Equal(t, "grpc", cfg.Transport)
	assert.Equal(t, "steady", cfg.Profile)

	// Custom arguments
	custom, err := config.Parse([]string{
		"-r", "25000",
		"-d", "500",
		"-t", "10s",
		"--transport", "http",
		"--profile", "burst",
		"--burst-multiplier", "4.0",
	})
	require.NoError(t, err)
	assert.Equal(t, 25000, custom.TargetRate)
	assert.Equal(t, 500, custom.NumDevices)
	assert.Equal(t, 10*time.Second, custom.Duration)
	assert.Equal(t, "http", custom.Transport)
	assert.Equal(t, "burst", custom.Profile)
	assert.Equal(t, 4.0, custom.BurstMultiplier)
}
