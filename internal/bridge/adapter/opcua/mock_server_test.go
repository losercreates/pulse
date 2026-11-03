package opcua

import (
	"context"
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/bridge/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMockOPCUAServer_Connect(t *testing.T) {
	srv := startMockOPCUAServer(t)
	defer srv.Close()

	cfg := config.OPCUAAdapterConfig{
		Endpoint:       srv.Endpoint(),
		ConnectTimeout: 2 * time.Second,
		RequestTimeout: 2 * time.Second,
	}

	ch := NewChannel(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := ch.Connect(ctx)
	require.NoError(t, err)
	assert.Equal(t, StateConnected, ch.State())

	err = ch.Close()
	require.NoError(t, err)
	assert.Equal(t, StateDisconnected, ch.State())
}
