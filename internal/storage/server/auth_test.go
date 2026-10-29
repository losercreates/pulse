package server_test

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/auth"
	"github.com/losercreates/pulse/internal/storage/engine"
	"github.com/losercreates/pulse/internal/storage/server"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestChronoDBHTTPAuthenticationAndRBAC(t *testing.T) {
	jwtSecret := []byte("pulse-storage-test-secret-key-32b")
	validator := auth.NewHS256Validator(jwtSecret, "pulse-auth", "pulse-chronodb")
	audit := &auth.NopAuditLogger{}

	eng := engine.NewShardedMemoryEngine(4)
	now := time.Now().UnixNano()

	// Seed test data
	_, _, _ = eng.PutAggregates(context.Background(), []*telemetryv1.StoredAggregate{
		{
			DeviceId:    "turbine-alpha-01",
			MetricName:  "temp",
			WindowStartNs: now - int64(10*time.Minute),
			WindowEndNs:   now,
			Count:       10,
			Min:         20.0,
			Max:         30.0,
			Sum:         250.0,
			Mean:        25.0,
		},
		{
			DeviceId:    "turbine-beta-01",
			MetricName:  "temp",
			WindowStartNs: now - int64(10*time.Minute),
			WindowEndNs:   now,
			Count:       10,
			Min:         50.0,
			Max:         60.0,
			Sum:         550.0,
			Mean:        55.0,
		},
	})

	s := server.NewHTTPServer(eng, nil).WithAuth(validator, audit)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	// Helper to generate test JWT
	makeToken := func(role auth.Role, allowedDevices []string) string {
		claims := auth.TokenClaims{
			Issuer:         "pulse-auth",
			Subject:        "user@pulse.internal",
			Audience:       "pulse-chronodb",
			Role:           role,
			AllowedDevices: allowedDevices,
			ExpiresAt:      time.Now().Unix() + 3600,
		}
		tok, err := auth.SignHS256(claims, jwtSecret)
		require.NoError(t, err)
		return tok
	}

	viewerAlphaToken := makeToken(auth.RoleViewer, []string{"turbine-alpha-*"})
	operatorToken := makeToken(auth.RoleOperator, []string{"*"})

	t.Run("SC-003: Unauthenticated query rejected with 401", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/api/v1/query?device_id=turbine-alpha-01&metric_name=temp&start_time=0&end_time=" + string(rune(now)))
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})

	t.Run("SC-003: Viewer permitted to query allowed device in scope", func(t *testing.T) {
		req, _ := http.NewRequest("GET", ts.URL+"/api/v1/query?device_id=turbine-alpha-01&metric_name=temp&start_time=0&end_time=999999999999999999", nil)
		req.Header.Set("Authorization", "Bearer "+viewerAlphaToken)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("SC-003: Viewer forbidden from querying device outside scope (403)", func(t *testing.T) {
		req, _ := http.NewRequest("GET", ts.URL+"/api/v1/query?device_id=turbine-beta-01&metric_name=temp&start_time=0&end_time=999999999999999999", nil)
		req.Header.Set("Authorization", "Bearer "+viewerAlphaToken)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	})

	t.Run("SC-003: Viewer forbidden from querying rollup (requires Operator role)", func(t *testing.T) {
		req, _ := http.NewRequest("GET", ts.URL+"/api/v1/query/rollup?device_id=turbine-alpha-01&metric_name=temp&start_time=0&end_time=999999999999999999&step=1m", nil)
		req.Header.Set("Authorization", "Bearer "+viewerAlphaToken)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	})

	t.Run("Operator permitted to query rollup", func(t *testing.T) {
		req, _ := http.NewRequest("GET", ts.URL+"/api/v1/query/rollup?device_id=turbine-alpha-01&metric_name=temp&start_time=0&end_time=999999999999999999&step=1m", nil)
		req.Header.Set("Authorization", "Bearer "+operatorToken)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("Device catalog tenant isolation", func(t *testing.T) {
		req, _ := http.NewRequest("GET", ts.URL+"/api/v1/devices", nil)
		req.Header.Set("Authorization", "Bearer "+viewerAlphaToken)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var catalog struct {
			TotalDevices int `json:"total_devices"`
			Devices      []struct {
				DeviceID string `json:"device_id"`
			} `json:"devices"`
		}
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&catalog))
		assert.Equal(t, 1, catalog.TotalDevices)
		assert.Equal(t, "turbine-alpha-01", catalog.Devices[0].DeviceID)
	})
}

func TestChronoDBGRPCAuthenticationAndRBAC(t *testing.T) {
	jwtSecret := []byte("pulse-storage-test-secret-key-32b")
	validator := auth.NewHS256Validator(jwtSecret, "pulse-auth", "pulse-chronodb")
	audit := &auth.NopAuditLogger{}

	eng := engine.NewShardedMemoryEngine(4)
	now := time.Now().UnixNano()

	_, _, _ = eng.PutAggregates(context.Background(), []*telemetryv1.StoredAggregate{
		{
			DeviceId:    "turbine-alpha-01",
			MetricName:  "temp",
			WindowStartNs: now - int64(10*time.Minute),
			WindowEndNs:   now,
			Count:       5,
		},
	})

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	chronoServer := server.NewChronoDBGRPCServer(eng, nil).WithAuth(validator, audit)
	telemetryv1.RegisterChronoDBServiceServer(grpcServer, chronoServer)

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := telemetryv1.NewChronoDBServiceClient(conn)

	makeToken := func(role auth.Role, allowedDevices []string) string {
		claims := auth.TokenClaims{
			Issuer:         "pulse-auth",
			Subject:        "user@pulse.internal",
			Audience:       "pulse-chronodb",
			Role:           role,
			AllowedDevices: allowedDevices,
			ExpiresAt:      time.Now().Unix() + 3600,
		}
		tok, err := auth.SignHS256(claims, jwtSecret)
		require.NoError(t, err)
		return tok
	}

	viewerToken := makeToken(auth.RoleViewer, []string{"turbine-alpha-*"})
	operatorToken := makeToken(auth.RoleOperator, []string{"*"})

	t.Run("gRPC unauthenticated query rejected with Unauthenticated", func(t *testing.T) {
		_, err := client.QueryTimeSeries(ctx, &telemetryv1.QueryTimeSeriesRequest{
			DeviceId:    "turbine-alpha-01",
			MetricName:  "temp",
			StartTimeNs: 0,
			EndTimeNs:   now,
		})
		require.Error(t, err)
		st, ok := status.FromError(err)
		assert.True(t, ok)
		assert.Equal(t, codes.Unauthenticated, st.Code())
	})

	t.Run("gRPC Viewer permitted to query allowed device in scope", func(t *testing.T) {
		authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+viewerToken)
		res, err := client.QueryTimeSeries(authCtx, &telemetryv1.QueryTimeSeriesRequest{
			DeviceId:    "turbine-alpha-01",
			MetricName:  "temp",
			StartTimeNs: 0,
			EndTimeNs:   now,
		})
		require.NoError(t, err)
		assert.Equal(t, uint32(1), res.TotalCount)
	})

	t.Run("gRPC Viewer forbidden from querying rollup (PermissionDenied)", func(t *testing.T) {
		authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+viewerToken)
		_, err := client.QueryRollup(authCtx, &telemetryv1.QueryRollupRequest{
			DeviceId:    "turbine-alpha-01",
			MetricName:  "temp",
			StartTimeNs: 0,
			EndTimeNs:   now,
			StepNs:      60_000_000_000,
		})
		require.Error(t, err)
		st, ok := status.FromError(err)
		assert.True(t, ok)
		assert.Equal(t, codes.PermissionDenied, st.Code())
	})

	t.Run("gRPC Operator permitted to query rollup", func(t *testing.T) {
		authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+operatorToken)
		res, err := client.QueryRollup(authCtx, &telemetryv1.QueryRollupRequest{
			DeviceId:    "turbine-alpha-01",
			MetricName:  "temp",
			StartTimeNs: 0,
			EndTimeNs:   now,
			StepNs:      60_000_000_000,
		})
		require.NoError(t, err)
		assert.NotNil(t, res)
	})
}
