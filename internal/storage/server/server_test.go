package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/losercreates/pulse/internal/storage/engine"
	"github.com/losercreates/pulse/internal/storage/metrics"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestServer(t *testing.T) (*httptest.Server, *ChronoDBGRPCServer, engine.ChronoEngine) {
	t.Helper()

	eng := engine.NewShardedMemoryEngine(4)
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)

	httpServer := NewHTTPServer(eng, m)
	grpcServer := NewChronoDBGRPCServer(eng, m)

	ts := httptest.NewServer(httpServer.Handler())
	t.Cleanup(func() {
		ts.Close()
		eng.Close()
	})

	return ts, grpcServer, eng
}

func TestHTTPServer_HealthAndReadiness(t *testing.T) {
	ts, _, _ := setupTestServer(t)

	// 1. Healthz
	resp, err := http.Get(ts.URL + "/healthz")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var health map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&health))
	assert.Equal(t, "UP", health["status"])

	// 2. Readyz
	respReady, err := http.Get(ts.URL + "/readyz")
	require.NoError(t, err)
	defer respReady.Body.Close()
	assert.Equal(t, http.StatusOK, respReady.StatusCode)

	var ready map[string]string
	require.NoError(t, json.NewDecoder(respReady.Body).Decode(&ready))
	assert.Equal(t, "READY", ready["status"])
}

func TestHTTPServer_QueryTimeSeries(t *testing.T) {
	ts, _, eng := setupTestServer(t)
	ctx := context.Background()

	// Populate test data
	_, _, err := eng.PutAggregates(ctx, []*telemetryv1.StoredAggregate{
		{
			DeviceId:      "pump-01",
			MetricName:    "pressure",
			WindowStartNs: 1000,
			WindowEndNs:   2000,
			Count:         10,
			Sum:           500.0,
			Min:           48.0,
			Max:           52.0,
			Mean:          50.0,
		},
		{
			DeviceId:      "pump-01",
			MetricName:    "pressure",
			WindowStartNs: 2000,
			WindowEndNs:   3000,
			Count:         10,
			Sum:           520.0,
			Min:           50.0,
			Max:           54.0,
			Mean:          52.0,
		},
	})
	require.NoError(t, err)

	// 1. Valid range query
	resp, err := http.Get(ts.URL + "/api/v1/query?device_id=pump-01&metric_name=pressure&start_time=1000&end_time=2000")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var body queryResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "pump-01", body.DeviceID)
	assert.Equal(t, "pressure", body.MetricName)
	assert.Equal(t, 1, body.TotalRecords)
	assert.Equal(t, int64(1000), body.Records[0].WindowStartNs)

	// 2. Missing parameter (400 Bad Request)
	respBad, err := http.Get(ts.URL + "/api/v1/query?device_id=pump-01")
	require.NoError(t, err)
	defer respBad.Body.Close()
	assert.Equal(t, http.StatusBadRequest, respBad.StatusCode)

	// 3. start_time > end_time (400 Bad Request)
	respInvalid, err := http.Get(ts.URL + "/api/v1/query?device_id=pump-01&metric_name=pressure&start_time=5000&end_time=1000")
	require.NoError(t, err)
	defer respInvalid.Body.Close()
	assert.Equal(t, http.StatusBadRequest, respInvalid.StatusCode)
}

func TestHTTPServer_QueryRollup(t *testing.T) {
	ts, _, eng := setupTestServer(t)
	ctx := context.Background()

	// Populate test data across 2 minutes
	var batch []*telemetryv1.StoredAggregate
	for i := 0; i < 6; i++ {
		batch = append(batch, &telemetryv1.StoredAggregate{
			DeviceId:      "motor-01",
			MetricName:    "rpm",
			WindowStartNs: int64(i * 10_000_000_000),
			WindowEndNs:   int64((i + 1) * 10_000_000_000),
			Count:         100,
			Sum:           150_000.0,
			Min:           1480.0,
			Max:           1520.0,
			Mean:          1500.0,
		})
	}
	_, _, err := eng.PutAggregates(ctx, batch)
	require.NoError(t, err)

	// Rollup query with step=1m
	resp, err := http.Get(ts.URL + "/api/v1/query/rollup?device_id=motor-01&metric_name=rpm&start_time=0&end_time=60000000000&step=1m")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var rollup rollupResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&rollup))
	assert.Equal(t, "motor-01", rollup.DeviceID)
	assert.Equal(t, 1, rollup.TotalBuckets)
	assert.Equal(t, uint64(600), rollup.Buckets[0].Count)
	assert.Equal(t, 1500.0, rollup.Buckets[0].Mean)
}

func TestHTTPServer_DeviceCatalog(t *testing.T) {
	ts, _, eng := setupTestServer(t)
	ctx := context.Background()

	_, _, err := eng.PutAggregates(ctx, []*telemetryv1.StoredAggregate{
		{
			DeviceId:      "sensor-alpha",
			MetricName:    "humidity",
			WindowStartNs: 100,
			WindowEndNs:   200,
			Count:         1,
			Sum:           45.0,
			Min:           45.0,
			Max:           45.0,
			Mean:          45.0,
		},
	})
	require.NoError(t, err)

	resp, err := http.Get(ts.URL + "/api/v1/devices")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var cat catalogResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&cat))
	assert.Equal(t, 1, cat.TotalDevices)
	assert.Equal(t, "sensor-alpha", cat.Devices[0].DeviceId)
}

func TestGRPCServer_QueryEndpoints(t *testing.T) {
	_, grpcSrv, eng := setupTestServer(t)
	ctx := context.Background()

	_, _, err := eng.PutAggregates(ctx, []*telemetryv1.StoredAggregate{
		{
			DeviceId:      "grpc-dev",
			MetricName:    "power",
			WindowStartNs: 500,
			WindowEndNs:   1000,
			Count:         20,
			Sum:           2000.0,
			Min:           90.0,
			Max:           110.0,
			Mean:          100.0,
		},
	})
	require.NoError(t, err)

	// 1. Time-series query
	res, err := grpcSrv.QueryTimeSeries(ctx, &telemetryv1.QueryTimeSeriesRequest{
		DeviceId:    "grpc-dev",
		MetricName:  "power",
		StartTimeNs: 0,
		EndTimeNs:   2000,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(1), res.TotalCount)
	assert.Equal(t, "grpc-dev", res.Records[0].DeviceId)

	// 2. Rollup query
	rollupRes, err := grpcSrv.QueryRollup(ctx, &telemetryv1.QueryRollupRequest{
		DeviceId:    "grpc-dev",
		MetricName:  "power",
		StartTimeNs: 0,
		EndTimeNs:   2000,
		StepNs:      5000,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(1), rollupRes.TotalBuckets)

	// 3. Catalog query
	catRes, err := grpcSrv.GetDeviceCatalog(ctx, &telemetryv1.GetDeviceCatalogRequest{Limit: 10})
	require.NoError(t, err)
	require.Len(t, catRes.Devices, 1)
	assert.Equal(t, "grpc-dev", catRes.Devices[0].DeviceId)
}
