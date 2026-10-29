package server_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/auth"
	"github.com/losercreates/pulse/internal/gateway/server"
	"github.com/losercreates/pulse/internal/gateway/service"
	"github.com/losercreates/pulse/internal/gateway/validator"
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

type mockProducer struct {
	published []*telemetryv1.TelemetryReading
}

func (m *mockProducer) Publish(ctx context.Context, reading *telemetryv1.TelemetryReading) error {
	m.published = append(m.published, reading)
	return nil
}

func (m *mockProducer) PublishBatch(ctx context.Context, readings []*telemetryv1.TelemetryReading) (int, error) {
	m.published = append(m.published, readings...)
	return len(readings), nil
}

func (m *mockProducer) Ping(ctx context.Context) error { return nil }
func (m *mockProducer) Close()                         {}

func TestHTTPAuthenticationAndAntiSpoofing(t *testing.T) {
	keyVal := auth.NewAPIKeyValidator(
		auth.APIKeyEntry{Key: "turbine-01-key", DeviceID: "turbine-01"},
		auth.APIKeyEntry{Key: "turbine-02-key", DeviceID: "turbine-02"},
	)
	authenticator := auth.NewGatewayAuthenticator(keyVal)
	rateLimiter := auth.NewTokenBucketRateLimiter(100, 50)
	audit := &auth.NopAuditLogger{}

	val := validator.New(5*time.Minute, 24*time.Hour)
	prod := &mockProducer{}
	svc := service.NewIngestionService(val, prod, nil)

	s := server.NewHTTPServer(svc, prod, "telemetry.raw",
		server.WithHTTPAuth(authenticator, rateLimiter, audit),
	)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	now := time.Now().UnixNano()

	t.Run("SC-001: Missing credentials rejected with 401 Unauthorized", func(t *testing.T) {
		body, _ := json.Marshal(map[string]any{
			"device_id":    "turbine-01",
			"timestamp_ns": now,
			"metric_name":  "temp",
			"value":        42.0,
		})

		resp, err := http.Post(ts.URL+"/api/v1/telemetry", "application/json", bytes.NewReader(body))
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})

	t.Run("Valid API key with matching device_id accepted with 202 Accepted", func(t *testing.T) {
		body, _ := json.Marshal(map[string]any{
			"device_id":    "turbine-01",
			"timestamp_ns": now,
			"metric_name":  "temp",
			"value":        42.0,
		})

		req, _ := http.NewRequest("POST", ts.URL+"/api/v1/telemetry", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer turbine-01-key")
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusAccepted, resp.StatusCode)
	})

	t.Run("SC-001: Anti-spoofing rejects mismatched device_id with 403 Forbidden", func(t *testing.T) {
		// Device turbine-01 tries to submit reading for turbine-02
		body, _ := json.Marshal(map[string]any{
			"device_id":    "turbine-02", // Spoofed!
			"timestamp_ns": now,
			"metric_name":  "temp",
			"value":        99.9,
		})

		req, _ := http.NewRequest("POST", ts.URL+"/api/v1/telemetry", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer turbine-01-key")
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	})

	t.Run("Anti-spoofing rejects batch with any spoofed device_id", func(t *testing.T) {
		body, _ := json.Marshal(map[string]any{
			"readings": []map[string]any{
				{
					"device_id":    "turbine-01",
					"timestamp_ns": now,
					"metric_name":  "temp",
					"value":        42.0,
				},
				{
					"device_id":    "turbine-02", // Spoofed!
					"timestamp_ns": now,
					"metric_name":  "pressure",
					"value":        10.5,
				},
			},
		})

		req, _ := http.NewRequest("POST", ts.URL+"/api/v1/telemetry/batch", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer turbine-01-key")
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	})

	t.Run("mTLS peer certificate authentication and anti-spoofing", func(t *testing.T) {
		cert := &x509.Certificate{
			SerialNumber: big.NewInt(1),
			Subject:      pkix.Name{CommonName: "turbine-01"},
		}

		// Inject TLS peer certificate in request
		body, _ := json.Marshal(map[string]any{
			"device_id":    "turbine-01",
			"timestamp_ns": now,
			"metric_name":  "temp",
			"value":        50.0,
		})
		req, _ := http.NewRequest("POST", "/api/v1/telemetry", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.TLS = &tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{cert},
		}

		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)
		assert.Equal(t, http.StatusAccepted, rec.Code)

		// Now test spoofing via mTLS
		spoofBody, _ := json.Marshal(map[string]any{
			"device_id":    "turbine-99", // Mismatch
			"timestamp_ns": now,
			"metric_name":  "temp",
			"value":        50.0,
		})
		reqSpoof, _ := http.NewRequest("POST", "/api/v1/telemetry", bytes.NewReader(spoofBody))
		reqSpoof.Header.Set("Content-Type", "application/json")
		reqSpoof.TLS = &tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{cert},
		}

		recSpoof := httptest.NewRecorder()
		s.Handler().ServeHTTP(recSpoof, reqSpoof)
		assert.Equal(t, http.StatusForbidden, recSpoof.Code)
	})
}

func TestGRPCAuthenticationAndAntiSpoofing(t *testing.T) {
	keyVal := auth.NewAPIKeyValidator(
		auth.APIKeyEntry{Key: "turbine-01-key", DeviceID: "turbine-01"},
	)
	authenticator := auth.NewGatewayAuthenticator(keyVal)
	rateLimiter := auth.NewTokenBucketRateLimiter(100, 50)
	audit := &auth.NopAuditLogger{}

	val := validator.New(5*time.Minute, 24*time.Hour)
	prod := &mockProducer{}
	svc := service.NewIngestionService(val, prod, nil)

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(auth.GatewayGRPCUnaryInterceptor(authenticator, rateLimiter, audit)),
		grpc.StreamInterceptor(auth.GatewayGRPCStreamInterceptor(authenticator, rateLimiter, audit)),
	)
	serverObj := server.NewGRPCServer(svc, server.WithGRPCAuth(authenticator, audit))
	telemetryv1.RegisterIngestionServiceServer(grpcServer, serverObj)

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

	client := telemetryv1.NewIngestionServiceClient(conn)
	now := time.Now().UnixNano()

	t.Run("gRPC unauthenticated call rejected", func(t *testing.T) {
		_, err := client.IngestReading(ctx, &telemetryv1.TelemetryReading{
			DeviceId:    "turbine-01",
			TimestampNs: now,
			MetricName:  "temp",
			Value:       45.0,
		})
		require.Error(t, err)
		st, ok := status.FromError(err)
		assert.True(t, ok)
		assert.Equal(t, codes.Unauthenticated, st.Code())
	})

	t.Run("gRPC authenticated with matching device_id accepted", func(t *testing.T) {
		authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer turbine-01-key")
		receipt, err := client.IngestReading(authCtx, &telemetryv1.TelemetryReading{
			DeviceId:    "turbine-01",
			TimestampNs: now,
			MetricName:  "temp",
			Value:       45.0,
		})
		require.NoError(t, err)
		assert.Equal(t, telemetryv1.IngestionReceipt_STATUS_ACCEPTED, receipt.Status)
	})

	t.Run("gRPC anti-spoofing rejects mismatched device_id with PermissionDenied", func(t *testing.T) {
		authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer turbine-01-key")
		_, err := client.IngestReading(authCtx, &telemetryv1.TelemetryReading{
			DeviceId:    "turbine-02", // Mismatched!
			TimestampNs: now,
			MetricName:  "temp",
			Value:       45.0,
		})
		require.Error(t, err)
		st, ok := status.FromError(err)
		assert.True(t, ok)
		assert.Equal(t, codes.PermissionDenied, st.Code())
	})
}

