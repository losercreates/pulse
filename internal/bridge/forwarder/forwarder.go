package forwarder

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/losercreates/pulse/internal/bridge/adapter"
	"github.com/losercreates/pulse/internal/bridge/config"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// ForwarderConfig captures client transport and security options.
type ForwarderConfig struct {
	Endpoint      string
	TLSClientCert string
	TLSClientKey  string
	TLSCACert     string
	APIKey        string
	DialTimeout   time.Duration
}

// GRPCForwarder implements adapter.GatewayForwarder using gRPC with mTLS support.
type GRPCForwarder struct {
	mu        sync.RWMutex
	cfg       ForwarderConfig
	conn      *grpc.ClientConn
	client    telemetryv1.IngestionServiceClient
	connected bool
}

// NewGRPCForwarder creates a new gateway forwarder from bridge configuration.
func NewGRPCForwarder(bCfg *config.EdgeBridgeConfig) (*GRPCForwarder, error) {
	cfg := ForwarderConfig{
		Endpoint:      bCfg.GatewayEndpoint,
		TLSClientCert: bCfg.TLSClientCert,
		TLSClientKey:  bCfg.TLSClientKey,
		TLSCACert:     bCfg.TLSCACert,
		APIKey:        bCfg.APIKey,
		DialTimeout:   5 * time.Second,
	}

	f := &GRPCForwarder{
		cfg: cfg,
	}

	if err := f.connect(); err != nil {
		// Log or retain error, but allow forwarder to start up offline
		f.connected = false
	}

	return f, nil
}

func (f *GRPCForwarder) connect() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.conn != nil {
		_ = f.conn.Close()
		f.conn = nil
		f.client = nil
	}

	var dialOpts []grpc.DialOption

	if f.cfg.TLSCACert != "" || (f.cfg.TLSClientCert != "" && f.cfg.TLSClientKey != "") {
		tlsConfig := &tls.Config{
			MinVersion: tls.VersionTLS13,
		}

		if f.cfg.TLSCACert != "" {
			caCert, err := os.ReadFile(filepath.Clean(f.cfg.TLSCACert))
			if err != nil {
				return fmt.Errorf("failed to read CA cert %s: %w", f.cfg.TLSCACert, err)
			}
			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM(caCert) {
				return fmt.Errorf("failed to append CA cert from %s", f.cfg.TLSCACert)
			}
			tlsConfig.RootCAs = caCertPool
		}

		if f.cfg.TLSClientCert != "" && f.cfg.TLSClientKey != "" {
			cert, err := tls.LoadX509KeyPair(filepath.Clean(f.cfg.TLSClientCert), filepath.Clean(f.cfg.TLSClientKey))
			if err != nil {
				return fmt.Errorf("failed to load client keypair (%s, %s): %w", f.cfg.TLSClientCert, f.cfg.TLSClientKey, err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}

		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	ctx, cancel := context.WithTimeout(context.Background(), f.cfg.DialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, f.cfg.Endpoint, dialOpts...)
	if err != nil {
		f.connected = false
		return fmt.Errorf("failed to connect to gateway at %s: %w", f.cfg.Endpoint, err)
	}

	f.conn = conn
	f.client = telemetryv1.NewIngestionServiceClient(conn)
	f.connected = true
	return nil
}

// ForwardBatch forwards a batch of telemetry readings to the upstream gateway.
func (f *GRPCForwarder) ForwardBatch(ctx context.Context, batch []*telemetryv1.TelemetryReading) (*telemetryv1.IngestionReceipt, error) {
	if len(batch) == 0 {
		return &telemetryv1.IngestionReceipt{Status: telemetryv1.IngestionReceipt_STATUS_ACCEPTED, ProcessedCount: 0}, nil
	}

	f.mu.RLock()
	client := f.client
	f.mu.RUnlock()

	if client == nil {
		if err := f.connect(); err != nil {
			return nil, err
		}
		f.mu.RLock()
		client = f.client
		f.mu.RUnlock()
	}

	outCtx := ctx
	if f.cfg.APIKey != "" {
		outCtx = metadata.AppendToOutgoingContext(ctx, "x-api-key", f.cfg.APIKey)
	}

	req := &telemetryv1.TelemetryBatch{
		Readings: batch,
	}

	receipt, err := client.IngestBatch(outCtx, req)
	if err != nil {
		f.mu.Lock()
		f.connected = false
		f.mu.Unlock()
		return nil, fmt.Errorf("gateway IngestBatch failed: %w", err)
	}

	f.mu.Lock()
	f.connected = true
	f.mu.Unlock()

	return receipt, nil
}

// IsConnected reports whether the upstream gateway is currently reachable.
func (f *GRPCForwarder) IsConnected() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.conn == nil || !f.connected {
		return false
	}

	state := f.conn.GetState()
	return state == connectivity.Ready || state == connectivity.Idle
}

// Close terminates the gRPC connection pool.
func (f *GRPCForwarder) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.connected = false
	if f.conn != nil {
		err := f.conn.Close()
		f.conn = nil
		f.client = nil
		return err
	}
	return nil
}

// Ensure interface adherence
var _ adapter.GatewayForwarder = (*GRPCForwarder)(nil)
