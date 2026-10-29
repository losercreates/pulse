package auth

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"strings"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

// GatewayAuthenticator authenticates edge clients connecting over HTTP or gRPC
// and verifies that the client identity matches the payload device_id.
type GatewayAuthenticator interface {
	AuthenticateHTTP(r *http.Request) (*DeviceIdentity, error)
	AuthenticateGRPC(ctx context.Context) (*DeviceIdentity, error)
	ValidateDeviceBinding(identity *DeviceIdentity, claimedDeviceID string) error
}

// LoadCACertPool reads a PEM-encoded CA certificate file into an *x509.CertPool.
func LoadCACertPool(caCertPath string) (*x509.CertPool, error) {
	if caCertPath == "" {
		return nil, fmt.Errorf("ca certificate path is empty")
	}
	caPEM, err := os.ReadFile(caCertPath)
	if err != nil {
		return nil, fmt.Errorf("read ca cert file %q: %w", caCertPath, err)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("failed to parse ca certificates from %q", caCertPath)
	}
	return pool, nil
}

// BuildServerTLSConfig builds a *tls.Config for mTLS or standard TLS.
// If clientCAPath is non-empty, RequireAndVerifyClientCert is configured.
func BuildServerTLSConfig(certFile, keyFile, clientCAPath string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load server key pair: %w", err)
	}

	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
	}

	if clientCAPath != "" {
		clientCAs, err := LoadCACertPool(clientCAPath)
		if err != nil {
			return nil, fmt.Errorf("load client ca pool: %w", err)
		}
		cfg.ClientCAs = clientCAs
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return cfg, nil
}

// ExtractDeviceIdentityFromCert inspects an X.509 client certificate and returns the DeviceIdentity.
func ExtractDeviceIdentityFromCert(cert *x509.Certificate) (*DeviceIdentity, error) {
	if cert == nil {
		return nil, fmt.Errorf("certificate is nil")
	}

	deviceID := cert.Subject.CommonName
	if deviceID == "" && len(cert.DNSNames) > 0 {
		deviceID = cert.DNSNames[0]
	}
	if deviceID == "" && len(cert.URIs) > 0 {
		deviceID = cert.URIs[0].String()
	}

	if deviceID == "" {
		return nil, fmt.Errorf("certificate does not contain Common Name or SAN")
	}

	return &DeviceIdentity{
		DeviceID:   deviceID,
		AuthMethod: "mTLS",
		Issuer:     cert.Issuer.CommonName,
		Subject:    cert.Subject.String(),
		IssuedAt:   cert.NotBefore,
		ExpiresAt:  cert.NotAfter,
	}, nil
}

// DefaultGatewayAuthenticator implements GatewayAuthenticator.
type DefaultGatewayAuthenticator struct {
	apiKeyValidator *APIKeyValidator
}

// NewGatewayAuthenticator creates a new GatewayAuthenticator.
func NewGatewayAuthenticator(apiKeyValidator *APIKeyValidator) *DefaultGatewayAuthenticator {
	return &DefaultGatewayAuthenticator{
		apiKeyValidator: apiKeyValidator,
	}
}

// AuthenticateHTTP inspects TLS client certificates (mTLS) or the Authorization / X-API-Key header.
func (a *DefaultGatewayAuthenticator) AuthenticateHTTP(r *http.Request) (*DeviceIdentity, error) {
	// 1. Inspect TLS peer certificates
	if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
		return ExtractDeviceIdentityFromCert(r.TLS.PeerCertificates[0])
	}

	// 2. Inspect API Key
	apiKey := extractAPIKeyFromHTTP(r)
	if apiKey != "" && a.apiKeyValidator != nil {
		return a.apiKeyValidator.Validate(apiKey)
	}

	return nil, ErrUnauthenticated
}

// AuthenticateGRPC extracts peer certificates from gRPC TLS info or metadata authorization.
func (a *DefaultGatewayAuthenticator) AuthenticateGRPC(ctx context.Context) (*DeviceIdentity, error) {
	// 1. Check TLS peer info
	if p, ok := peer.FromContext(ctx); ok && p.AuthInfo != nil {
		if tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo); ok && len(tlsInfo.State.PeerCertificates) > 0 {
			return ExtractDeviceIdentityFromCert(tlsInfo.State.PeerCertificates[0])
		}
	}

	// 2. Check metadata
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		apiKey := extractAPIKeyFromMetadata(md)
		if apiKey != "" && a.apiKeyValidator != nil {
			return a.apiKeyValidator.Validate(apiKey)
		}
	}

	return nil, ErrUnauthenticated
}

// ValidateDeviceBinding enforces anti-spoofing: identity.DeviceID MUST match claimedDeviceID.
func (a *DefaultGatewayAuthenticator) ValidateDeviceBinding(identity *DeviceIdentity, claimedDeviceID string) error {
	if identity == nil {
		return ErrUnauthenticated
	}
	if claimedDeviceID == "" {
		return fmt.Errorf("%w: claimed device_id is empty", ErrSpoofedIdentity)
	}
	if identity.DeviceID != claimedDeviceID {
		return fmt.Errorf("%w: authenticated device %q tried to send data as %q",
			ErrSpoofedIdentity, identity.DeviceID, claimedDeviceID)
	}
	return nil
}

func extractAPIKeyFromHTTP(r *http.Request) string {
	authHdr := r.Header.Get("Authorization")
	if authHdr != "" {
		parts := strings.SplitN(authHdr, " ", 2)
		if len(parts) == 2 && strings.EqualFold(parts[0], "bearer") {
			return strings.TrimSpace(parts[1])
		}
		if len(parts) == 1 {
			return strings.TrimSpace(parts[0])
		}
	}
	if key := r.Header.Get("X-API-Key"); key != "" {
		return strings.TrimSpace(key)
	}
	return ""
}

func extractAPIKeyFromMetadata(md metadata.MD) string {
	if vals := md.Get("authorization"); len(vals) > 0 {
		raw := vals[0]
		parts := strings.SplitN(raw, " ", 2)
		if len(parts) == 2 && strings.EqualFold(parts[0], "bearer") {
			return strings.TrimSpace(parts[1])
		}
		return strings.TrimSpace(raw)
	}
	if vals := md.Get("x-api-key"); len(vals) > 0 {
		return strings.TrimSpace(vals[0])
	}
	return ""
}

