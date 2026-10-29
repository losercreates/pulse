package auth

import (
	"encoding/json"
	"io"
	"os"
	"sync"
	"time"
)

// Standard audit event types.
const (
	EventAuthSuccess     = "AUTH_SUCCESS"
	EventAuthFailure     = "AUTH_FAILURE"
	EventSpoofingAttempt = "SPOOFING_ATTEMPT"
	EventAccessDenied    = "ACCESS_DENIED"
	EventRateLimited     = "RATE_LIMITED"
)

// SecurityAuditRecord defines the structured format for audit logs.
type SecurityAuditRecord struct {
	Timestamp       string `json:"timestamp"`
	EventType       string `json:"event_type"`
	Service         string `json:"service"`
	ClientIP        string `json:"client_ip,omitempty"`
	AuthMethod      string `json:"auth_method,omitempty"`
	ClaimedDeviceID string `json:"claimed_device_id,omitempty"`
	CertDeviceID    string `json:"cert_device_id,omitempty"`
	Outcome         string `json:"outcome"` // "ALLOWED" or "DENIED"
	Reason          string `json:"reason,omitempty"`
	HTTPStatus      int    `json:"http_status,omitempty"`
	GRPCCode        int    `json:"grpc_code,omitempty"`
}

// AuditLogger records security events.
type AuditLogger interface {
	Log(record SecurityAuditRecord)
	LogAuthSuccess(clientIP, method, deviceID string)
	LogAuthFailure(clientIP, method, reason string, httpStatus, grpcCode int)
	LogSpoofingAttempt(clientIP, method, claimedID, certID string, httpStatus, grpcCode int)
	LogAccessDenied(clientIP, callerID, reason string, httpStatus, grpcCode int)
	LogRateLimited(clientIP, key string)
}

// JSONAuditLogger writes JSON-encoded audit records to an io.Writer in a thread-safe manner.
type JSONAuditLogger struct {
	service string
	out     io.Writer
	mu      sync.Mutex
	encoder *json.Encoder
}

// NewJSONAuditLogger creates a new JSONAuditLogger for the specified service.
func NewJSONAuditLogger(out io.Writer, service string) *JSONAuditLogger {
	if out == nil {
		out = os.Stderr
	}
	return &JSONAuditLogger{
		service: service,
		out:     out,
		encoder: json.NewEncoder(out),
	}
}

// Log writes a single security audit record as a single JSON line.
func (l *JSONAuditLogger) Log(record SecurityAuditRecord) {
	if record.Timestamp == "" {
		record.Timestamp = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if record.Service == "" {
		record.Service = l.service
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	_ = l.encoder.Encode(record)
}

// LogAuthSuccess records a successful authentication event.
func (l *JSONAuditLogger) LogAuthSuccess(clientIP, method, deviceID string) {
	l.Log(SecurityAuditRecord{
		EventType:       EventAuthSuccess,
		ClientIP:        clientIP,
		AuthMethod:      method,
		ClaimedDeviceID: deviceID,
		CertDeviceID:    deviceID,
		Outcome:         "ALLOWED",
	})
}

// LogAuthFailure records a failed authentication event.
func (l *JSONAuditLogger) LogAuthFailure(clientIP, method, reason string, httpStatus, grpcCode int) {
	l.Log(SecurityAuditRecord{
		EventType:  EventAuthFailure,
		ClientIP:   clientIP,
		AuthMethod: method,
		Outcome:    "DENIED",
		Reason:     reason,
		HTTPStatus: httpStatus,
		GRPCCode:   grpcCode,
	})
}

// LogSpoofingAttempt records an identity spoofing violation.
func (l *JSONAuditLogger) LogSpoofingAttempt(clientIP, method, claimedID, certID string, httpStatus, grpcCode int) {
	l.Log(SecurityAuditRecord{
		EventType:       EventSpoofingAttempt,
		ClientIP:        clientIP,
		AuthMethod:      method,
		ClaimedDeviceID: claimedID,
		CertDeviceID:    certID,
		Outcome:         "DENIED",
		Reason:          "device_id spoofing attempt: certificate/key identity does not match payload",
		HTTPStatus:      httpStatus,
		GRPCCode:        grpcCode,
	})
}

// LogAccessDenied records an authorization denial (RBAC / device scope).
func (l *JSONAuditLogger) LogAccessDenied(clientIP, callerID, reason string, httpStatus, grpcCode int) {
	l.Log(SecurityAuditRecord{
		EventType:    EventAccessDenied,
		ClientIP:     clientIP,
		CertDeviceID: callerID,
		Outcome:      "DENIED",
		Reason:       reason,
		HTTPStatus:   httpStatus,
		GRPCCode:     grpcCode,
	})
}

// LogRateLimited records a rate limit drop event.
func (l *JSONAuditLogger) LogRateLimited(clientIP, key string) {
	l.Log(SecurityAuditRecord{
		EventType:       EventRateLimited,
		ClientIP:        clientIP,
		ClaimedDeviceID: key,
		Outcome:         "DENIED",
		Reason:          "rate limit exceeded",
		HTTPStatus:      429,
		GRPCCode:        8, // ResourceExhausted
	})
}

// NopAuditLogger is a no-op audit logger useful for tests.
type NopAuditLogger struct{}

func (n *NopAuditLogger) Log(record SecurityAuditRecord)                                                      {}
func (n *NopAuditLogger) LogAuthSuccess(clientIP, method, deviceID string)                                    {}
func (n *NopAuditLogger) LogAuthFailure(clientIP, method, reason string, httpStatus, grpcCode int)            {}
func (n *NopAuditLogger) LogSpoofingAttempt(clientIP, method, claimedID, certID string, httpStatus, grpcCode int) {}
func (n *NopAuditLogger) LogAccessDenied(clientIP, callerID, reason string, httpStatus, grpcCode int)        {}
func (n *NopAuditLogger) LogRateLimited(clientIP, key string)                                                 {}

