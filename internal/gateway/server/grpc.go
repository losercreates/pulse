package server

import (
	"context"
	"io"
	"time"

	"github.com/losercreates/pulse/internal/auth"
	"github.com/losercreates/pulse/internal/gateway/metrics"
	"github.com/losercreates/pulse/internal/gateway/service"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GRPCServer implements the Protobuf IngestionServiceServer interface.
type GRPCServer struct {
	telemetryv1.UnimplementedIngestionServiceServer
	service       *service.IngestionService
	authenticator auth.GatewayAuthenticator
	audit         auth.AuditLogger
	metrics       *metrics.Metrics
}

// GRPCOption defines functional options for configuring GRPCServer.
type GRPCOption func(*GRPCServer)

// WithGRPCAuth configures authentication and audit logging for the gRPC server.
func WithGRPCAuth(authenticator auth.GatewayAuthenticator, audit auth.AuditLogger) GRPCOption {
	return func(s *GRPCServer) {
		s.authenticator = authenticator
		s.audit = audit
	}
}

// WithGRPCMetrics configures Prometheus metrics for the gRPC server.
func WithGRPCMetrics(m *metrics.Metrics) GRPCOption {
	return func(s *GRPCServer) {
		s.metrics = m
	}
}

// NewGRPCServer returns a new GRPCServer instance.
func NewGRPCServer(svc *service.IngestionService, opts ...GRPCOption) *GRPCServer {
	s := &GRPCServer{
		service: svc,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// IngestReading handles single unary reading ingestion.
func (s *GRPCServer) IngestReading(ctx context.Context, reading *telemetryv1.TelemetryReading) (*telemetryv1.IngestionReceipt, error) {
	if reading == nil {
		return nil, status.Error(codes.InvalidArgument, "reading cannot be nil")
	}

	// Device anti-spoofing check
	if s.authenticator != nil {
		if id, ok := auth.FromDeviceIdentity(ctx); ok {
			if err := s.authenticator.ValidateDeviceBinding(id, reading.DeviceId); err != nil {
				if s.metrics != nil && s.metrics.AuthFailuresTotal != nil {
					s.metrics.AuthFailuresTotal.WithLabelValues("grpc", "spoofed").Inc()
				}
				if s.audit != nil {
					s.audit.LogSpoofingAttempt(auth.ExtractPeerIP(ctx), "gRPC", reading.DeviceId, id.DeviceID, 403, int(codes.PermissionDenied))
				}
				return nil, status.Errorf(codes.PermissionDenied, "device anti-spoofing violation: %v", err)
			}
			if s.metrics != nil && s.metrics.AuthSuccessesTotal != nil {
				s.metrics.AuthSuccessesTotal.WithLabelValues("grpc", id.AuthMethod).Inc()
			}
		}
	}

	receipt, err := s.service.ProcessReading(ctx, reading, "grpc")
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "broker produce failed: %v", err)
	}

	return receipt, nil
}

// IngestBatch handles batch reading ingestion.
func (s *GRPCServer) IngestBatch(ctx context.Context, batch *telemetryv1.TelemetryBatch) (*telemetryv1.IngestionReceipt, error) {
	if batch == nil || len(batch.Readings) == 0 {
		return nil, status.Error(codes.InvalidArgument, "batch cannot be empty")
	}

	// Device anti-spoofing check for batch
	if s.authenticator != nil {
		if id, ok := auth.FromDeviceIdentity(ctx); ok {
			for _, rd := range batch.Readings {
				if rd != nil {
					if err := s.authenticator.ValidateDeviceBinding(id, rd.DeviceId); err != nil {
						if s.metrics != nil && s.metrics.AuthFailuresTotal != nil {
							s.metrics.AuthFailuresTotal.WithLabelValues("grpc", "spoofed").Inc()
						}
						if s.audit != nil {
							s.audit.LogSpoofingAttempt(auth.ExtractPeerIP(ctx), "gRPC", rd.DeviceId, id.DeviceID, 403, int(codes.PermissionDenied))
						}
						return nil, status.Errorf(codes.PermissionDenied, "device anti-spoofing violation: %v", err)
					}
				}
			}
			if s.metrics != nil && s.metrics.AuthSuccessesTotal != nil {
				s.metrics.AuthSuccessesTotal.WithLabelValues("grpc", id.AuthMethod).Inc()
			}
		}
	}

	receipt, err := s.service.ProcessBatch(ctx, batch, "grpc")
	if err != nil && receipt == nil {
		return nil, status.Errorf(codes.Unavailable, "broker batch produce failed: %v", err)
	}

	return receipt, nil
}

// IngestStream handles client-side streaming of telemetry readings.
func (s *GRPCServer) IngestStream(stream telemetryv1.IngestionService_IngestStreamServer) error {
	var processedCount int32
	var rejectedCount int32
	var validationErrors []*telemetryv1.ValidationError

	ctx := stream.Context()

	for {
		reading, err := stream.Recv()
		if err == io.EOF {
			st := telemetryv1.IngestionReceipt_STATUS_ACCEPTED
			if rejectedCount > 0 && processedCount > 0 {
				st = telemetryv1.IngestionReceipt_STATUS_PARTIAL_SUCCESS
			} else if rejectedCount > 0 && processedCount == 0 {
				st = telemetryv1.IngestionReceipt_STATUS_REJECTED
			}

			return stream.SendAndClose(&telemetryv1.IngestionReceipt{
				Status:         st,
				ProcessedCount: processedCount,
				RejectedCount:  rejectedCount,
				ServerTimeNs:   time.Now().UnixNano(),
				ErrorDetails:   validationErrors,
			})
		}
		if err != nil {
			return status.Errorf(codes.Unknown, "stream error: %v", err)
		}

		// Device anti-spoofing check
		if s.authenticator != nil {
			if id, ok := auth.FromDeviceIdentity(ctx); ok {
				if err := s.authenticator.ValidateDeviceBinding(id, reading.DeviceId); err != nil {
					if s.metrics != nil && s.metrics.AuthFailuresTotal != nil {
						s.metrics.AuthFailuresTotal.WithLabelValues("grpc", "spoofed").Inc()
					}
					if s.audit != nil {
						s.audit.LogSpoofingAttempt(auth.ExtractPeerIP(ctx), "gRPC", reading.DeviceId, id.DeviceID, 403, int(codes.PermissionDenied))
					}
					return status.Errorf(codes.PermissionDenied, "device anti-spoofing violation: %v", err)
				}
				if s.metrics != nil && s.metrics.AuthSuccessesTotal != nil {
					s.metrics.AuthSuccessesTotal.WithLabelValues("grpc", id.AuthMethod).Inc()
				}
			}
		}

		receipt, err := s.service.ProcessReading(ctx, reading, "grpc")
		if err != nil {
			return status.Errorf(codes.Unavailable, "produce error: %v", err)
		}

		if receipt.Status == telemetryv1.IngestionReceipt_STATUS_ACCEPTED {
			processedCount++
		} else {
			rejectedCount++
			validationErrors = append(validationErrors, receipt.ErrorDetails...)
		}
	}
}
