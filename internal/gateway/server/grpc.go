package server

import (
	"context"
	"io"
	"time"

	"github.com/losercreates/pulse/internal/gateway/service"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GRPCServer implements the Protobuf IngestionServiceServer interface.
type GRPCServer struct {
	telemetryv1.UnimplementedIngestionServiceServer
	service *service.IngestionService
}

// NewGRPCServer returns a new GRPCServer instance.
func NewGRPCServer(svc *service.IngestionService) *GRPCServer {
	return &GRPCServer{
		service: svc,
	}
}

// IngestReading handles single unary reading ingestion.
func (s *GRPCServer) IngestReading(ctx context.Context, reading *telemetryv1.TelemetryReading) (*telemetryv1.IngestionReceipt, error) {
	if reading == nil {
		return nil, status.Error(codes.InvalidArgument, "reading cannot be nil")
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

		receipt, err := s.service.ProcessReading(stream.Context(), reading, "grpc")
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
