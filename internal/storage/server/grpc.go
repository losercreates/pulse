package server

import (
	"context"

	"github.com/losercreates/pulse/internal/storage/engine"
	"github.com/losercreates/pulse/internal/storage/metrics"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ChronoDBGRPCServer implements the ChronoDBServiceServer gRPC interface across the polyglot boundary.
type ChronoDBGRPCServer struct {
	telemetryv1.UnimplementedChronoDBServiceServer
	engine  engine.ChronoEngine
	metrics *metrics.Metrics
}

// NewChronoDBGRPCServer creates a new ChronoDBGRPCServer.
func NewChronoDBGRPCServer(eng engine.ChronoEngine, m *metrics.Metrics) *ChronoDBGRPCServer {
	return &ChronoDBGRPCServer{
		engine:  eng,
		metrics: m,
	}
}

// PutAggregates writes a batch of aggregates across the gRPC storage boundary idempotently.
func (s *ChronoDBGRPCServer) PutAggregates(ctx context.Context, req *telemetryv1.PutAggregatesRequest) (*telemetryv1.PutAggregatesResponse, error) {
	if req == nil || len(req.Aggregates) == 0 {
		return &telemetryv1.PutAggregatesResponse{InsertedCount: 0, UpdatedCount: 0}, nil
	}

	inserted, updated, err := s.engine.PutAggregates(ctx, req.Aggregates)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to persist aggregates: %v", err)
	}

	return &telemetryv1.PutAggregatesResponse{
		InsertedCount: uint32(inserted),
		UpdatedCount:  uint32(updated),
	}, nil
}

// QueryTimeSeries retrieves historical stored window aggregates.
func (s *ChronoDBGRPCServer) QueryTimeSeries(ctx context.Context, req *telemetryv1.QueryTimeSeriesRequest) (*telemetryv1.QueryTimeSeriesResponse, error) {
	if req.DeviceId == "" || req.MetricName == "" {
		return nil, status.Error(codes.InvalidArgument, "device_id and metric_name are required")
	}
	if req.StartTimeNs > req.EndTimeNs {
		return nil, status.Error(codes.InvalidArgument, "start_time_ns must be <= end_time_ns")
	}

	records, err := s.engine.QueryTimeSeries(ctx, req.DeviceId, req.MetricName, req.StartTimeNs, req.EndTimeNs, int(req.Limit))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query failed: %v", err)
	}

	return &telemetryv1.QueryTimeSeriesResponse{
		Records:    records,
		TotalCount: uint32(len(records)),
	}, nil
}

// QueryRollup evaluates dynamic downsampled bucket rollups.
func (s *ChronoDBGRPCServer) QueryRollup(ctx context.Context, req *telemetryv1.QueryRollupRequest) (*telemetryv1.QueryRollupResponse, error) {
	if req.DeviceId == "" || req.MetricName == "" {
		return nil, status.Error(codes.InvalidArgument, "device_id and metric_name are required")
	}
	if req.StartTimeNs > req.EndTimeNs {
		return nil, status.Error(codes.InvalidArgument, "start_time_ns must be <= end_time_ns")
	}

	buckets, err := s.engine.QueryRollup(ctx, req.DeviceId, req.MetricName, req.StartTimeNs, req.EndTimeNs, req.StepNs)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "rollup query failed: %v", err)
	}

	return &telemetryv1.QueryRollupResponse{
		Buckets:      buckets,
		TotalBuckets: uint32(len(buckets)),
	}, nil
}

// GetDeviceCatalog lists discovered active devices and recorded metrics.
func (s *ChronoDBGRPCServer) GetDeviceCatalog(ctx context.Context, req *telemetryv1.GetDeviceCatalogRequest) (*telemetryv1.GetDeviceCatalogResponse, error) {
	limit := 1000
	if req != nil && req.Limit > 0 {
		limit = int(req.Limit)
	}

	devices, err := s.engine.GetDeviceCatalog(ctx, limit)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "catalog query failed: %v", err)
	}

	return &telemetryv1.GetDeviceCatalogResponse{
		Devices: devices,
	}, nil
}
