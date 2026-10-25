package transport

import (
	"context"
	"fmt"
	"sync"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCSender streams telemetry readings to the Ingestion Gateway over gRPC.
type GRPCSender struct {
	conn    *grpc.ClientConn
	client  telemetryv1.IngestionServiceClient
	streams []telemetryv1.IngestionService_IngestStreamClient
	mu      sync.Mutex
	curr    int
}

// NewGRPCSender initializes gRPC connection and worker streams.
func NewGRPCSender(targetAddr string, streamPoolSize int) (*GRPCSender, error) {
	conn, err := grpc.NewClient(
		targetAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(10*1024*1024), grpc.MaxCallSendMsgSize(10*1024*1024)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial gateway gRPC %s: %w", targetAddr, err)
	}

	client := telemetryv1.NewIngestionServiceClient(conn)

	if streamPoolSize <= 0 {
		streamPoolSize = 4
	}

	streams := make([]telemetryv1.IngestionService_IngestStreamClient, streamPoolSize)
	for i := 0; i < streamPoolSize; i++ {
		stream, err := client.IngestStream(context.Background())
		if err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("failed to open gRPC ingest stream %d: %w", i, err)
		}
		streams[i] = stream
	}

	return &GRPCSender{
		conn:    conn,
		client:  client,
		streams: streams,
	}, nil
}

// SendBatch sends a slice of telemetry readings over pooled gRPC streams.
func (s *GRPCSender) SendBatch(ctx context.Context, readings []*telemetryv1.TelemetryReading) (int, error) {
	if len(readings) == 0 {
		return 0, nil
	}

	s.mu.Lock()
	stream := s.streams[s.curr]
	s.curr = (s.curr + 1) % len(s.streams)
	s.mu.Unlock()

	sentCount := 0
	for _, r := range readings {
		select {
		case <-ctx.Done():
			return sentCount, ctx.Err()
		default:
		}

		if err := stream.Send(r); err != nil {
			// If stream errored, fallback to IngestBatch unary RPC
			_, batchErr := s.client.IngestBatch(ctx, &telemetryv1.TelemetryBatch{Readings: readings[sentCount:]})
			if batchErr != nil {
				return sentCount, fmt.Errorf("gRPC stream send failed: %w; batch fallback failed: %v", err, batchErr)
			}
			return len(readings), nil
		}
		sentCount++
	}

	return sentCount, nil
}

// Close gracefully closes all active client streams and network connection.
func (s *GRPCSender) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, stream := range s.streams {
		if stream != nil {
			_, _ = stream.CloseAndRecv()
		}
	}
	if s.conn != nil {
		return s.conn.Close()
	}
	return nil
}
