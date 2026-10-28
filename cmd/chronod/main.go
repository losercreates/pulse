package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/losercreates/pulse/internal/storage/config"
	"github.com/losercreates/pulse/internal/storage/consumer"
	"github.com/losercreates/pulse/internal/storage/engine"
	"github.com/losercreates/pulse/internal/storage/metrics"
	"github.com/losercreates/pulse/internal/storage/server"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
)

func main() {
	log.Println("Starting Pulse ChronoDB Storage & Query Service (chronod)...")

	cfg := config.Load()
	reg := prometheus.DefaultRegisterer
	m := metrics.New(reg)
	m.ActiveShardsGauge.Set(float64(cfg.NumShards))

	// 1. Initialize Storage Engine
	var eng engine.ChronoEngine
	if cfg.DataDir != "" {
		log.Printf("Initializing durable LSM storage engine at %s (%d shards)...", cfg.DataDir, cfg.NumShards)
		durableEng, err := engine.NewDurableEngine(engine.DurableConfig{
			DataDir:            cfg.DataDir,
			NumShards:          cfg.NumShards,
			MemtableLimit:      5000,
			RetentionTTL:       time.Duration(cfg.RetentionDays) * 24 * time.Hour,
			CompactionInterval: cfg.CompactionInterval,
		})
		if err != nil {
			log.Fatalf("Failed to initialize durable storage engine: %v", err)
		}
		eng = durableEng
	} else {
		log.Printf("Initializing in-memory storage engine (%d shards)...", cfg.NumShards)
		eng = engine.NewShardedMemoryEngine(cfg.NumShards)
	}
	defer eng.Close()

	// 2. Initialize Franz-go Consumer Client
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.TopicAggregates),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.DisableAutoCommit(), // Synchronous offset commit post-storage acknowledgment
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		log.Printf("Warning: Failed to initialize Franz-go client: %v. Running in standalone mode.", err)
		client = nil
	} else {
		defer client.Close()
	}

	// 3. Initialize Storage Consumer Sink
	sink := consumer.NewStorageConsumerSink(cfg, client, eng, m)

	// 4. Initialize and start gRPC Server on port :50054
	grpcServer := grpc.NewServer()
	chronoGRPCServer := server.NewChronoDBGRPCServer(eng, m)
	telemetryv1.RegisterChronoDBServiceServer(grpcServer, chronoGRPCServer)

	grpcListener, err := net.Listen("tcp", cfg.GRPCPort)
	if err != nil {
		log.Fatalf("Failed to listen on gRPC port %s: %v", cfg.GRPCPort, err)
	}

	go func() {
		log.Printf("ChronoDB gRPC Server listening on %s", cfg.GRPCPort)
		if err := grpcServer.Serve(grpcListener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Fatalf("gRPC server error: %v", err)
		}
	}()

	// 5. Initialize and start HTTP Observability & Query Server on port :8084
	httpServerWrapper := server.NewHTTPServer(eng, m, cfg.DataDir)
	httpServer := &http.Server{
		Addr:         cfg.HTTPPort,
		Handler:      httpServerWrapper.Handler(),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("HTTP Metrics, Health & Query Server listening on %s", cfg.HTTPPort)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// 6. Start Storage Consumer Loop
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		log.Printf("Consuming window aggregates from topic '%s'...", cfg.TopicAggregates)
		if err := sink.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Fatalf("Storage consumer error: %v", err)
		}
	}()

	// 7. Await Termination Signals
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down Pulse ChronoDB Storage & Query Service...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// Shutdown HTTP Server
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown warning: %v", err)
	}

	// Stop gRPC Server
	grpcServer.GracefulStop()

	// Stop Consumer Sink
	cancel()
	sink.Stop()

	log.Println("Pulse ChronoDB Storage & Query Service stopped cleanly.")
}
