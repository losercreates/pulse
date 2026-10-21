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

	"github.com/losercreates/pulse/internal/gateway/config"
	"github.com/losercreates/pulse/internal/gateway/metrics"
	"github.com/losercreates/pulse/internal/gateway/producer"
	"github.com/losercreates/pulse/internal/gateway/server"
	"github.com/losercreates/pulse/internal/gateway/service"
	"github.com/losercreates/pulse/internal/gateway/validator"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
)

func main() {
	log.Println("Starting Pulse Ingestion Gateway...")

	cfg := config.Load()
	reg := prometheus.DefaultRegisterer
	m := metrics.New(reg)

	val := validator.New(cfg.MaxFutureSkewDuration, cfg.MaxPastSkewDuration)

	prod, err := producer.New(cfg, m)
	if err != nil {
		log.Fatalf("Failed to initialize Franz-go producer: %v", err)
	}
	defer prod.Close()

	svc := service.NewIngestionService(val, prod, m)

	// 1. Setup HTTP Server
	httpServerWrapper := server.NewHTTPServer(svc, prod, cfg.TopicRaw)
	httpServer := &http.Server{
		Addr:         cfg.HTTPPort,
		Handler:      httpServerWrapper.Handler(),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("HTTP Gateway listening on %s", cfg.HTTPPort)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// 2. Setup gRPC Server
	grpcListener, err := net.Listen("tcp", cfg.GRPCPort)
	if err != nil {
		log.Fatalf("Failed to listen on gRPC port %s: %v", cfg.GRPCPort, err)
	}

	grpcServer := grpc.NewServer()
	grpcServiceServer := server.NewGRPCServer(svc)
	telemetryv1.RegisterIngestionServiceServer(grpcServer, grpcServiceServer)

	go func() {
		log.Printf("gRPC Gateway listening on %s", cfg.GRPCPort)
		if err := grpcServer.Serve(grpcListener); err != nil {
			log.Fatalf("gRPC server error: %v", err)
		}
	}()

	// 3. Graceful Shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down Pulse Ingestion Gateway...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	grpcServer.GracefulStop()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown warning: %v", err)
	}

	log.Println("Pulse Ingestion Gateway stopped gracefully.")
}
