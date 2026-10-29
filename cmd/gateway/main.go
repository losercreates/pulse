package main

import (
	"context"
	"crypto/tls"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/losercreates/pulse/internal/auth"
	"github.com/losercreates/pulse/internal/gateway/config"
	"github.com/losercreates/pulse/internal/gateway/metrics"
	"github.com/losercreates/pulse/internal/gateway/producer"
	"github.com/losercreates/pulse/internal/gateway/server"
	"github.com/losercreates/pulse/internal/gateway/service"
	"github.com/losercreates/pulse/internal/gateway/validator"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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

	// Setup Security & Authentication if configured
	var authenticator auth.GatewayAuthenticator
	var rateLimiter auth.RateLimiter
	var auditLogger auth.AuditLogger
	var serverTLS *tls.Config

	if cfg.AuthEnabled {
		auditLogger = auth.NewJSONAuditLogger(os.Stderr, "pulse-gateway")
		keyValidator := auth.NewAPIKeyValidator()
		if cfg.APIKeysFile != "" {
			if err := keyValidator.LoadFile(cfg.APIKeysFile); err != nil {
				log.Printf("Warning: failed to load api keys file: %v", err)
			}
		}
		authenticator = auth.NewGatewayAuthenticator(keyValidator)
		if cfg.RateLimitRate > 0 {
			rateLimiter = auth.NewTokenBucketRateLimiter(cfg.RateLimitRate, cfg.RateLimitBurst)
		}
	}

	if cfg.TLSEnabled {
		tlsConfig, err := auth.BuildServerTLSConfig(cfg.TLSCertFile, cfg.TLSKeyFile, cfg.ClientCAFile)
		if err != nil {
			log.Fatalf("Failed to initialize TLS config: %v", err)
		}
		serverTLS = tlsConfig
	}

	// 1. Setup HTTP Server
	var httpOpts []server.HTTPOption
	if authenticator != nil {
		httpOpts = append(httpOpts, server.WithHTTPAuth(authenticator, rateLimiter, auditLogger))
	}
	httpOpts = append(httpOpts, server.WithHTTPMetrics(m))

	httpServerWrapper := server.NewHTTPServer(svc, prod, cfg.TopicRaw, httpOpts...)
	httpServer := &http.Server{
		Addr:         cfg.HTTPPort,
		Handler:      httpServerWrapper.Handler(),
		TLSConfig:    serverTLS,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("HTTP Gateway listening on %s (TLS: %v, Auth: %v)", cfg.HTTPPort, cfg.TLSEnabled, cfg.AuthEnabled)
		if serverTLS != nil {
			if err := httpServer.ListenAndServeTLS("", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Fatalf("HTTP TLS server error: %v", err)
			}
		} else {
			if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Fatalf("HTTP server error: %v", err)
			}
		}
	}()

	// 2. Setup gRPC Server
	var grpcServerOpts []grpc.ServerOption
	if serverTLS != nil {
		grpcServerOpts = append(grpcServerOpts, grpc.Creds(credentials.NewTLS(serverTLS)))
	}
	if authenticator != nil {
		grpcServerOpts = append(grpcServerOpts,
			grpc.UnaryInterceptor(auth.GatewayGRPCUnaryInterceptor(authenticator, rateLimiter, auditLogger)),
			grpc.StreamInterceptor(auth.GatewayGRPCStreamInterceptor(authenticator, rateLimiter, auditLogger)),
		)
	}

	grpcListener, err := net.Listen("tcp", cfg.GRPCPort)
	if err != nil {
		log.Fatalf("Failed to listen on gRPC port %s: %v", cfg.GRPCPort, err)
	}

	grpcServer := grpc.NewServer(grpcServerOpts...)
	var grpcOpts []server.GRPCOption
	if authenticator != nil {
		grpcOpts = append(grpcOpts, server.WithGRPCAuth(authenticator, auditLogger))
	}
	grpcOpts = append(grpcOpts, server.WithGRPCMetrics(m))

	grpcServiceServer := server.NewGRPCServer(svc, grpcOpts...)
	telemetryv1.RegisterIngestionServiceServer(grpcServer, grpcServiceServer)

	go func() {
		log.Printf("gRPC Gateway listening on %s (TLS: %v, Auth: %v)", cfg.GRPCPort, cfg.TLSEnabled, cfg.AuthEnabled)
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
