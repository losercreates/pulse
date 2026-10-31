package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/losercreates/pulse/internal/bridge"
	"github.com/losercreates/pulse/internal/bridge/adapter"
	"github.com/losercreates/pulse/internal/bridge/config"
	"github.com/losercreates/pulse/internal/bridge/forwarder"
	"github.com/losercreates/pulse/internal/bridge/metrics"
	"github.com/losercreates/pulse/internal/bridge/spooler"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	configPath := flag.String("config", "", "Path to YAML/JSON configuration file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("failed to load bridge configuration: %v", err)
	}

	log.Printf("Starting pulse-bridge (ID: %s, Gateway: %s)", cfg.BridgeID, cfg.GatewayEndpoint)

	// Metrics
	m := metrics.NewBridgeMetrics(prometheus.DefaultRegisterer)

	// HTTP Server for metrics & health
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK\n"))
	})

	metricsServer := &http.Server{
		Addr:              cfg.MetricsPort,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("pulse-bridge metrics listening on %s", cfg.MetricsPort)
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("metrics server error: %v", err)
		}
	}()

	// Initialize Spooler
	sp, err := spooler.NewDiskSpooler(cfg.SpoolDir, cfg.MaxSpoolBytes)
	if err != nil {
		log.Fatalf("failed to initialize disk spooler: %v", err)
	}

	// Initialize Forwarder
	fwd, err := forwarder.NewGRPCForwarder(cfg)
	if err != nil {
		log.Printf("warning: initial connection to gateway failed: %v", err)
	}

	// Initialize Adapters
	var adapters []adapter.ProtocolAdapter
	for _, mCfg := range cfg.MQTTAdapters {
		ad, err := adapter.NewMQTTAdapter(mCfg)
		if err != nil {
			log.Fatalf("failed to create MQTT adapter %s: %v", mCfg.Name, err)
		}
		adapters = append(adapters, ad)
	}

	for _, modCfg := range cfg.ModbusAdapters {
		ad, err := adapter.NewModbusAdapter(modCfg)
		if err != nil {
			log.Fatalf("failed to create Modbus adapter %s: %v", modCfg.Name, err)
		}
		adapters = append(adapters, ad)
	}

	coord := bridge.NewCoordinator(cfg, adapters, sp, fwd, m)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := coord.Start(ctx); err != nil {
		log.Fatalf("failed to start bridge coordinator: %v", err)
	}

	log.Printf("pulse-bridge operational with %d adapters", len(adapters))

	// Wait for OS termination signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("Received signal %v, initiating graceful shutdown...", sig)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	_ = metricsServer.Shutdown(shutdownCtx)
	if err := coord.Stop(); err != nil {
		log.Printf("error stopping coordinator: %v", err)
	}

	fmt.Println("pulse-bridge stopped cleanly")
}

