package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/losercreates/pulse/internal/alert/config"
	"github.com/losercreates/pulse/internal/alert/dispatcher"
	"github.com/losercreates/pulse/internal/alert/metrics"
	"github.com/losercreates/pulse/internal/alert/server"
	"github.com/losercreates/pulse/internal/alert/sink"
	"github.com/losercreates/pulse/internal/auth"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	log.Println("Starting Pulse Alert Dispatcher Daemon (alertd)...")

	cfg := config.Load()
	reg := prometheus.DefaultRegisterer
	m := metrics.New(reg)

	// 1. Initialize Franz-go client for consuming telemetry.alerts and producing dead-letter alerts
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.TopicAlerts),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		log.Printf("Warning: Failed to initialize Franz-go client: %v. Running in standalone mode.", err)
		client = nil
	} else {
		defer client.Close()
	}

	// 2. Setup Sinks
	primarySink := sink.NewWebhookSink(cfg.WebhookURL, nil)
	if cfg.WebhookHMACSecret != "" {
		primarySink.WithSigner(auth.NewWebhookSigner([]byte(cfg.WebhookHMACSecret)))
		log.Println("Alert Dispatcher HMAC-SHA256 outbound signing enabled.")
	}
	deadLetterSink := sink.NewDeadLetterSink(client, cfg.TopicDeadLetter)

	// 3. Setup Alert Dispatcher
	alertDispatcher := dispatcher.NewAlertDispatcher(cfg, client, primarySink, deadLetterSink, m)

	// 4. Setup HTTP Observability Server on designated port :8083
	httpServerWrapper := server.NewHTTPServer()
	httpServer := &http.Server{
		Addr:         cfg.HTTPPort,
		Handler:      httpServerWrapper.Handler(),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("HTTP Metrics & Health Server listening on %s", cfg.HTTPPort)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// 5. Start Alert Dispatcher Loop
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		log.Printf("Consuming from topic '%s' with %d workers...", cfg.TopicAlerts, cfg.WorkerPoolSize)
		if err := alertDispatcher.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Fatalf("Alert dispatcher error: %v", err)
		}
	}()

	// 6. Await OS termination signals
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down Pulse Alert Dispatcher Daemon...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// Shutdown HTTP Server
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown warning: %v", err)
	}

	// Stop alert dispatcher and drain workers
	cancel()
	alertDispatcher.Stop()

	log.Println("Pulse Alert Dispatcher Daemon stopped cleanly.")
}
