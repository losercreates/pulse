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

	"github.com/losercreates/pulse/internal/alert/detector"
	"github.com/losercreates/pulse/internal/processor/cep"
	"github.com/losercreates/pulse/internal/processor/config"
	"github.com/losercreates/pulse/internal/processor/consumer"
	"github.com/losercreates/pulse/internal/processor/latesink"
	"github.com/losercreates/pulse/internal/processor/metrics"
	"github.com/losercreates/pulse/internal/processor/publisher"
	"github.com/losercreates/pulse/internal/processor/server"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	log.Println("Starting Pulse Stream Processing Engine...")

	cfg := config.Load()
	reg := prometheus.DefaultRegisterer
	m := metrics.New(reg)

	// 1. Setup Franz-go client for publishing aggregates and late records
	producerClient, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
	)
	if err != nil {
		log.Fatalf("Failed to initialize Franz-go publisher client: %v", err)
	}
	defer producerClient.Close()

	aggPub := publisher.NewAggregatePublisher(producerClient, cfg.TopicAggregates)
	lateSink := latesink.NewLateDataSink(producerClient, cfg.TopicLate)
	alertPub := detector.NewKafkaAlertPublisher(producerClient, "telemetry.alerts")

	// 2. Setup Partition Consumer Group
	consumerGroup, err := consumer.NewPartitionConsumerGroup(cfg, aggPub, lateSink, m)
	if err != nil {
		log.Fatalf("Failed to initialize Partition Consumer Group: %v", err)
	}
	consumerGroup.SetAlertPublisher(alertPub)

	checkpointMgr := consumer.NewCheckpointManager(consumerGroup, consumerGroup.Client(), cfg.TopicRaw, 1*time.Second)

	// 3. Setup CEP Rule Reloader & HTTP Observability Server
	var httpOpts []server.HTTPOption
	if cfg.CEPRulesFile != "" {
		reloader := cep.NewRuleReloader(cfg.CEPRulesFile, consumerGroup)
		if rules, err := reloader.Reload(); err == nil {
			log.Printf("Loaded %d CEP rules from %s", len(rules), cfg.CEPRulesFile)
		} else {
			log.Printf("Warning: CEP initial rule load from %s failed: %v", cfg.CEPRulesFile, err)
		}
		httpOpts = append(httpOpts, server.WithRuleReloader(reloader))
	}

	httpServerWrapper := server.NewHTTPServer(httpOpts...)
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

	// 4. Start Checkpoint Manager & Consumer Group
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go checkpointMgr.Run(ctx)

	go func() {
		log.Printf("Consuming from topic '%s' using group '%s'...", cfg.TopicRaw, cfg.GroupID)
		if err := consumerGroup.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Fatalf("Consumer group encountered error: %v", err)
		}
	}()

	// 5. Await Termination Signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down Pulse Stream Processing Engine...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// Shutdown HTTP Server
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown warning: %v", err)
	}

	// Stop consumer loop and flush partition workers
	cancel()
	consumerGroup.Stop()

	// Final synchronous offset commit
	commitCtx, commitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer commitCancel()
	if err := checkpointMgr.CommitSync(commitCtx); err != nil {
		log.Printf("Warning: Final offset checkpoint failed: %v", err)
	}

	log.Println("Pulse Stream Processing Engine stopped gracefully.")
}
