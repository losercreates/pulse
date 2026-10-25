package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/losercreates/pulse/internal/simulator/canary"
	"github.com/losercreates/pulse/internal/simulator/chaos"
	"github.com/losercreates/pulse/internal/simulator/config"
	"github.com/losercreates/pulse/internal/simulator/fleet"
	"github.com/losercreates/pulse/internal/simulator/metrics"
	"github.com/losercreates/pulse/internal/simulator/pacing"
	"github.com/losercreates/pulse/internal/simulator/report"
	"github.com/losercreates/pulse/internal/simulator/transport"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

func main() {
	cfg, err := config.Parse(os.Args[1:])
	if err != nil {
		if err == flag.ErrHelp {
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "Error parsing configuration: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle OS interrupt signals
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	interrupted := false

	go func() {
		<-sigCh
		interrupted = true
		cancel()
		<-sigCh
		os.Exit(130)
	}()

	// Initialize metrics tracker
	tracker := metrics.NewTracker()

	// Initialize fleet
	f := fleet.NewFleet(cfg.NumDevices)

	// Initialize transport client
	var sender transport.Sender
	if cfg.Transport == "http" {
		sender = transport.NewHTTPSender(cfg.GatewayHTTPAddr)
	} else {
		grpcSender, err := transport.NewGRPCSender(cfg.GatewayGRPCAddr, cfg.Workers)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Gateway connection error: %v\n", err)
			os.Exit(2)
		}
		sender = grpcSender
	}
	defer sender.Close()

	// Rate limiter & pacing
	limiter := pacing.NewLimiter(cfg.TargetRate)

	// Chaos controllers
	burstController := chaos.NewBurstController(limiter, cfg.TargetRate, cfg.BurstMultiplier, cfg.BurstDuration*2, cfg.BurstDuration)
	if cfg.Profile == "burst" || cfg.Profile == "mixed" {
		go burstController.Run(ctx)
	}

	offlineController := chaos.NewOfflineController(f, cfg.OfflinePct, cfg.OfflineDuration*2, cfg.OfflineDuration)
	if cfg.Profile == "offline" || cfg.Profile == "mixed" {
		go offlineController.Run(ctx)
	}

	anomalyInjector := chaos.NewAnomalyInjector(cfg.AnomalyPct)

	// Canary probe
	var canaryProbe *canary.Probe
	if cfg.ChronoQueryAddr != "" {
		canaryProbe = canary.NewProbe(sender, cfg.ChronoQueryAddr, tracker, cfg.CanaryInterval)
		go canaryProbe.Run(ctx)
	}

	// Banner in interactive mode
	if !cfg.JSONOutput {
		fmt.Printf("Pulse Fleet Load Simulator — Running\n")
		fmt.Printf("Target: %d events/sec | Fleet: %d devices | Transport: %s\n", cfg.TargetRate, cfg.NumDevices, cfg.Transport)
		fmt.Printf("Profile: %s | Duration: %v | Workers: %d\n\n", cfg.Profile, cfg.Duration, cfg.Workers)
	}

	startTime := time.Now()

	// Launch worker pool
	partitions := f.WorkerPartitions(cfg.Workers)
	var wg sync.WaitGroup

	for workerID, partition := range partitions {
		wg.Add(1)
		go func(wID int, devs []*fleet.Device) {
			defer wg.Done()
			runWorker(ctx, wID, devs, limiter, sender, tracker, anomalyInjector, startTime)
		}(workerID, partition)
	}

	// Progress dashboard ticker
	var dashWg sync.WaitGroup
	if !cfg.JSONOutput {
		dashWg.Add(1)
		go func() {
			defer dashWg.Done()
			ticker := time.NewTicker(250 * time.Millisecond)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					elapsed := time.Since(startTime)
					bursting := burstController.IsBursting()
					offline := offlineController.ActiveOfflineCount()
					report.RenderDashboard(os.Stdout, cfg, tracker.Snapshot(), elapsed, cfg.Workers, bursting, offline)
				}
			}
		}()
	}

	// Execution timer
	timer := time.NewTimer(cfg.Duration)
	select {
	case <-timer.C:
		cancel()
	case <-ctx.Done():
		timer.Stop()
	}

	wg.Wait()
	dashWg.Wait()

	if !cfg.JSONOutput {
		fmt.Printf("\n\n")
	}

	// Final report and verdict
	finalSnap := tracker.Snapshot()
	var canarySent, canaryResolved int
	if canaryProbe != nil {
		canarySent = canaryProbe.ProbesSent()
		canaryResolved = canaryProbe.ProbesResolved()
	}

	rep := report.BuildReport(cfg, finalSnap, canarySent, canaryResolved)

	if cfg.JSONOutput {
		_ = report.PrintJSON(os.Stdout, rep)
	} else {
		printHumanSummary(rep)
	}

	if interrupted {
		os.Exit(130)
	}

	if !rep.SLAVerdict.Compliant {
		os.Exit(1)
	}

	os.Exit(0)
}

func runWorker(
	ctx context.Context,
	workerID int,
	devices []*fleet.Device,
	limiter *pacing.Limiter,
	sender transport.Sender,
	tracker *metrics.Tracker,
	anomaly *chaos.AnomalyInjector,
	startTime time.Time,
) {
	if len(devices) == 0 {
		return
	}

	// Dynamic batch size tailored to throughput
	batchSize := 50
	if len(devices) < batchSize {
		batchSize = len(devices)
	}
	if batchSize < 1 {
		batchSize = 1
	}

	metricIdx := 0
	devIdx := 0
	batch := make([]*telemetryv1.TelemetryReading, 0, batchSize*2)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if !limiter.Wait(ctx, batchSize) {
			return
		}

		nowNs := time.Now().UnixNano()
		elapsed := time.Since(startTime)
		batch = batch[:0]

		for i := 0; i < batchSize; i++ {
			dev := devices[devIdx]
			devIdx = (devIdx + 1) % len(devices)

			r := dev.NextReading(nowNs, elapsed, metricIdx)
			if r != nil {
				if anomaly != nil && len(dev.Metrics) > 0 {
					mDef := dev.Metrics[metricIdx%len(dev.Metrics)]
					if mDef.Generator != nil {
						anomaly.MaybeInject(r, mDef.Generator.BaseValue(), mDef.Generator.NoiseSigma())
					}
				}
				batch = append(batch, r)
			}

			// If device reconnected, drain offline backfill buffer
			backfill := dev.DrainOfflineBuffer()
			if len(backfill) > 0 {
				batch = append(batch, backfill...)
			}
		}
		metricIdx++

		if len(batch) > 0 {
			sent, err := sender.SendBatch(ctx, batch)
			if err != nil {
				tracker.RecordFailed(len(batch) - sent)
			}
			tracker.RecordSent(sent)
		}
	}
}

func printHumanSummary(rep *report.Report) {
	fmt.Printf("================================================================================\n")
	fmt.Printf("                       PULSE SIMULATION EXECUTION SUMMARY                       \n")
	fmt.Printf("================================================================================\n")
	fmt.Printf("Duration:         %.1fs\n", rep.Config.DurationSeconds)
	fmt.Printf("Transport:        %s\n", rep.Config.Transport)
	fmt.Printf("Profile:          %s\n", rep.Config.Profile)
	fmt.Printf("Total Readings:   %d sent, %d failed (%.3f%% error)\n", rep.Results.TotalSent, rep.Results.TotalFailed, rep.Results.ErrorRatePct)
	fmt.Printf("Throughput:       Achieved %.1f ev/s (Target: %d ev/s, Peak: %.1f ev/s)\n", rep.Results.AchievedRate, rep.Config.TargetRate, rep.Results.PeakRate)

	if rep.Results.CanaryProbesResolved > 0 {
		fmt.Printf("Canary Latency:   p50: %.1fms | p95: %.1fms (Probes: %d/%d)\n",
			rep.Results.CanaryP50LatencyMs, rep.Results.CanaryP95LatencyMs, rep.Results.CanaryProbesResolved, rep.Results.CanaryProbesSent)
	}

	fmt.Printf("--------------------------------------------------------------------------------\n")
	if rep.SLAVerdict.Compliant {
		fmt.Printf("SLA STATUS:       [COMPLIANT] ALL PERFORMANCE GATES PASSED (Exit Code: 0)\n")
	} else {
		fmt.Printf("SLA STATUS:       [VIOLATION] ONE OR MORE SLA GATES FAILED (Exit Code: 1)\n")
		for _, v := range rep.SLAVerdict.Violations {
			fmt.Printf("                  - %s\n", v)
		}
	}
	fmt.Printf("================================================================================\n")
}
