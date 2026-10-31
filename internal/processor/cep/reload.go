package cep

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// RuleReloadTarget receives reloaded rule definitions (e.g. PartitionConsumerGroup).
type RuleReloadTarget interface {
	ReloadCEPRules(rules []CEPRule)
}

// RuleReloader coordinates live reloading of CEP rules via HTTP endpoint or SIGHUP signal.
type RuleReloader struct {
	mu       sync.Mutex
	rulePath string
	compiler RuleCompiler
	target   RuleReloadTarget
	lastErr  error
}

// NewRuleReloader constructs a rule reloader instance.
func NewRuleReloader(rulePath string, target RuleReloadTarget) *RuleReloader {
	return &RuleReloader{
		rulePath: rulePath,
		compiler: NewRuleCompiler(),
		target:   target,
	}
}

// Reload parses the rule file and updates the reload target atomically.
func (r *RuleReloader) Reload() ([]CEPRule, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.rulePath == "" {
		return nil, fmt.Errorf("no rule path configured")
	}

	rules, err := r.compiler.CompileFile(r.rulePath)
	if err != nil {
		r.lastErr = err
		return nil, fmt.Errorf("failed to compile rules from %s: %w", r.rulePath, err)
	}

	if r.target != nil {
		r.target.ReloadCEPRules(rules)
	}

	r.lastErr = nil
	return rules, nil
}

// StartSignalListener begins listening for SIGHUP in the background.
func (r *RuleReloader) StartSignalListener(ctx context.Context) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGHUP)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-sigChan:
				log.Printf("Received SIGHUP, reloading CEP rules from %s...", r.rulePath)
				if rules, err := r.Reload(); err != nil {
					log.Printf("Failed to reload CEP rules on SIGHUP: %v", err)
				} else {
					log.Printf("Successfully reloaded %d CEP rules", len(rules))
				}
			}
		}
	}()
}

