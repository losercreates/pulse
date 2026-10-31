package cep

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

var (
	ErrEmptyRuleID        = errors.New("rule_id cannot be empty")
	ErrEmptyRuleName      = errors.New("rule name cannot be empty")
	ErrInvalidWindow      = errors.New("window_duration must be positive")
	ErrInvalidPatternType = errors.New("pattern_type must be either SEQUENCE or EXPRESSION")
	ErrInvalidSequence    = errors.New("SEQUENCE rule must define at least two sequential steps")
	ErrEmptyExpression    = errors.New("EXPRESSION rule must define a valid arithmetic expression")
)

// RawRuleWrapper allows unmarshaling either a top-level list of rules or an object with a `rules` array.
type RawRuleWrapper struct {
	Rules []CEPRule `json:"rules" yaml:"rules"`
}

// Compiler implements RuleCompiler.
type Compiler struct{}

// NewRuleCompiler creates a new rule compiler instance.
func NewRuleCompiler() *Compiler {
	return &Compiler{}
}

// Compile parses YAML or JSON byte slice into validated CEPRules.
func (c *Compiler) Compile(rawConfig []byte) ([]CEPRule, error) {
	if len(rawConfig) == 0 {
		return nil, nil
	}

	var rules []CEPRule

	// Try unmarshaling as wrapper struct { rules: [...] }
	var wrapper RawRuleWrapper
	errWrapper := yaml.Unmarshal(rawConfig, &wrapper)
	if errWrapper == nil && len(wrapper.Rules) > 0 {
		rules = wrapper.Rules
	} else {
		// Try unmarshaling as direct list []CEPRule
		var list []CEPRule
		errList := yaml.Unmarshal(rawConfig, &list)
		if errList == nil && len(list) > 0 {
			rules = list
		} else {
			// Try as single CEPRule
			var single CEPRule
			errSingle := yaml.Unmarshal(rawConfig, &single)
			if errSingle == nil && single.RuleID != "" {
				rules = []CEPRule{single}
			} else {
				// Also try JSON unmarshal as fallback
				if errJSON := json.Unmarshal(rawConfig, &wrapper); errJSON == nil && len(wrapper.Rules) > 0 {
					rules = wrapper.Rules
				} else if errJSONList := json.Unmarshal(rawConfig, &list); errJSONList == nil && len(list) > 0 {
					rules = list
				} else {
					return nil, fmt.Errorf("failed to parse CEP rules config: %v", errWrapper)
				}
			}
		}
	}

	// Validate each rule
	for i := range rules {
		if err := c.Validate(&rules[i]); err != nil {
			return nil, fmt.Errorf("invalid rule at index %d (%s): %w", i, rules[i].RuleID, err)
		}
	}

	return rules, nil
}

// CompileFile loads and validates rule definitions from a file path.
func (c *Compiler) CompileFile(filePath string) ([]CEPRule, error) {
	cleanPath := filepath.Clean(filePath)
	data, err := os.ReadFile(cleanPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read rule file %s: %w", cleanPath, err)
	}
	return c.Compile(data)
}

// Validate checks and normalizes a CEPRule.
func (c *Compiler) Validate(rule *CEPRule) error {
	if strings.TrimSpace(rule.RuleID) == "" {
		return ErrEmptyRuleID
	}
	if strings.TrimSpace(rule.Name) == "" {
		return ErrEmptyRuleName
	}
	if rule.DevicePattern == "" {
		rule.DevicePattern = "*"
	}
	if rule.WindowDuration <= 0 {
		return ErrInvalidWindow
	}

	rule.PatternType = PatternType(strings.ToUpper(string(rule.PatternType)))
	if rule.PatternType != PatternTypeSequence && rule.PatternType != PatternTypeExpression {
		return ErrInvalidPatternType
	}

	if rule.PatternType == PatternTypeSequence {
		if len(rule.SequenceSteps) < 2 {
			return ErrInvalidSequence
		}
		for idx, step := range rule.SequenceSteps {
			if strings.TrimSpace(step.MetricName) == "" {
				return fmt.Errorf("step %d missing metric_name", idx)
			}
		}
	} else if rule.PatternType == PatternTypeExpression {
		if strings.TrimSpace(rule.Expression) == "" {
			return ErrEmptyExpression
		}
	}

	severity := strings.ToUpper(strings.TrimSpace(rule.Severity))
	switch severity {
	case "INFO", "WARNING", "CRITICAL":
		rule.Severity = severity
	default:
		rule.Severity = "WARNING"
	}

	if rule.Cooldown <= 0 {
		rule.Cooldown = 1 * time.Minute
	}

	return nil
}

// Ensure interface adherence
var _ RuleCompiler = (*Compiler)(nil)

