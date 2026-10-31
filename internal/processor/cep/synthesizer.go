package cep

import (
	"encoding/json"
	"fmt"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

// IncidentSynthesizer collapses cascading multi-metric alerts into structured CompositeIncidentEvents.
type IncidentSynthesizer struct{}

// NewIncidentSynthesizer creates a new incident synthesizer.
func NewIncidentSynthesizer() *IncidentSynthesizer {
	return &IncidentSynthesizer{}
}

// SynthesizeCompositeIncident constructs a unified CompositeIncidentEvent from sequence matches.
func (s *IncidentSynthesizer) SynthesizeCompositeIncident(
	rule CEPRule,
	triggerReading *telemetryv1.TelemetryReading,
	zScore float64,
	matchedEvents []CandidateEvent,
) *CompositeIncidentEvent {
	deviceID := "unknown"
	timestampNs := int64(0)
	if triggerReading != nil {
		deviceID = triggerReading.DeviceId
		timestampNs = triggerReading.TimestampNs
	} else if len(matchedEvents) > 0 {
		last := matchedEvents[len(matchedEvents)-1]
		timestampNs = last.TimestampNs
	}

	incidentID := fmt.Sprintf("comp-inc-%s-%s-%d", rule.RuleID, deviceID, timestampNs)

	var initMetric IncidentMetric
	var symptoms []IncidentSymptom

	if len(matchedEvents) > 0 {
		first := matchedEvents[0]
		initMetric = IncidentMetric{
			MetricName:  first.MetricName,
			Value:       first.Value,
			ZScore:      first.ZScore,
			TimestampNs: first.TimestampNs,
		}

		for i := 1; i < len(matchedEvents); i++ {
			ev := matchedEvents[i]
			deltaSec := float64(ev.TimestampNs-first.TimestampNs) / 1e9
			symptoms = append(symptoms, IncidentSymptom{
				MetricName:       ev.MetricName,
				Value:            ev.Value,
				ZScore:           ev.ZScore,
				DeltaTimeSeconds: deltaSec,
				TimestampNs:      ev.TimestampNs,
			})
		}
	} else if triggerReading != nil {
		initMetric = IncidentMetric{
			MetricName:  triggerReading.MetricName,
			Value:       triggerReading.Value,
			ZScore:      zScore,
			TimestampNs: triggerReading.TimestampNs,
		}
	}

	summary := fmt.Sprintf("%s: %s anomaly detected on %s", rule.Name, initMetric.MetricName, deviceID)
	if len(symptoms) > 0 {
		lastSymptom := symptoms[len(symptoms)-1]
		summary = fmt.Sprintf("%s: %s triggered and correlated with %s within %.1fs",
			rule.Name, initMetric.MetricName, lastSymptom.MetricName, lastSymptom.DeltaTimeSeconds)
	}

	return &CompositeIncidentEvent{
		IncidentID:         incidentID,
		RuleID:             rule.RuleID,
		DeviceID:           deviceID,
		Severity:           rule.Severity,
		Status:             "FIRING",
		Summary:            summary,
		DetectedTimeNs:     timestampNs,
		InitiatingMetric:   initMetric,
		CorrelatedSymptoms: symptoms,
	}
}

// ToAlertEvent transforms a CompositeIncidentEvent into a Protobuf AlertEvent for Kafka publishing.
func (s *IncidentSynthesizer) ToAlertEvent(inc *CompositeIncidentEvent, partitionID int32) *telemetryv1.AlertEvent {
	severity := telemetryv1.AlertSeverity_SEVERITY_WARNING
	if inc.Severity == "CRITICAL" {
		severity = telemetryv1.AlertSeverity_SEVERITY_CRITICAL
	} else if inc.Severity == "INFO" {
		severity = telemetryv1.AlertSeverity_SEVERITY_INFO
	}

	return &telemetryv1.AlertEvent{
		AlertId:           inc.IncidentID,
		DeviceId:          inc.DeviceID,
		MetricName:        inc.InitiatingMetric.MetricName,
		TriggerValue:      inc.InitiatingMetric.Value,
		ExpectedMean:      0,
		StandardDeviation: 0,
		ZScore:            inc.InitiatingMetric.ZScore,
		Severity:          severity,
		Status:            telemetryv1.AlertStatus_STATUS_FIRING,
		TimestampNs:       inc.DetectedTimeNs,
		PartitionId:       partitionID,
	}
}

// FormatJSON serializes the composite incident into JSON format for alerting.
func (s *IncidentSynthesizer) FormatJSON(inc *CompositeIncidentEvent) ([]byte, error) {
	return json.Marshal(inc)
}

// UnmarshalIncident parses a composite incident JSON payload.
func UnmarshalIncident(data []byte) (*CompositeIncidentEvent, error) {
	var inc CompositeIncidentEvent
	err := json.Unmarshal(data, &inc)
	return &inc, err
}
