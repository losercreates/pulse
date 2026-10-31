package cep

import (
	"fmt"
)

const MaxCandidatesPerPartition = 50000

// CandidateTable manages in-flight sequence candidates for a single partition worker.
// Runs strictly within the worker goroutine: zero mutex locks (Constitution Principle I).
type CandidateTable struct {
	candidates map[string]*SequenceCandidate
}

// NewCandidateTable initializes an in-memory candidate table.
func NewCandidateTable() *CandidateTable {
	return &CandidateTable{
		candidates: make(map[string]*SequenceCandidate),
	}
}

func makeKey(ruleID, deviceID string) string {
	return fmt.Sprintf("%s:%s", ruleID, deviceID)
}

// Get returns the active candidate for a given rule and device, or nil.
func (ct *CandidateTable) Get(ruleID, deviceID string) *SequenceCandidate {
	return ct.candidates[makeKey(ruleID, deviceID)]
}

// Put adds or updates a candidate.
func (ct *CandidateTable) Put(c *SequenceCandidate) {
	if len(ct.candidates) >= MaxCandidatesPerPartition {
		// Safety cap: discard oldest or skip if exceeded
		return
	}
	ct.candidates[makeKey(c.RuleID, c.DeviceID)] = c
}

// Remove deletes a candidate upon sequence completion.
func (ct *CandidateTable) Remove(ruleID, deviceID string) {
	delete(ct.candidates, makeKey(ruleID, deviceID))
}

// Expire purges candidates whose deadline has passed the current partition event-time watermark.
func (ct *CandidateTable) Expire(watermarkNs int64) int {
	expiredCount := 0
	for k, c := range ct.candidates {
		if c.DeadlineNs <= watermarkNs {
			delete(ct.candidates, k)
			expiredCount++
		}
	}
	return expiredCount
}

// Count returns the number of active sequence candidates.
func (ct *CandidateTable) Count() int {
	return len(ct.candidates)
}

// Clear clears all active candidates (used on rule reloads).
func (ct *CandidateTable) Clear() {
	ct.candidates = make(map[string]*SequenceCandidate)
}

