package auth

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"
	"time"
)

// APIKeyEntry defines the stored configuration for an edge device pre-shared key.
type APIKeyEntry struct {
	Key             string    `json:"key"`
	DeviceID        string    `json:"device_id"`
	RateLimitTokens int       `json:"rate_limit_tokens,omitempty"`
	ExpiresAt       time.Time `json:"expires_at,omitempty"`
}

type keyStore struct {
	byHash map[[32]byte]APIKeyEntry
}

// APIKeyValidator performs constant-time validation of pre-shared API keys.
// Read operations are zero-lock using atomic pointer swaps.
type APIKeyValidator struct {
	store atomic.Pointer[keyStore]
}

// NewAPIKeyValidator creates a new APIKeyValidator initialized with the provided entries.
func NewAPIKeyValidator(entries ...APIKeyEntry) *APIKeyValidator {
	v := &APIKeyValidator{}
	v.SetKeys(entries)
	return v
}

// SetKeys replaces the current active keys atomically.
func (v *APIKeyValidator) SetKeys(entries []APIKeyEntry) {
	byHash := make(map[[32]byte]APIKeyEntry, len(entries))
	for _, e := range entries {
		h := sha256.Sum256([]byte(e.Key))
		byHash[h] = e
	}
	s := &keyStore{byHash: byHash}
	v.store.Store(s)
}

// LoadFile reads API key entries from a JSON file and atomically updates the validator.
// The JSON file can be either an array of APIKeyEntry objects:
//
//	[{"key":"secret-key-1","device_id":"turbine-01"}]
//
// or a map of key -> device_id:
//
//	{"secret-key-1": "turbine-01"}
//
// or a map of key -> APIKeyEntry:
//
//	{"secret-key-1": {"device_id": "turbine-01"}}
func (v *APIKeyValidator) LoadFile(filePath string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("read apikey file %q: %w", filePath, err)
	}

	var entries []APIKeyEntry

	// Attempt 1: Slice of entries
	if err := json.Unmarshal(data, &entries); err == nil && len(entries) > 0 {
		v.SetKeys(entries)
		return nil
	}

	// Attempt 2: Map of key -> APIKeyEntry
	var entryMap map[string]APIKeyEntry
	if err := json.Unmarshal(data, &entryMap); err == nil && len(entryMap) > 0 {
		entries = make([]APIKeyEntry, 0, len(entryMap))
		for k, val := range entryMap {
			if val.Key == "" {
				val.Key = k
			}
			entries = append(entries, val)
		}
		v.SetKeys(entries)
		return nil
	}

	// Attempt 3: Map of key -> device_id (string)
	var strMap map[string]string
	if err := json.Unmarshal(data, &strMap); err == nil {
		entries = make([]APIKeyEntry, 0, len(strMap))
		for k, devID := range strMap {
			entries = append(entries, APIKeyEntry{
				Key:      k,
				DeviceID: devID,
			})
		}
		v.SetKeys(entries)
		return nil
	}

	return fmt.Errorf("invalid json format in apikey file %q", filePath)
}

// Validate verifies the given key in constant time and returns the DeviceIdentity.
func (v *APIKeyValidator) Validate(key string) (*DeviceIdentity, error) {
	if key == "" {
		return nil, ErrMissingCredentials
	}

	h := sha256.Sum256([]byte(key))
	s := v.store.Load()
	if s == nil {
		return nil, ErrUnauthenticated
	}

	entry, found := s.byHash[h]
	if !found {
		// Perform a dummy constant-time compare to mitigate timing side-channels
		var dummy [32]byte
		subtle.ConstantTimeCompare(h[:], dummy[:])
		return nil, ErrUnauthenticated
	}

	// Constant-time compare of the full key bytes
	if subtle.ConstantTimeCompare([]byte(key), []byte(entry.Key)) != 1 {
		return nil, ErrUnauthenticated
	}

	// Check expiration if set
	if !entry.ExpiresAt.IsZero() && time.Now().After(entry.ExpiresAt) {
		return nil, ErrTokenExpired
	}

	return &DeviceIdentity{
		DeviceID:        entry.DeviceID,
		AuthMethod:      "APIKey",
		Subject:         entry.DeviceID,
		ExpiresAt:       entry.ExpiresAt,
		RateLimitTokens: entry.RateLimitTokens,
	}, nil
}

