package server_test

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/losercreates/pulse/internal/processor/cep"
	"github.com/losercreates/pulse/internal/processor/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockReloadTarget struct{}

func (m *mockReloadTarget) ReloadCEPRules(rules []cep.CEPRule) {}

func TestHTTPServer_HealthAndReload(t *testing.T) {
	tmpDir := t.TempDir()
	rulesFile := filepath.Join(tmpDir, "rules.yaml")
	require.NoError(t, os.WriteFile(rulesFile, []byte(`
rules:
  - rule_id: R1
    name: Test Rule
    device_pattern: "*"
    window_duration: 10s
    pattern_type: SEQUENCE
    sequence_steps:
      - step_index: 0
        metric_name: m1
        condition: ">"
        threshold: 1.0
      - step_index: 1
        metric_name: m2
        condition: ">"
        threshold: 2.0
`), 0600))

	reloader := cep.NewRuleReloader(rulesFile, &mockReloadTarget{})
	srv := server.NewHTTPServer(server.WithRuleReloader(reloader))

	// GET /healthz
	reqHealth := httptest.NewRequest("GET", "/healthz", nil)
	wHealth := httptest.NewRecorder()
	srv.Handler().ServeHTTP(wHealth, reqHealth)
	assert.Equal(t, http.StatusOK, wHealth.Code)

	// POST /api/v1/rules/reload
	reqReload := httptest.NewRequest("POST", "/api/v1/rules/reload", nil)
	wReload := httptest.NewRecorder()
	srv.Handler().ServeHTTP(wReload, reqReload)
	assert.Equal(t, http.StatusOK, wReload.Code)
	assert.Contains(t, wReload.Body.String(), `"rules_loaded":1`)
}

