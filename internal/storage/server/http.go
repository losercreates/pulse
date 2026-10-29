package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/losercreates/pulse/internal/auth"
	"github.com/losercreates/pulse/internal/storage/engine"
	"github.com/losercreates/pulse/internal/storage/metrics"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// HTTPServer manages REST query and observability endpoints on port :8084.
type HTTPServer struct {
	mux        *http.ServeMux
	engine     engine.ChronoEngine
	metrics    *metrics.Metrics
	dataDir    string
	authorizer auth.TokenAuthorizer
	audit      auth.AuditLogger
}

// NewHTTPServer builds the HTTP router with REST query endpoints and observability probes.
func NewHTTPServer(eng engine.ChronoEngine, m *metrics.Metrics, dataDir ...string) *HTTPServer {
	dir := ""
	if len(dataDir) > 0 {
		dir = dataDir[0]
	}
	s := &HTTPServer{
		mux:     http.NewServeMux(),
		engine:  eng,
		metrics: m,
		dataDir: dir,
	}

	// Observability
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("GET /readyz", s.handleReadyz)
	s.mux.Handle("GET /metrics", promhttp.Handler())

	// REST Query APIs
	s.mux.HandleFunc("GET /api/v1/query", s.handleQueryTimeSeries)
	s.mux.HandleFunc("GET /api/v1/query/rollup", s.handleQueryRollup)
	s.mux.HandleFunc("GET /api/v1/devices", s.handleDeviceCatalog)

	return s
}

// WithAuth configures JWT authentication and role-based access control.
func (s *HTTPServer) WithAuth(authorizer auth.TokenAuthorizer, audit auth.AuditLogger) *HTTPServer {
	s.authorizer = authorizer
	s.audit = audit
	return s
}

// Handler returns the underlying http.Handler.
func (s *HTTPServer) Handler() http.Handler {
	return s.mux
}

func (s *HTTPServer) checkDiskHealth() (bool, string) {
	if s.dataDir == "" {
		return true, ""
	}

	// 1. Verify writeability by creating/removing a temporary probe file
	probeFile := filepath.Join(s.dataDir, ".healthz_probe")
	if err := os.WriteFile(probeFile, []byte("ok"), 0600); err != nil {
		return false, "disk write failed: " + err.Error()
	}
	_ = os.Remove(probeFile)

	// 2. Validate free disk space > 5%
	var stat syscall.Statfs_t
	if err := syscall.Statfs(s.dataDir, &stat); err == nil {
		total := stat.Blocks * uint64(stat.Bsize)
		free := stat.Bavail * uint64(stat.Bsize)
		if total > 0 {
			freePct := float64(free) / float64(total)
			if freePct < 0.05 {
				return false, "insufficient disk space (< 5% remaining)"
			}
		}
	}

	return true, ""
}

func (s *HTTPServer) handleHealthz(w http.ResponseWriter, r *http.Request) {
	ok, reason := s.checkDiskHealth()
	w.Header().Set("Content-Type", "application/json")
	if !ok {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status": "UNHEALTHY",
			"error":  reason,
			"system": "pulse-chronodb-storage",
		})
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status": "UP",
		"system": "pulse-chronodb-storage",
	})
}

func (s *HTTPServer) handleReadyz(w http.ResponseWriter, r *http.Request) {
	ok, reason := s.checkDiskHealth()
	w.Header().Set("Content-Type", "application/json")
	if !ok {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status": "NOT_READY",
			"error":  reason,
			"system": "pulse-chronodb-storage",
		})
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status": "READY",
		"system": "pulse-chronodb-storage",
	})
}

type queryResponse struct {
	DeviceID     string                         `json:"device_id"`
	MetricName   string                         `json:"metric_name"`
	TotalRecords int                            `json:"total_records"`
	Records      []*telemetryv1.StoredAggregate `json:"records"`
}

func (s *HTTPServer) recordAuthFailure(protocol string, err error, clientIP string) {
	reason := "unauthenticated"
	statusCode := http.StatusUnauthorized
	grpcCode := 16 // Unauthenticated

	if errors.Is(err, auth.ErrForbidden) {
		reason = "role_forbidden"
		statusCode = http.StatusForbidden
		grpcCode = 7 // PermissionDenied
	} else if errors.Is(err, auth.ErrDeviceScopeForbidden) {
		reason = "device_scope_forbidden"
		statusCode = http.StatusForbidden
		grpcCode = 7 // PermissionDenied
	} else if errors.Is(err, auth.ErrTokenExpired) {
		reason = "token_expired"
	}

	if s.metrics != nil && s.metrics.AuthFailuresTotal != nil {
		s.metrics.AuthFailuresTotal.WithLabelValues(protocol, reason).Inc()
	}

	if s.audit != nil {
		if statusCode == http.StatusForbidden {
			s.audit.LogAccessDenied(clientIP, "JWT", err.Error(), statusCode, grpcCode)
		} else {
			s.audit.LogAuthFailure(clientIP, "JWT", err.Error(), statusCode, grpcCode)
		}
	}
}

func (s *HTTPServer) handleQueryTimeSeries(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	defer func() {
		if s.metrics != nil {
			s.metrics.QueryLatencySeconds.WithLabelValues("query").Observe(time.Since(start).Seconds())
		}
	}()

	q := r.URL.Query()
	deviceID := q.Get("device_id")
	metricName := q.Get("metric_name")
	startStr := q.Get("start_time")
	endStr := q.Get("end_time")

	if deviceID == "" || metricName == "" || startStr == "" || endStr == "" {
		http.Error(w, `{"error": "device_id, metric_name, start_time, and end_time are required"}`, http.StatusBadRequest)
		return
	}

	// RBAC & Device Scope Validation (requires RoleViewer)
	if s.authorizer != nil {
		authCtx, err := s.authorizer.AuthorizeHTTP(r, auth.RoleViewer, deviceID)
		if err != nil {
			s.recordAuthFailure("http", err, r.RemoteAddr)
			if errors.Is(err, auth.ErrForbidden) || errors.Is(err, auth.ErrDeviceScopeForbidden) {
				http.Error(w, `{"error": "Forbidden: `+err.Error()+`"}`, http.StatusForbidden)
			} else {
				http.Error(w, `{"error": "Unauthorized: `+err.Error()+`"}`, http.StatusUnauthorized)
			}
			return
		}
		r = r.WithContext(auth.WithAuthContext(r.Context(), authCtx))
	}

	startNs, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil {
		http.Error(w, `{"error": "invalid start_time integer"}`, http.StatusBadRequest)
		return
	}
	endNs, err := strconv.ParseInt(endStr, 10, 64)
	if err != nil {
		http.Error(w, `{"error": "invalid end_time integer"}`, http.StatusBadRequest)
		return
	}

	if startNs > endNs {
		http.Error(w, `{"error": "start_time must be <= end_time"}`, http.StatusBadRequest)
		return
	}

	limit := 1000
	if limitStr := q.Get("limit"); limitStr != "" {
		if parsedLimit, err := strconv.Atoi(limitStr); err == nil && parsedLimit > 0 {
			limit = parsedLimit
		}
	}

	records, err := s.engine.QueryTimeSeries(r.Context(), deviceID, metricName, startNs, endNs, limit)
	if err != nil {
		http.Error(w, `{"error": "internal query error"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(queryResponse{
		DeviceID:     deviceID,
		MetricName:   metricName,
		TotalRecords: len(records),
		Records:      records,
	})
}

type rollupResponse struct {
	DeviceID     string                      `json:"device_id"`
	MetricName   string                      `json:"metric_name"`
	StepNs       int64                       `json:"step_ns"`
	TotalBuckets int                         `json:"total_buckets"`
	Buckets      []*telemetryv1.RollupBucket `json:"buckets"`
}

func (s *HTTPServer) handleQueryRollup(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	defer func() {
		if s.metrics != nil {
			s.metrics.QueryLatencySeconds.WithLabelValues("rollup").Observe(time.Since(start).Seconds())
		}
	}()

	q := r.URL.Query()
	deviceID := q.Get("device_id")
	metricName := q.Get("metric_name")
	startStr := q.Get("start_time")
	endStr := q.Get("end_time")
	stepStr := q.Get("step")

	if deviceID == "" || metricName == "" || startStr == "" || endStr == "" {
		http.Error(w, `{"error": "device_id, metric_name, start_time, and end_time are required"}`, http.StatusBadRequest)
		return
	}

	// RBAC & Device Scope Validation (requires RoleOperator)
	if s.authorizer != nil {
		authCtx, err := s.authorizer.AuthorizeHTTP(r, auth.RoleOperator, deviceID)
		if err != nil {
			s.recordAuthFailure("http", err, r.RemoteAddr)
			if errors.Is(err, auth.ErrForbidden) || errors.Is(err, auth.ErrDeviceScopeForbidden) {
				http.Error(w, `{"error": "Forbidden: `+err.Error()+`"}`, http.StatusForbidden)
			} else {
				http.Error(w, `{"error": "Unauthorized: `+err.Error()+`"}`, http.StatusUnauthorized)
			}
			return
		}
		r = r.WithContext(auth.WithAuthContext(r.Context(), authCtx))
	}

	startNs, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil {
		http.Error(w, `{"error": "invalid start_time integer"}`, http.StatusBadRequest)
		return
	}
	endNs, err := strconv.ParseInt(endStr, 10, 64)
	if err != nil {
		http.Error(w, `{"error": "invalid end_time integer"}`, http.StatusBadRequest)
		return
	}

	if startNs > endNs {
		http.Error(w, `{"error": "start_time must be <= end_time"}`, http.StatusBadRequest)
		return
	}

	stepNs := int64(60_000_000_000) // Default 1m
	if stepStr != "" {
		if d, err := time.ParseDuration(stepStr); err == nil && d > 0 {
			stepNs = d.Nanoseconds()
		} else if parsedStep, err := strconv.ParseInt(stepStr, 10, 64); err == nil && parsedStep > 0 {
			stepNs = parsedStep
		}
	}

	buckets, err := s.engine.QueryRollup(r.Context(), deviceID, metricName, startNs, endNs, stepNs)
	if err != nil {
		http.Error(w, `{"error": "internal rollup error"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(rollupResponse{
		DeviceID:     deviceID,
		MetricName:   metricName,
		StepNs:       stepNs,
		TotalBuckets: len(buckets),
		Buckets:      buckets,
	})
}

type catalogResponse struct {
	TotalDevices int                               `json:"total_devices"`
	Devices      []*telemetryv1.DeviceCatalogEntry `json:"devices"`
}

func (s *HTTPServer) handleDeviceCatalog(w http.ResponseWriter, r *http.Request) {
	var authCtx *auth.AuthContext
	if s.authorizer != nil {
		var err error
		authCtx, err = s.authorizer.AuthorizeHTTP(r, auth.RoleViewer, "")
		if err != nil {
			s.recordAuthFailure("http", err, r.RemoteAddr)
			if errors.Is(err, auth.ErrForbidden) || errors.Is(err, auth.ErrDeviceScopeForbidden) {
				http.Error(w, `{"error": "Forbidden: `+err.Error()+`"}`, http.StatusForbidden)
			} else {
				http.Error(w, `{"error": "Unauthorized: `+err.Error()+`"}`, http.StatusUnauthorized)
			}
			return
		}
	}

	limit := 1000
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if parsedLimit, err := strconv.Atoi(limitStr); err == nil && parsedLimit > 0 {
			limit = parsedLimit
		}
	}

	devices, err := s.engine.GetDeviceCatalog(r.Context(), limit)
	if err != nil {
		http.Error(w, `{"error": "internal catalog error"}`, http.StatusInternalServerError)
		return
	}

	// Filter device catalog by allowed devices in token
	if authCtx != nil && len(authCtx.AllowedDevices) > 0 {
		filtered := make([]*telemetryv1.DeviceCatalogEntry, 0, len(devices))
		for _, d := range devices {
			if authCtx.HasAccessToDevice(d.DeviceId) {
				filtered = append(filtered, d)
			}
		}
		devices = filtered
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(catalogResponse{
		TotalDevices: len(devices),
		Devices:      devices,
	})
}
