package server

import (
	"net/http"
	"time"

	"github.com/losercreates/pulse/internal/gateway/metrics"
)

// MetricsMiddleware wraps an http.Handler and records request metrics.
type MetricsMiddleware struct {
	handler http.Handler
	metrics *metrics.Metrics
}

// NewMetricsMiddleware creates a new HTTP metrics middleware.
func NewMetricsMiddleware(handler http.Handler, m *metrics.Metrics) *MetricsMiddleware {
	return &MetricsMiddleware{
		handler: handler,
		metrics: m,
	}
}

func (m *MetricsMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

	m.handler.ServeHTTP(rw, r)

	if m.metrics != nil && r.URL.Path != "/metrics" && r.URL.Path != "/healthz" {
		m.metrics.IngestionLatencySeconds.WithLabelValues("http").Observe(time.Since(start).Seconds())
		if r.ContentLength > 0 {
			m.metrics.IngestedBytesTotal.WithLabelValues("http").Add(float64(r.ContentLength))
		}
	}
}

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}
