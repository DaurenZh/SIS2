// Package observability records server attempts. It is opt-in so monitoring
// does not silently change the controlled benchmark's measurement overhead.
package observability

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/DaurenZh/SIS2/internal/rpcspec"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

type Metrics struct {
	registry *prometheus.Registry
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

func New() *Metrics {
	m := &Metrics{
		registry: prometheus.NewRegistry(),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sis2_requests_total", Help: "Completed server attempts, including retries, by protocol, operation and outcome.",
		}, []string{"protocol", "operation", "outcome"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "sis2_request_duration_seconds", Help: "Server attempt duration; excludes client backoff and network round trips.",
			Buckets: []float64{.0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1, .3, 1, 3},
		}, []string{"protocol", "operation"}),
	}
	// A private registry allows multiple servers in tests without global duplicate
	// registration. Standard collectors add Go memory/GC and process CPU metrics.
	m.registry.MustRegister(m.requests, m.duration, collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}
func (m *Metrics) observe(protocol, operation string, failed bool, began time.Time) {
	outcome := "success"
	if failed {
		outcome = "error"
	}
	m.requests.WithLabelValues(protocol, operation, outcome).Inc()
	m.duration.WithLabelValues(protocol, operation).Observe(time.Since(began).Seconds())
}
func (m *Metrics) Unary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	began := time.Now()
	out, err := handler(ctx, req)
	method := info.FullMethod[strings.LastIndex(info.FullMethod, "/")+1:]
	m.observe("grpc", method, status.Code(err) != 0, began)
	return out, err
}

type responseStatus struct {
	http.ResponseWriter
	code int
}

func (w *responseStatus) WriteHeader(code int) {
	if w.code != 0 {
		return
	}
	w.code = code
	w.ResponseWriter.WriteHeader(code)
}
func (w *responseStatus) Write(b []byte) (int, error) {
	if w.code == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}
func (w *responseStatus) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (m *Metrics) HTTP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		began := time.Now()
		record := &responseStatus{ResponseWriter: w}
		next.ServeHTTP(record, r)
		// Use the matched route template, never raw URLs, image IDs, owners or keys:
		// an unbounded label would create one time series per resource.
		operation := "unmatched"
		for _, op := range rpcspec.Operations {
			if r.Pattern == op.Verb+" "+op.Path {
				operation = op.Name
				break
			}
		}
		switch r.Pattern {
		case "POST /v1/images/upload":
			operation = "UploadImage"
		case "GET /v1/objects/{key...}":
			operation = "GetObject"
		}
		m.observe("rest", operation, record.code >= 400, began)
	})
}
