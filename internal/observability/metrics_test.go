package observability

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMetricsBoundedRoutesAndOutcomes(t *testing.T) {
	m := New()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/images/{image_id}", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("image_id") == "missing" {
			http.Error(w, "missing", 404)
			return
		}
		w.Write([]byte("ok")) // Tests implicit HTTP 200 tracking.
	})
	handler := m.HTTP(mux)
	for _, path := range []string{"/v1/images/first", "/v1/images/second", "/v1/images/missing", "/unknown-one", "/unknown-two"} {
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", path, nil))
	}
	_, err := m.Unary(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/sis.image.v1.ImageService/GetImage"}, func(context.Context, any) (any, error) { return nil, status.Error(codes.Unavailable, "temporary") })
	if status.Code(err) != codes.Unavailable {
		t.Fatal(err)
	}
	families, err := m.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	totals := map[string]float64{}
	for _, family := range families {
		if family.GetName() != "sis2_requests_total" {
			continue
		}
		for _, metric := range family.Metric {
			labels := map[string]string{}
			for _, label := range metric.Label {
				labels[label.GetName()] = label.GetValue()
			}
			totals[labels["protocol"]+"/"+labels["operation"]+"/"+labels["outcome"]] = metric.GetCounter().GetValue()
		}
	}
	for key, want := range map[string]float64{"rest/GetImage/success": 2, "rest/GetImage/error": 1, "rest/unmatched/error": 2, "grpc/GetImage/error": 1} {
		if got := totals[key]; got != want {
			t.Errorf("%s got %v want %v", key, got, want)
		}
	}
	if len(totals) != 4 {
		t.Fatalf("raw paths leaked into labels: %v", totals)
	}
	scrape := httptest.NewRecorder()
	m.Handler().ServeHTTP(scrape, httptest.NewRequest("GET", "/metrics", nil))
	if scrape.Code != 200 || !strings.Contains(scrape.Body.String(), "sis2_request_duration_seconds_bucket") {
		t.Fatalf("scrape failed: %d", scrape.Code)
	}
	// Registries are independent: creating another instance must not panic.
	New()
}
