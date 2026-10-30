package storage

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestLocalObjectIdentityAndAtomicReplacement(t *testing.T) {
	s, err := NewLocal(t.TempDir(), "http://localhost")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for key, value := range map[string]string{"a/photo.jpg": "first", "b/photo.jpg": "second"} {
		if err = s.Put(ctx, key, strings.NewReader(value), int64(len(value)), "image/jpeg"); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.Put(ctx, "a/photo.jpg", strings.NewReader("partial"), 100, "image/jpeg"); err == nil {
		t.Fatal("accepted truncated replacement")
	}
	for key, want := range map[string]string{"a/photo.jpg": "first", "b/photo.jpg": "second"} {
		r, err := s.Open(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(r)
		r.Close()
		if err != nil || string(b) != want {
			t.Fatalf("%s: got %q err %v", key, b, err)
		}
	}
	if err = s.Put(ctx, "../escape", strings.NewReader("x"), 1, ""); err == nil {
		t.Fatal("accepted traversal")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err = s.Put(canceled, "canceled.jpg", strings.NewReader("x"), 1, ""); err == nil {
		t.Fatal("ignored cancellation")
	}
}

func TestPresignedURLWorksWithoutRegionLookup(t *testing.T) {
	t.Setenv("MINIO_REGION", "us-east-1")
	var requests atomic.Int64
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, "MinIO offline", http.StatusServiceUnavailable)
	}))
	defer endpoint.Close()
	// A fresh client simulates restart: no region cache can hide a network call.
	s, err := NewMinIO(strings.TrimPrefix(endpoint.URL, "http://"), "minioadmin", "minioadmin", "images", false)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := s.URL(context.Background(), "demo.png")
	if err != nil {
		t.Fatalf("offline URL signing: %v", err)
	}
	if !strings.Contains(signed, "X-Amz-Signature=") {
		t.Fatalf("unsigned URL: %s", signed)
	}
	if requests.Load() != 0 {
		t.Fatal("presigning made an HTTP request to unavailable storage")
	}
}
