package transport_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/DaurenZh/SIS2/internal/service"
	"github.com/DaurenZh/SIS2/internal/transport"
)

type outageStore struct {
	puts atomic.Int64
	down atomic.Bool
}

func (s *outageStore) Put(_ context.Context, _ string, body io.Reader, _ int64, _ string) error {
	s.puts.Add(1)
	if s.down.Load() {
		return errors.New("storage unavailable")
	}
	_, err := io.Copy(io.Discard, body)
	return err
}
func (s *outageStore) URL(context.Context, string) (string, error) {
	return "http://example.test/signed-object", nil
}
func (s *outageStore) Open(context.Context, string) (io.ReadCloser, error) {
	return io.NopCloser(io.MultiReader(strings.NewReader(strings.Repeat("x", 64<<10)), failedObjectReader{})), nil
}

type failedObjectReader struct{}

func (failedObjectReader) Read([]byte) (int, error) {
	return 0, errors.New("object stream interrupted")
}

func TestFailedDownloadIsNotAcceptedAsComplete(t *testing.T) {
	svc := service.NewServer()
	defer svc.Close()
	server := httptest.NewServer(transport.NewRESTHandler(svc, &outageStore{}))
	defer server.Close()
	response, err := http.Get(server.URL + "/v1/objects/interrupted.png")
	if err != nil {
		return
	} // Aborting before headers also correctly signals failure.
	defer response.Body.Close()
	if _, err = io.ReadAll(response.Body); err == nil {
		t.Fatal("partial object was accepted as a complete HTTP response")
	}
}
func TestUploadReplaySkipsUnavailableStorage(t *testing.T) {
	svc := service.NewServer()
	defer svc.Close()
	objects := &outageStore{}
	server := httptest.NewServer(transport.NewRESTHandler(svc, objects))
	defer server.Close()
	png, err := os.ReadFile("../../testdata/demo.png")
	if err != nil {
		t.Fatal(err)
	}
	upload := func(caption string) (int, []byte) {
		t.Helper()
		var body bytes.Buffer
		form := multipart.NewWriter(&body)
		part, err := form.CreateFormFile("image", "demo.png")
		if err != nil {
			t.Fatal(err)
		}
		if _, err = part.Write(png); err != nil {
			t.Fatal(err)
		}
		for key, value := range map[string]string{"idempotency_key": "upload-outage", "owner_id": "owner", "caption": caption} {
			if err = form.WriteField(key, value); err != nil {
				t.Fatal(err)
			}
		}
		if err = form.Close(); err != nil {
			t.Fatal(err)
		}
		response, err := http.Post(server.URL+"/v1/images/upload", form.FormDataContentType(), &body)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		return response.StatusCode, data
	}
	status, first := upload("original")
	if status != 201 {
		t.Fatalf("first upload: %d %s", status, first)
	}
	objects.down.Store(true)
	status, replay := upload("original")
	if status != 201 {
		t.Fatalf("replay during outage: %d %s", status, replay)
	}
	// Fake URL is stable, so the entire successful response should match.
	if !bytes.Equal(first, replay) {
		t.Fatalf("replay changed response: %s vs %s", first, replay)
	}
	status, _ = upload("conflicting caption")
	if status != 400 {
		t.Fatalf("conflict: got %d", status)
	}
	if got := objects.puts.Load(); got != 1 {
		t.Fatalf("replay/conflict touched storage: %d puts", got)
	}
}
