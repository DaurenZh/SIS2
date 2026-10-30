package transport_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	pngdecoder "image/png"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	pb "github.com/DaurenZh/SIS2/internal/imagev1"
	"github.com/DaurenZh/SIS2/internal/service"
	"github.com/DaurenZh/SIS2/internal/storage"
	"github.com/DaurenZh/SIS2/internal/transport"
)

func TestMultipartUpload(t *testing.T) {
	for _, backend := range []string{"local", "minio"} {
		t.Run(backend, func(t *testing.T) {
			svc := service.NewServer()
			defer svc.Close()
			var objects storage.Store
			if backend == "local" {
				var err error
				objects, err = storage.NewLocal(t.TempDir(), "http://unused")
				if err != nil {
					t.Fatal(err)
				}
			} else {
				endpoint := os.Getenv("SIS2_TEST_MINIO_ENDPOINT")
				if endpoint == "" {
					t.Skip("set SIS2_TEST_MINIO_ENDPOINT for real MinIO integration")
				}
				m, err := storage.NewMinIO(endpoint, "minioadmin", "minioadmin", "images", false)
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if err = m.EnsureBucket(ctx); err != nil {
					t.Fatal(err)
				}
				objects = m
			}
			server := httptest.NewServer(transport.NewRESTHandler(svc, objects))
			defer server.Close()
			png, err := os.ReadFile("../../testdata/demo.png")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := pngdecoder.Decode(bytes.NewReader(png)); err != nil {
				t.Fatalf("invalid demo PNG fixture: %v", err)
			}
			key := fmt.Sprintf("upload-%d", time.Now().UnixNano())
			type result struct {
				Image struct {
					ImageID string `json:"image_id"`
					Status  string `json:"status"`
				} `json:"image"`
				ObjectKey string `json:"object_key"`
				ObjectURL string `json:"object_url"`
			}
			upload := func(caption string) (int, result) {
				t.Helper()
				var b bytes.Buffer
				form := multipart.NewWriter(&b)
				part, e := form.CreateFormFile("image", "demo.png")
				if e != nil {
					t.Fatal(e)
				}
				part.Write(png)
				for k, v := range map[string]string{"idempotency_key": key, "owner_id": "owner", "caption": caption} {
					if e = form.WriteField(k, v); e != nil {
						t.Fatal(e)
					}
				}
				if e = form.Close(); e != nil {
					t.Fatal(e)
				}
				response, e := http.Post(server.URL+"/v1/images/upload", form.FormDataContentType(), &b)
				if e != nil {
					t.Fatal(e)
				}
				defer response.Body.Close()
				var r result
				json.NewDecoder(response.Body).Decode(&r)
				return response.StatusCode, r
			}
			code, first := upload("hello")
			if code != 201 || first.Image.Status != "READY" || first.ObjectURL == "" {
				t.Fatalf("upload %d %+v", code, first)
			}
			code, replayed := upload("hello")
			if code != 201 || first.Image.ImageID != replayed.Image.ImageID || first.ObjectKey != replayed.ObjectKey {
				t.Fatalf("replay %d %+v", code, replayed)
			}
			if code, _ = upload("conflicting caption"); code != 400 {
				t.Fatalf("expected conflicting key rejection, got %d", code)
			}
			objectURL := first.ObjectURL
			if backend == "local" {
				objectURL = server.URL + "/v1/objects/" + first.ObjectKey
			}
			response, err := http.Get(objectURL)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			downloaded, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != 200 || !bytes.Equal(downloaded, png) {
				t.Fatalf("download %d, %d bytes", response.StatusCode, len(downloaded))
			}
			page, err := svc.ListUserImages(context.Background(), &pb.ListUserImagesRequest{OwnerId: "owner"})
			if err != nil {
				t.Fatal(err)
			}
			if len(page.Items) != 1 {
				t.Fatalf("got %d images for repeated upload", len(page.Items))
			}
		})
	}
}
