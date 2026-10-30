package transport

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"unicode/utf8"

	pb "github.com/DaurenZh/SIS2/internal/imagev1"
	"github.com/DaurenZh/SIS2/internal/service"
	"github.com/DaurenZh/SIS2/internal/storage"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

func registerObjects(mux *http.ServeMux, svc *service.Server, objects storage.Store) {
	mux.HandleFunc("POST /v1/images/upload", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 17<<20)
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			http.Error(w, "invalid multipart form or upload too large (16 MiB max image)", 400)
			return
		}
		defer r.MultipartForm.RemoveAll()
		f, h, err := r.FormFile("image")
		if err != nil {
			http.Error(w, "image is required", 400)
			return
		}
		defer f.Close()
		if h.Size <= 0 || h.Size > 16<<20 {
			http.Error(w, "image must be 1 byte..16 MiB", 400)
			return
		}
		key := r.Header.Get("Idempotency-Key")
		formKey := r.FormValue("idempotency_key")
		if key != "" && formKey != "" && key != formKey {
			http.Error(w, "conflicting idempotency keys", 400)
			return
		}
		if key == "" {
			key = formKey
		}
		owner := r.FormValue("owner_id")
		caption := r.FormValue("caption")
		if key == "" || len(key) > 200 || owner == "" || !validUploadCaption(caption) {
			http.Error(w, "valid key, owner and caption required", 400)
			return
		}
		head := make([]byte, 512)
		n, _ := io.ReadFull(f, head)
		contentType := http.DetectContentType(head[:n])
		ext := map[string]string{"image/jpeg": "jpg", "image/png": "png", "image/webp": "webp"}[contentType]
		if ext == "" {
			http.Error(w, "only JPEG, PNG and WebP supported", 400)
			return
		}
		if _, err = f.Seek(0, io.SeekStart); err != nil {
			http.Error(w, "read upload", 500)
			return
		}
		hash := sha256.New()
		if _, err = io.Copy(hash, f); err != nil {
			http.Error(w, "hash upload", 500)
			return
		}
		if _, err = f.Seek(0, io.SeekStart); err != nil {
			http.Error(w, "read upload", 500)
			return
		}
		// Content in the key prevents a conflicting replay from overwriting the first
		// upload's object. SQL and S3 are not one transaction: failures can leave orphans.
		namespace := sha256.Sum256([]byte(owner + "\x00" + key))
		objectKey := "images/" + hex.EncodeToString(namespace[:]) + "/" + hex.EncodeToString(hash.Sum(nil)) + "." + ext
		request := &pb.CreateImageRequest{IdempotencyKey: key, OwnerId: owner, Caption: caption, ObjectKey: objectKey}
		created, replayed, err := svc.ReplayCreateImage(r.Context(), request)
		if err != nil {
			writeError(w, err)
			return
		}
		if !replayed {
			if err = objects.Put(r.Context(), objectKey, f, h.Size, contentType); err != nil {
				http.Error(w, "object store upload failed", http.StatusBadGateway)
				return
			}
			created, err = svc.CreateImage(r.Context(), request)
			if err != nil {
				writeError(w, err)
				return
			}
		}
		ready, err := svc.CompleteUpload(r.Context(), &pb.CompleteUploadRequest{ImageId: created.Image.ImageId})
		if err != nil {
			writeError(w, err)
			return
		}
		objectURL, err := objects.URL(r.Context(), objectKey)
		if err != nil {
			writeError(w, status.Error(codes.Unavailable, "object URL generation failed"))
			return
		}
		// Use the same protobuf JSON mapping as the metadata endpoints.
		data, err := marshalImage(ready.Image)
		if err != nil {
			http.Error(w, "encode image", 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(struct {
			Image     json.RawMessage `json:"image"`
			ObjectKey string          `json:"object_key"`
			ObjectURL string          `json:"object_url"`
		}{data, objectKey, objectURL})
	})
	mux.HandleFunc("GET /v1/objects/{key...}", func(w http.ResponseWriter, r *http.Request) {
		object, err := objects.Open(r.Context(), r.PathValue("key"))
		if err != nil {
			http.NotFound(w, r)
			return
		}
		defer object.Close()
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if _, err := io.Copy(w, object); err != nil {
			// Once bytes are sent, a JSON error/status cannot replace them. Abort the
			// stream so HTTP/1 clients see an incomplete body and HTTP/2 resets it.
			panic(http.ErrAbortHandler)
		}
	})
}

func validUploadCaption(s string) bool {
	return utf8.ValidString(s) && utf8.RuneCountInString(s) <= 1000
}
func marshalImage(i *pb.Image) ([]byte, error) {
	return (protojson.MarshalOptions{UseProtoNames: true}).Marshal(i)
}
