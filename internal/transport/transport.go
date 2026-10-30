package transport

import (
	"encoding/json"
	"net/http"

	"github.com/DaurenZh/SIS2/internal/imagev1"
	"github.com/DaurenZh/SIS2/internal/service"
	"google.golang.org/grpc"
)

type JSONCodec struct{}

func (JSONCodec) Name() string                    { return "sis-json" }
func (JSONCodec) Marshal(v any) ([]byte, error)   { return json.Marshal(v) }
func (JSONCodec) Unmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }

func NewGRPCServer(svc *service.Server) *grpc.Server {
	server := grpc.NewServer()
	imagev1.RegisterImageServiceServer(server, svc)
	return server
}

func NewRESTHandler(svc *service.Server) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/images", func(w http.ResponseWriter, r *http.Request) {
		var req imagev1.CreateImageRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		response, err := svc.CreateImage(r.Context(), &req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	})
	return mux
}
