package transport

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	pb "github.com/DaurenZh/SIS2/internal/imagev1"
	"github.com/DaurenZh/SIS2/internal/observability"
	"github.com/DaurenZh/SIS2/internal/rpcspec"
	"github.com/DaurenZh/SIS2/internal/service"
	"github.com/DaurenZh/SIS2/internal/storage"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Faults is deterministic: first N matching calls fail, then recover. Counters
// are atomic because real requests run concurrently. 
type Faults struct {
	Method                string
	Delay                 time.Duration
	FailBefore, FailAfter int64
	calls                 atomic.Int64
}

func (f *Faults) Invoke(ctx context.Context, method string, run func() (any, error)) (any, error) {
	if f == nil || (f.Method != "" && f.Method != method) {
		return run()
	}
	n := f.calls.Add(1)
	if f.Delay > 0 {
		timer := time.NewTimer(f.Delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		case <-timer.C:
		}
	}
	if n <= f.FailBefore {
		return nil, status.Error(codes.Unavailable, "injected failure before execution")
	}
	result, err := run()
	if err == nil && n <= f.FailBefore+f.FailAfter {
		return nil, status.Error(codes.Unavailable, "injected response loss AFTER commit")
	}
	return result, err
}
func NewGRPCServer(svc *service.Server, faults ...*Faults) *grpc.Server {
	var f *Faults
	if len(faults) > 0 {
		f = faults[0]
	}
	return NewGRPCServerWithMetrics(svc, f, nil)
}

// Metrics wraps fault injection so failed attempts are counted too.
func NewGRPCServerWithMetrics(svc *service.Server, f *Faults, metrics *observability.Metrics) *grpc.Server {
	interceptors := []grpc.UnaryServerInterceptor{}
	if metrics != nil {
		interceptors = append(interceptors, metrics.Unary)
	}
	interceptors = append(interceptors, func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		m := req.(proto.Message)
		// Support the conventional metadata key and the existing request field. A
		// disagreement is rejected rather than silently choosing one identity.
		if keys := metadata.ValueFromIncomingContext(ctx, "idempotency-key"); len(keys) > 0 {
			old := rpcspec.String(m, "idempotency_key")
			if old != "" && old != keys[0] {
				return nil, status.Error(codes.InvalidArgument, "conflicting idempotency keys")
			}
			rpcspec.SetString(m, "idempotency_key", keys[0])
		}
		method := info.FullMethod[strings.LastIndex(info.FullMethod, "/")+1:]
		return f.Invoke(ctx, method, func() (any, error) { return handler(ctx, req) })
	})
	server := grpc.NewServer(grpc.MaxRecvMsgSize(1<<20), grpc.ChainUnaryInterceptor(interceptors...))
	pb.RegisterImageServiceServer(server, svc)
	return server
}

// call is deliberately explicit: it is the small adapter between a REST route
// and the SAME business method used by generated gRPC dispatch.
func call(ctx context.Context, svc *service.Server, req proto.Message) (any, error) {
	switch r := req.(type) {
	case *pb.CreateUserRequest:
		return svc.CreateUser(ctx, r)
	case *pb.CreateImageRequest:
		return svc.CreateImage(ctx, r)
	case *pb.CompleteUploadRequest:
		return svc.CompleteUpload(ctx, r)
	case *pb.GetImageRequest:
		return svc.GetImage(ctx, r)
	case *pb.UpdateImageRequest:
		return svc.UpdateImage(ctx, r)
	case *pb.DeleteImageRequest:
		return svc.DeleteImage(ctx, r)
	case *pb.ListUserImagesRequest:
		return svc.ListUserImages(ctx, r)
	case *pb.CreateCommentRequest:
		return svc.CreateComment(ctx, r)
	case *pb.LikeImageRequest:
		return svc.LikeImage(ctx, r)
	case *pb.CreateShareLinkRequest:
		return svc.CreateShareLink(ctx, r)
	default:
		return nil, status.Error(codes.Unimplemented, "unknown operation")
	}
}
func NewRESTHandler(svc *service.Server, objects storage.Store, faults ...*Faults) http.Handler {
	var f *Faults
	if len(faults) > 0 {
		f = faults[0]
	}
	mux := http.NewServeMux()
	for _, op := range rpcspec.Operations {
		mux.HandleFunc(op.Verb+" "+op.Path, func(w http.ResponseWriter, r *http.Request) {
			req := op.Request()
			if op.Verb != "GET" {
				r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
				if r.ContentLength != 0 {
					raw, err := io.ReadAll(r.Body)
					if err != nil {
						writeError(w, status.Error(codes.InvalidArgument, "invalid JSON"))
						return
					}
					if err := protojson.Unmarshal(raw, req); err != nil {
						writeError(w, status.Error(codes.InvalidArgument, "invalid request fields"))
						return
					}
				}
			}
			for _, name := range []string{"image_id", "owner_id"} {
				if v := r.PathValue(name); v != "" {
					rpcspec.SetString(req, name, v)
				}
			}
			if lr, ok := req.(*pb.ListUserImagesRequest); ok {
				if v := r.URL.Query().Get("limit"); v != "" {
					n, err := strconv.ParseInt(v, 10, 32)
					if err != nil {
						writeError(w, status.Error(codes.InvalidArgument, "invalid limit"))
						return
					}
					lr.Limit = int32(n)
				}
				lr.Cursor = r.URL.Query().Get("cursor")
			}
			if key := r.Header.Get("Idempotency-Key"); key != "" {
				old := rpcspec.String(req, "idempotency_key")
				if old != "" && old != key {
					writeError(w, status.Error(codes.InvalidArgument, "conflicting idempotency keys"))
					return
				}
				rpcspec.SetString(req, "idempotency_key", key)
			}
			// The header carries the same remaining per-attempt deadline as gRPC's
			// grpc-timeout. Server timeouts alone do not cancel a slow handler's work.
			ctx := r.Context()
			if value := r.Header.Get("X-Request-Timeout-Ms"); value != "" {
				ms, err := strconv.ParseInt(value, 10, 64)
				if err != nil || ms < 1 || ms > 60000 {
					writeError(w, status.Error(codes.InvalidArgument, "timeout must be 1..60000 ms"))
					return
				}
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, time.Duration(ms)*time.Millisecond)
				defer cancel()
			}
			result, err := f.Invoke(ctx, op.Name, func() (any, error) { return call(ctx, svc, req) })
			if err != nil {
				writeError(w, err)
				return
			}
			data, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(result.(proto.Message))
			if err != nil {
				writeError(w, status.Error(codes.Internal, "encode response"))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			if strings.HasPrefix(op.Name, "Create") {
				w.WriteHeader(http.StatusCreated)
			}
			w.Write(data)
		})
	}
	if objects != nil {
		registerObjects(mux, svc, objects)
	}
	return mux
}
func writeError(w http.ResponseWriter, err error) {
	code := status.Code(err)
	httpCode := http.StatusInternalServerError
	switch code {
	case codes.InvalidArgument:
		httpCode = 400
	case codes.NotFound:
		httpCode = 404
	case codes.AlreadyExists, codes.Aborted:
		httpCode = 409
	case codes.FailedPrecondition:
		httpCode = 422
	case codes.ResourceExhausted:
		httpCode = 429
	case codes.Unavailable:
		httpCode = 503
	case codes.DeadlineExceeded:
		httpCode = 504
	case codes.Canceled:
		httpCode = 408
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpCode)
	json.NewEncoder(w).Encode(struct {
		Code  int32  `json:"code"`
		Error string `json:"error"`
	}{int32(code), status.Convert(err).Message()})
}
func NewHTTPServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{Addr: addr, Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
}
