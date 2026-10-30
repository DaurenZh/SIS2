// Package client applies the same timeout/retry policy to gRPC and REST.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	pb "github.com/DaurenZh/SIS2/internal/imagev1"
	"github.com/DaurenZh/SIS2/internal/rpcspec"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type Client struct {
	conn              grpc.ClientConnInterface
	baseURL           string
	http              *http.Client
	attemptTimeout    time.Duration
	maxAttempts       int
	Attempts          atomic.Int64
	Retries           atomic.Int64
	RequestBodyBytes  atomic.Int64
	ResponseBodyBytes atomic.Int64
}

func New(conn grpc.ClientConnInterface, timeout time.Duration, attempts int) *Client {
	return &Client{conn: conn, attemptTimeout: timeout, maxAttempts: attempts}
}
func NewREST(base string, h *http.Client, timeout time.Duration, attempts int) *Client {
	if h == nil {
		h = &http.Client{}
	}
	return &Client{baseURL: base, http: h, attemptTimeout: timeout, maxAttempts: attempts}
}

// Call accepts generated messages for either protocol. Business methods below
// give convenient typed access for the most important create/read examples.
func (c *Client) Call(ctx context.Context, method string, req, out proto.Message) error {
	if c.maxAttempts < 1 || c.maxAttempts > 10 || c.attemptTimeout <= 0 {
		return status.Error(codes.InvalidArgument, "attempts must be 1..10 and timeout positive")
	}
	op, err := rpcspec.Find(method)
	if err != nil {
		return status.Error(codes.Unimplemented, err.Error())
	}
	// Bound the whole operation even if the caller forgot a deadline. Each retry
	// uses a fresh child deadline, bounded by this parent deadline.
	budget := time.Duration(c.maxAttempts) * c.attemptTimeout
	for i := 0; i < c.maxAttempts-1; i++ {
		budget += backoffCap(i)
	}
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	safe := method == "GetImage" || method == "ListUserImages" || method == "LikeImage" || method == "DeleteImage" || method == "CompleteUpload" || rpcspec.String(req, "idempotency_key") != ""
	var last error
	for attempt := 0; attempt < c.maxAttempts; attempt++ {
		if ctx.Err() != nil {
			return status.FromContextError(ctx.Err()).Err()
		}
		attemptCtx, stop := context.WithTimeout(ctx, c.attemptTimeout)
		c.Attempts.Add(1)
		// Reset to avoid stale fields when a previous attempt partially decoded.
		proto.Reset(out)
		if c.conn != nil {
			if key := rpcspec.String(req, "idempotency_key"); key != "" {
				attemptCtx = metadata.AppendToOutgoingContext(attemptCtx, "idempotency-key", key)
			}
			c.RequestBodyBytes.Add(int64(proto.Size(req)))
			last = c.conn.Invoke(attemptCtx, "/sis.image.v1.ImageService/"+method, req, out)
			if last == nil {
				c.ResponseBodyBytes.Add(int64(proto.Size(out)))
			}
		} else {
			last = c.rest(attemptCtx, op, req, out)
		}
		stop()
		if last == nil {
			return nil
		}
		if !safe || !retryable(last) || attempt == c.maxAttempts-1 {
			return last
		}
		// Full jitter spreads retries. Bounded attempts and a global operation budget
		// keep one caller bounded; a production fleet also needs a shared retry budget.
		wait := time.Duration(rand.Int64N(int64(backoffCap(attempt)) + 1))
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return status.FromContextError(ctx.Err()).Err()
		case <-timer.C:
		}
		c.Retries.Add(1)
	}
	return last
}
func backoffCap(attempt int) time.Duration {
	d := 20 * time.Millisecond * time.Duration(1<<attempt)
	if d > 200*time.Millisecond {
		return 200 * time.Millisecond
	}
	return d
}
func retryable(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.Aborted, codes.ResourceExhausted, codes.DeadlineExceeded:
		return true
	}
	return false
}
func (c *Client) rest(ctx context.Context, op rpcspec.Operation, req, out proto.Message) error {
	var body []byte
	var err error
	if op.Verb != "GET" {
		body, err = (protojson.MarshalOptions{UseProtoNames: true}).Marshal(req)
		if err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
	}
	r, err := http.NewRequestWithContext(ctx, op.Verb, c.baseURL+op.URL(req), bytes.NewReader(body))
	if err != nil {
		return err
	}
	r.Header.Set("Content-Type", "application/json")
	if key := rpcspec.String(req, "idempotency_key"); key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	if deadline, ok := ctx.Deadline(); ok {
		ms := (time.Until(deadline) + time.Millisecond - 1) / time.Millisecond
		if ms < 1 {
			ms = 1
		}
		if ms > 60000 {
			ms = 60000
		}
		r.Header.Set("X-Request-Timeout-Ms", strconv.FormatInt(int64(ms), 10))
	}
	c.RequestBodyBytes.Add(int64(len(body)))
	response, err := c.http.Do(r)
	if err != nil {
		if ctx.Err() != nil {
			return status.FromContextError(ctx.Err()).Err()
		}
		return status.Error(codes.Unavailable, "HTTP transport: "+err.Error())
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	c.ResponseBodyBytes.Add(int64(len(data)))
	if err != nil {
		if ctx.Err() != nil {
			return status.FromContextError(ctx.Err()).Err()
		}
		return status.Error(codes.Unavailable, "read response")
	}
	if response.StatusCode >= 400 {
		var failure struct {
			Code  int32  `json:"code"`
			Error string `json:"error"`
		}
		if json.Unmarshal(data, &failure) == nil && failure.Code > 0 && failure.Code <= 16 {
			return status.Error(codes.Code(failure.Code), failure.Error)
		}
		switch response.StatusCode {
		case 502, 503:
			return status.Error(codes.Unavailable, "HTTP upstream unavailable")
		case 504:
			return status.Error(codes.DeadlineExceeded, "HTTP upstream timeout")
		case 429:
			return status.Error(codes.ResourceExhausted, "HTTP rate limited")
		}
		return status.Error(codes.Unknown, fmt.Sprintf("HTTP %d", response.StatusCode))
	}
	if err = protojson.Unmarshal(data, out); err != nil {
		return status.Error(codes.Internal, "decode HTTP response: "+err.Error())
	}
	return nil
}
func (c *Client) CreateImage(ctx context.Context, r *pb.CreateImageRequest) (*pb.CreateImageResponse, error) {
	out := &pb.CreateImageResponse{}
	err := c.Call(ctx, "CreateImage", r, out)
	return out, err
}
func (c *Client) GetImage(ctx context.Context, r *pb.GetImageRequest) (*pb.GetImageResponse, error) {
	out := &pb.GetImageResponse{}
	err := c.Call(ctx, "GetImage", r, out)
	return out, err
}
