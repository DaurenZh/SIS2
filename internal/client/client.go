package client

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/DaurenZh/SIS2/internal/imagev1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Client struct {
	api            imagev1.ImageServiceClient
	attemptTimeout time.Duration
	maxAttempts    int
}

func New(conn grpc.ClientConnInterface, attemptTimeout time.Duration, maxAttempts int) *Client {
	return &Client{api: imagev1.NewImageServiceClient(conn), attemptTimeout: attemptTimeout, maxAttempts: maxAttempts}
}

func (c *Client) CreateImage(ctx context.Context, req *imagev1.CreateImageRequest) (*imagev1.CreateImageResponse, error) {
	var lastErr error
	for attempt := 0; attempt < c.maxAttempts; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, c.attemptTimeout)
		response, err := c.api.CreateImage(attemptCtx, req, grpc.ForceCodec(codec{}))
		cancel()
		if err == nil {
			return response, nil
		}
		lastErr = err
		if !retryable(err) || ctx.Err() != nil {
			break
		}
		backoff := time.Duration(math.Pow(2, float64(attempt))) * 5 * time.Millisecond
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
	}
	return nil, lastErr
}

func retryable(err error) bool {
	code := status.Code(err)
	return code == codes.Unavailable || code == codes.DeadlineExceeded || errors.Is(err, context.DeadlineExceeded)
}

type codec struct{}

func (codec) Name() string                    { return "sis-json" }
func (codec) Marshal(v any) ([]byte, error)   { return jsonCodec{}.Marshal(v) }
func (codec) Unmarshal(b []byte, v any) error { return jsonCodec{}.Unmarshal(b, v) }

type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error)   { return json.Marshal(v) }
func (jsonCodec) Unmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }
