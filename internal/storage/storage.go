// Package storage stores bytes separately from SQL metadata. MinIO implements
// the S3 API; Local is a development fallback, not a distributed object store.
package storage

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Store interface {
	Put(context.Context, string, io.Reader, int64, string) error
	URL(context.Context, string) (string, error)
	Open(context.Context, string) (io.ReadCloser, error)
}
type Local struct{ root, base string }

func NewLocal(root, base string) (*Local, error) {
	if err := os.MkdirAll(root, 0755); err != nil {
		return nil, err
	}
	return &Local{root: root, base: strings.TrimRight(base, "/")}, nil
}
func (s *Local) path(key string) (string, error) {
	// Preserve directories (basename-only mapping causes collisions), while
	// rejecting traversal outside the configured object root.
	if key == "" || !filepath.IsLocal(key) || strings.Contains(key, "\\") {
		return "", fmt.Errorf("invalid object key")
	}
	return filepath.Join(s.root, key), nil
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}
func (s *Local) Put(ctx context.Context, key string, body io.Reader, size int64, _ string) error {
	path, err := s.path(key)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".upload-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	// Rename only a complete object; failed copies must not truncate a prior upload.
	n, copyErr := io.Copy(f, contextReader{ctx, body})
	closeErr := f.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	if size >= 0 && n != size {
		return fmt.Errorf("object size mismatch")
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}
func (s *Local) URL(_ context.Context, key string) (string, error) {
	if _, err := s.path(key); err != nil {
		return "", err
	}
	return s.base + "/v1/objects/" + strings.ReplaceAll(url.PathEscape(key), "%2F", "/"), nil
}
func (s *Local) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path, err := s.path(key)
	if err != nil {
		return nil, err
	}
	return os.Open(path)
}

type MinIO struct {
	client *minio.Client
	bucket string
	expiry time.Duration
}

func NewMinIO(endpoint, accessKey, secretKey, bucket string, useSSL bool) (*MinIO, error) {
	if bucket == "" || accessKey == "" || secretKey == "" {
		return nil, fmt.Errorf("MinIO bucket and credentials are required")
	}
	// Explicit region allows presigned URLs to be signed offline, including
	// after a service restart during a MinIO outage (no bucket-region lookup).
	region := os.Getenv("MINIO_REGION")
	if region == "" {
		region = "us-east-1"
	} // Default region of the local MinIO demo.
	c, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(accessKey, secretKey, ""), Secure: useSSL, Region: region})
	if err != nil {
		return nil, err
	}
	return &MinIO{client: c, bucket: bucket, expiry: 15 * time.Minute}, nil
}
func (s *MinIO) Put(ctx context.Context, key string, r io.Reader, n int64, contentType string) error {
	_, err := s.client.PutObject(ctx, s.bucket, key, r, n, minio.PutObjectOptions{ContentType: contentType})
	return err
}
func (s *MinIO) URL(ctx context.Context, key string) (string, error) {
	u, err := s.client.PresignedGetObject(ctx, s.bucket, key, s.expiry, nil)
	if err != nil {
		return "", err
	}
	return u.String(), nil
}
func (s *MinIO) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	object, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	// GetObject is lazy; Stat discovers missing objects before HTTP sends 200.
	if _, err = object.Stat(); err != nil {
		object.Close()
		return nil, err
	}
	return object, nil
}

// EnsureBucket is opt-in convenience for local demos; normal deployments
// provision buckets separately and give the service narrower permissions.
func (s *MinIO) EnsureBucket(ctx context.Context) error {
	exists, err := s.client.BucketExists(ctx, s.bucket)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	return s.client.MakeBucket(ctx, s.bucket, minio.MakeBucketOptions{})
}
