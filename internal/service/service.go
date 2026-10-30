package service

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/DaurenZh/SIS2/internal/imagev1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	_ "modernc.org/sqlite"
)

type Server struct {
	db *sql.DB
}

func NewServer() *Server {
	server, err := NewPersistentServer(":memory:")
	if err != nil {
		panic(err)
	}
	return server
}

func NewPersistentServer(path string) (*Server, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open metadata database: %w", err)
	}
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS images (
			image_id TEXT PRIMARY KEY,
			idempotency_key TEXT NOT NULL UNIQUE,
			owner_id TEXT NOT NULL,
			caption TEXT NOT NULL,
			status TEXT NOT NULL
		);
	`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("create metadata schema: %w", err)
	}
	return &Server{db: db}, nil
}

func (s *Server) Close() error {
	return s.db.Close()
}

func (s *Server) CreateImage(ctx context.Context, req *imagev1.CreateImageRequest) (*imagev1.CreateImageResponse, error) {
	if req.IdempotencyKey == "" {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key is required")
	}
	if req.OwnerID == "" {
		return nil, status.Error(codes.InvalidArgument, "owner_id is required")
	}
	image, err := s.findByIdempotencyKey(ctx, req.IdempotencyKey)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read idempotency record: %v", err)
	}
	if image != nil {
		return &imagev1.CreateImageResponse{Image: image}, nil
	}

	imageID := fmt.Sprintf("img-%d", time.Now().UnixNano())
	image = &imagev1.Image{ImageID: imageID, OwnerID: req.OwnerID, Caption: req.Caption, Status: "READY"}
	_, err = s.db.ExecContext(ctx, `INSERT INTO images (image_id, idempotency_key, owner_id, caption, status) VALUES (?, ?, ?, ?, ?)`,
		image.ImageID, req.IdempotencyKey, image.OwnerID, image.Caption, image.Status)
	if err != nil {
		image, lookupErr := s.findByIdempotencyKey(ctx, req.IdempotencyKey)
		if lookupErr == nil && image != nil {
			return &imagev1.CreateImageResponse{Image: image}, nil
		}
		return nil, status.Errorf(codes.Internal, "store image metadata: %v", err)
	}

	if req.ResponseDelayMS > 0 {
		time.Sleep(time.Duration(req.ResponseDelayMS) * time.Millisecond)
	}
	return &imagev1.CreateImageResponse{Image: image}, nil
}

func (s *Server) GetImage(ctx context.Context, req *imagev1.GetImageRequest) (*imagev1.GetImageResponse, error) {
	var image imagev1.Image
	err := s.db.QueryRowContext(ctx, `SELECT image_id, owner_id, caption, status FROM images WHERE image_id = ?`, req.ImageID).
		Scan(&image.ImageID, &image.OwnerID, &image.Caption, &image.Status)
	if err == sql.ErrNoRows {
		return nil, status.Error(codes.NotFound, "image not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read image metadata: %v", err)
	}
	return &imagev1.GetImageResponse{Image: &image}, nil
}

func (s *Server) findByIdempotencyKey(ctx context.Context, key string) (*imagev1.Image, error) {
	var image imagev1.Image
	err := s.db.QueryRowContext(ctx, `SELECT image_id, owner_id, caption, status FROM images WHERE idempotency_key = ?`, key).
		Scan(&image.ImageID, &image.OwnerID, &image.Caption, &image.Status)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &image, nil
}
