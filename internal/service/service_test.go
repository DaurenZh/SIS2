package service_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/DaurenZh/SIS2/internal/client"
	"github.com/DaurenZh/SIS2/internal/imagev1"
	"github.com/DaurenZh/SIS2/internal/service"
	"github.com/DaurenZh/SIS2/internal/transport"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestMetadataPersistsAcrossServerInstances(t *testing.T) {
	path := t.TempDir() + "/images.db"
	first, err := service.NewPersistentServer(path)
	if err != nil {
		t.Fatal(err)
	}
	created, err := first.CreateImage(context.Background(), &imagev1.CreateImageRequest{
		IdempotencyKey: "persistent-key", OwnerId: "owner", Caption: "saved",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := service.NewPersistentServer(path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	retrieved, err := second.GetImage(context.Background(), &imagev1.GetImageRequest{ImageId: created.Image.ImageId})
	if err != nil {
		t.Fatal(err)
	}
	if retrieved.Image.Caption != "saved" {
		t.Fatalf("got caption %q", retrieved.Image.Caption)
	}
	replayed, err := second.CreateImage(context.Background(), &imagev1.CreateImageRequest{
		IdempotencyKey: "persistent-key", OwnerId: "owner", Caption: "saved",
	})
	if err != nil {
		t.Fatal(err)
	}
	if replayed.Image.ImageId != created.Image.ImageId {
		t.Fatalf("got new image %q, want %q", replayed.Image.ImageId, created.Image.ImageId)
	}
}

func TestSocialMetadataUsesSQLite(t *testing.T) {
	svc := service.NewServer()
	defer svc.Close()
	ctx := context.Background()
	user, err := svc.CreateUser(ctx, &imagev1.CreateUserRequest{Username: "alice"})
	if err != nil {
		t.Fatal(err)
	}
	image, err := svc.CreateImage(ctx, &imagev1.CreateImageRequest{
		IdempotencyKey: "social-image", OwnerId: user.User.UserId, Caption: "hello",
	})
	if err != nil {
		t.Fatal(err)
	}
	comment, err := svc.CreateComment(ctx, &imagev1.CreateCommentRequest{
		ImageId: image.Image.ImageId, UserId: user.User.UserId, Text: "nice",
	})
	if err != nil {
		t.Fatal(err)
	}
	if comment.Comment.Text != "nice" {
		t.Fatalf("got comment %q", comment.Comment.Text)
	}
	like, err := svc.LikeImage(ctx, &imagev1.LikeImageRequest{ImageId: image.Image.ImageId, UserId: user.User.UserId})
	if err != nil {
		t.Fatal(err)
	}
	if like.LikesCount != 1 {
		t.Fatalf("got like count %d", like.LikesCount)
	}
	share, err := svc.CreateShareLink(ctx, &imagev1.CreateShareLinkRequest{
		ImageId: image.Image.ImageId, UserId: user.User.UserId, ExpiresInSeconds: 60,
	})
	if err != nil {
		t.Fatal(err)
	}
	if share.Token == "" || share.ExpiresAt == "" {
		t.Fatal("expected share link token and expiry")
	}
	stored, err := svc.GetImage(ctx, &imagev1.GetImageRequest{ImageId: image.Image.ImageId})
	if err != nil {
		t.Fatal(err)
	}
	if stored.Image.CommentsCount != 1 || stored.Image.LikesCount != 1 || stored.Image.SharesCount != 1 {
		t.Fatalf("unexpected counters: %+v", stored.Image)
	}
}

func testClient(t *testing.T, timeout time.Duration, attempts int) (*client.Client, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	svc := service.NewServer()
	server := transport.NewGRPCServer(svc)
	go server.Serve(listener)
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	return client.New(conn, timeout, attempts), func() { conn.Close(); server.Stop(); listener.Close(); svc.Close() }
}

func TestCreateIsIdempotentAfterResponseTimeout(t *testing.T) {
	api, cleanup := testClient(t, 10*time.Millisecond, 3)
	defer cleanup()
	response, err := api.CreateImage(context.Background(), &imagev1.CreateImageRequest{
		IdempotencyKey: "same-write", OwnerId: "owner", ResponseDelayMs: 30,
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Image.ImageId == "" {
		t.Fatal("expected an image ID")
	}
}

func TestDeadlineStopsRetries(t *testing.T) {
	api, cleanup := testClient(t, 10*time.Millisecond, 5)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	_, err := api.CreateImage(ctx, &imagev1.CreateImageRequest{
		IdempotencyKey: "deadline", OwnerId: "owner", ResponseDelayMs: 100,
	})
	if err == nil {
		t.Fatal("expected deadline error")
	}
}
