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
		IdempotencyKey: "persistent-key", OwnerID: "owner", Caption: "saved",
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
	retrieved, err := second.GetImage(context.Background(), &imagev1.GetImageRequest{ImageID: created.Image.ImageID})
	if err != nil {
		t.Fatal(err)
	}
	if retrieved.Image.Caption != "saved" {
		t.Fatalf("got caption %q", retrieved.Image.Caption)
	}
	replayed, err := second.CreateImage(context.Background(), &imagev1.CreateImageRequest{
		IdempotencyKey: "persistent-key", OwnerID: "owner",
	})
	if err != nil {
		t.Fatal(err)
	}
	if replayed.Image.ImageID != created.Image.ImageID {
		t.Fatalf("got new image %q, want %q", replayed.Image.ImageID, created.Image.ImageID)
	}
}

func testClient(t *testing.T, timeout time.Duration, attempts int) (*client.Client, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := transport.NewGRPCServer(service.NewServer())
	go server.Serve(listener)
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	return client.New(conn, timeout, attempts), func() { conn.Close(); server.Stop(); listener.Close() }
}

func TestCreateIsIdempotentAfterResponseTimeout(t *testing.T) {
	api, cleanup := testClient(t, 10*time.Millisecond, 3)
	defer cleanup()
	response, err := api.CreateImage(context.Background(), &imagev1.CreateImageRequest{
		IdempotencyKey: "same-write", OwnerID: "owner", ResponseDelayMS: 30,
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Image.ImageID == "" {
		t.Fatal("expected an image ID")
	}
}

func TestDeadlineStopsRetries(t *testing.T) {
	api, cleanup := testClient(t, 10*time.Millisecond, 5)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()
	_, err := api.CreateImage(ctx, &imagev1.CreateImageRequest{
		IdempotencyKey: "deadline", OwnerID: "owner", ResponseDelayMS: 100,
	})
	if err == nil {
		t.Fatal("expected deadline error")
	}
}
