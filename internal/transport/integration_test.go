package transport_test

import (
	"context"
	"fmt"
	"net"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/DaurenZh/SIS2/internal/client"
	pb "github.com/DaurenZh/SIS2/internal/imagev1"
	"github.com/DaurenZh/SIS2/internal/service"
	"github.com/DaurenZh/SIS2/internal/transport"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func setup(t *testing.T, protocol string, f *transport.Faults, timeout time.Duration, attempts int) (*client.Client, *service.Server) {
	t.Helper()
	svc := service.NewServer()
	t.Cleanup(func() { svc.Close() })
	if protocol == "rest" {
		server := httptest.NewServer(transport.NewRESTHandler(svc, nil, f))
		t.Cleanup(server.Close)
		return client.NewREST(server.URL, server.Client(), timeout, attempts), svc
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := transport.NewGRPCServer(svc, f)
	go server.Serve(l)
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient(l.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return client.New(conn, timeout, attempts), svc
}
func TestBothProtocols(t *testing.T) {
	for _, protocol := range []string{"grpc", "rest"} {
		t.Run(protocol, func(t *testing.T) {
			t.Run("lost response replays one write", func(t *testing.T) {
				c, s := setup(t, protocol, &transport.Faults{Method: "CreateImage", FailAfter: 1}, 300*time.Millisecond, 4)
				r := &pb.CreateImageRequest{IdempotencyKey: "lost", OwnerId: "owner", Caption: "original"}
				first, err := c.CreateImage(context.Background(), r)
				if err != nil {
					t.Fatal(err)
				}
				second, err := c.CreateImage(context.Background(), r)
				if err != nil {
					t.Fatal(err)
				}
				if c.Attempts.Load() != 3 || c.Retries.Load() != 1 || first.Image.ImageId != second.Image.ImageId {
					t.Fatalf("attempts %d, retries %d, ids %q %q", c.Attempts.Load(), c.Retries.Load(), first.Image.ImageId, second.Image.ImageId)
				}
				// Completion and listing provide independent evidence of exactly one row.
				if _, err = s.CompleteUpload(context.Background(), &pb.CompleteUploadRequest{ImageId: first.Image.ImageId}); status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("missing object must block completion: %v", err)
				}
				r.Caption = "different"
				if _, err = c.CreateImage(context.Background(), r); status.Code(err) != codes.InvalidArgument {
					t.Fatalf("conflicting key: %v", err)
				}
				if c.Attempts.Load() != 4 {
					t.Fatal("non-retryable conflict was retried")
				}
			})
			t.Run("50 concurrent duplicates", func(t *testing.T) {
				c, _ := setup(t, protocol, nil, time.Second, 4)
				var wg sync.WaitGroup
				results := make(chan string, 50)
				for i := 0; i < 50; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						r, e := c.CreateImage(context.Background(), &pb.CreateImageRequest{IdempotencyKey: "parallel", OwnerId: "owner"})
						if e != nil {
							results <- "ERROR: " + e.Error()
						} else {
							results <- r.Image.ImageId
						}
					}()
				}
				wg.Wait()
				close(results)
				id := ""
				for result := range results {
					if id == "" {
						id = result
					}
					if result != id || len(result) < 4 || result[:4] != "img-" {
						t.Fatalf("different result %q vs %q", result, id)
					}
				}
			})
			t.Run("deadline cancels before commit", func(t *testing.T) {
				c, s := setup(t, protocol, &transport.Faults{Method: "CreateImage", Delay: time.Second}, 20*time.Millisecond, 2)
				ctx, cancel := context.WithTimeout(context.Background(), 55*time.Millisecond)
				defer cancel()
				start := time.Now()
				_, err := c.CreateImage(ctx, &pb.CreateImageRequest{IdempotencyKey: "slow", OwnerId: "owner", Caption: "not committed"})
				if status.Code(err) != codes.DeadlineExceeded {
					t.Fatalf("expected deadline: %v", err)
				}
				if time.Since(start) > 400*time.Millisecond || c.Attempts.Load() > 2 {
					t.Fatal("deadline or attempt bound ignored")
				}
				// A different caption with this key succeeds only if the delayed work did not commit.
				if _, err = s.CreateImage(context.Background(), &pb.CreateImageRequest{IdempotencyKey: "slow", OwnerId: "owner", Caption: "later"}); err != nil {
					t.Fatal(err)
				}
			})
			t.Run("exhaustion and unsafe writes", func(t *testing.T) {
				c, _ := setup(t, protocol, &transport.Faults{Method: "CreateUser", FailBefore: 100}, 100*time.Millisecond, 3)
				err := c.Call(context.Background(), "CreateUser", &pb.CreateUserRequest{Username: "alice"}, &pb.CreateUserResponse{})
				if status.Code(err) != codes.Unavailable || c.Attempts.Load() != 1 {
					t.Fatalf("unsafe retry: attempts=%d err=%v", c.Attempts.Load(), err)
				}
				err = c.Call(context.Background(), "CreateUser", &pb.CreateUserRequest{Username: "alice", IdempotencyKey: "safe"}, &pb.CreateUserResponse{})
				if status.Code(err) != codes.Unavailable || c.Attempts.Load() != 4 {
					t.Fatalf("exhaustion: attempts=%d err=%v", c.Attempts.Load(), err)
				}
			})
			t.Run("CRUD pagination and social counters", func(t *testing.T) {
				c, _ := setup(t, protocol, nil, time.Second, 3)
				ctx := context.Background()
				u := &pb.CreateUserResponse{}
				if err := c.Call(ctx, "CreateUser", &pb.CreateUserRequest{Username: "alice", IdempotencyKey: "user"}, u); err != nil {
					t.Fatal(err)
				}
				ids := []string{}
				for i := 0; i < 5; i++ {
					image, err := c.CreateImage(ctx, &pb.CreateImageRequest{IdempotencyKey: fmt.Sprint("image-", i), OwnerId: u.User.UserId, ObjectKey: "demo/key.jpg"})
					if err != nil {
						t.Fatal(err)
					}
					ids = append(ids, image.Image.ImageId)
					for j := 0; j < 2; j++ {
						if err = c.Call(ctx, "CompleteUpload", &pb.CompleteUploadRequest{ImageId: image.Image.ImageId}, &pb.GetImageResponse{}); err != nil {
							t.Fatal(err)
						}
					}
				}
				seen := map[string]bool{}
				cursor := ""
				for {
					page := &pb.ListUserImagesResponse{}
					if err := c.Call(ctx, "ListUserImages", &pb.ListUserImagesRequest{OwnerId: u.User.UserId, Limit: 2, Cursor: cursor}, page); err != nil {
						t.Fatal(err)
					}
					for _, image := range page.Items {
						if seen[image.ImageId] {
							t.Fatal("duplicate page item")
						}
						if image.ImageId != ids[len(ids)-1-len(seen)] {
							t.Fatal("wrong descending order")
						}
						seen[image.ImageId] = true
					}
					cursor = page.NextCursor
					if cursor == "" {
						break
					}
				}
				if len(seen) != 5 {
					t.Fatalf("got %d images", len(seen))
				}
				imageID := ids[0]
				for i := 0; i < 2; i++ {
					if err := c.Call(ctx, "CreateComment", &pb.CreateCommentRequest{ImageId: imageID, UserId: u.User.UserId, Text: "nice", IdempotencyKey: "comment"}, &pb.CreateCommentResponse{}); err != nil {
						t.Fatal(err)
					}
					if err := c.Call(ctx, "LikeImage", &pb.LikeImageRequest{ImageId: imageID, UserId: u.User.UserId}, &pb.LikeImageResponse{}); err != nil {
						t.Fatal(err)
					}
					if err := c.Call(ctx, "CreateShareLink", &pb.CreateShareLinkRequest{ImageId: imageID, UserId: u.User.UserId, IdempotencyKey: "share"}, &pb.CreateShareLinkResponse{}); err != nil {
						t.Fatal(err)
					}
				}
				image, err := c.GetImage(ctx, &pb.GetImageRequest{ImageId: imageID})
				if err != nil {
					t.Fatal(err)
				}
				if image.Image.LikesCount != 1 || image.Image.CommentsCount != 1 || image.Image.SharesCount != 1 {
					t.Fatalf("counters: %v", image.Image)
				}
				if err = c.Call(ctx, "UpdateImage", &pb.UpdateImageRequest{ImageId: imageID, Caption: "updated", IdempotencyKey: "update"}, &pb.GetImageResponse{}); err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 2; i++ {
					if err = c.Call(ctx, "DeleteImage", &pb.DeleteImageRequest{ImageId: imageID}, &pb.GetImageResponse{}); err != nil {
						t.Fatal(err)
					}
				}
				if _, err = c.GetImage(ctx, &pb.GetImageRequest{ImageId: imageID}); status.Code(err) != codes.NotFound {
					t.Fatalf("deleted read: %v", err)
				}
				if err = c.Call(ctx, "CompleteUpload", &pb.CompleteUploadRequest{ImageId: imageID}, &pb.GetImageResponse{}); status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("resurrected deleted image: %v", err)
				}
			})
		})
	}
}
