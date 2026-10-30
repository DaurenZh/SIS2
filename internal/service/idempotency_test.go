package service

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"testing"
	"time"

	pb "github.com/DaurenZh/SIS2/internal/imagev1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestExpiryAndRollback(t *testing.T) {
	s := NewServer()
	defer s.Close()
	ctx := context.Background()
	r := &pb.CreateImageRequest{IdempotencyKey: "ttl", OwnerId: "owner", Caption: "first"}
	first, err := s.CreateImage(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	r.Caption = "changed"
	if _, err = s.CreateImage(ctx, r); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("conflict: %v", err)
	}
	if _, err = s.db.Exec(`UPDATE idempotency SET expires_at=? WHERE key='ttl'`, time.Now().Add(-time.Second).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	second, err := s.CreateImage(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	if first.Image.ImageId == second.Image.ImageId {
		t.Fatal("expired key unexpectedly replayed")
	}
	// Invalid writes must not poison a key: neither business rows nor replay rows
	// survive a failed transaction.
	comment := &pb.CreateCommentRequest{IdempotencyKey: "rollback", ImageId: first.Image.ImageId, UserId: "missing", Text: "hello"}
	if _, err = s.CreateComment(ctx, comment); status.Code(err) != codes.NotFound {
		t.Fatalf("missing user: %v", err)
	}
	var count int
	if err = s.db.QueryRow(`SELECT COUNT(*) FROM idempotency WHERE key='rollback'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed write cached: count=%d err=%v", count, err)
	}
}
func TestAfterCommitCancellationDoesNotUndoWrite(t *testing.T) {
	s := NewServer()
	defer s.Close()
	r := &pb.CreateImageRequest{IdempotencyKey: "committed", OwnerId: "owner", ResponseDelayMs: 1000}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := s.CreateImage(ctx, r); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("got %v", err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("server ignored cancellation")
	}
	replay, err := s.CreateImage(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	var count int
	if err = s.db.QueryRow(`SELECT COUNT(*) FROM images`).Scan(&count); err != nil || count != 1 || replay.Image == nil {
		t.Fatalf("count %d err %v", count, err)
	}
}

func TestPaginationWithIdenticalTimestamps(t *testing.T) {
	s := NewServer()
	defer s.Close()
	ctx := context.Background()
	ids := make([]string, 5)
	for i := range ids {
		out, err := s.CreateImage(ctx, &pb.CreateImageRequest{IdempotencyKey: fmt.Sprint("tie-", i), OwnerId: "owner", ObjectKey: "demo.jpg"})
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = out.Image.ImageId
		if _, err = s.CompleteUpload(ctx, &pb.CompleteUploadRequest{ImageId: ids[i]}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.db.Exec(`UPDATE images SET created_at='2026-10-30T12:00:00.000000000Z'`); err != nil {
		t.Fatal(err)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(ids)))
	var got []string
	cursor := ""
	for {
		page, err := s.ListUserImages(ctx, &pb.ListUserImagesRequest{OwnerId: "owner", Limit: 2, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		for _, image := range page.Items {
			got = append(got, image.ImageId)
		}
		cursor = page.NextCursor
		if cursor == "" {
			break
		}
	}
	if !slices.Equal(ids, got) {
		t.Fatalf("tie pagination: got %v want %v", got, ids)
	}
}
