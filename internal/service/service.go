// Package service owns metadata rules. Both transports call these same methods.
package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	pb "github.com/DaurenZh/SIS2/internal/imagev1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	_ "modernc.org/sqlite"
)

type Server struct {
	pb.UnimplementedImageServiceServer
	db *sql.DB
}

func NewServer() *Server {
	s, err := NewPersistentServer(":memory:")
	if err != nil {
		panic(err)
	}
	return s
}

func NewPersistentServer(path string) (*Server, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	// SQLite has a single writer. One connection also keeps :memory: one database;
	// concurrent requests wait here with their contexts instead of racing SELECT/INSERT.
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`
 PRAGMA foreign_keys = ON;
 PRAGMA busy_timeout = 5000;
 PRAGMA journal_mode = WAL;
 CREATE TABLE IF NOT EXISTS users (user_id TEXT PRIMARY KEY, username TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS images (
 image_id TEXT PRIMARY KEY, idempotency_key TEXT NOT NULL UNIQUE,
 owner_id TEXT NOT NULL, caption TEXT NOT NULL, status TEXT NOT NULL,
 object_key TEXT NOT NULL DEFAULT '', likes_count INTEGER NOT NULL DEFAULT 0,
 comments_count INTEGER NOT NULL DEFAULT 0, shares_count INTEGER NOT NULL DEFAULT 0,
 created_at TEXT NOT NULL DEFAULT '');
 CREATE TABLE IF NOT EXISTS comments (
 comment_id TEXT PRIMARY KEY, image_id TEXT NOT NULL REFERENCES images(image_id),
 user_id TEXT NOT NULL REFERENCES users(user_id), text TEXT NOT NULL, created_at TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS likes (
 image_id TEXT NOT NULL REFERENCES images(image_id), user_id TEXT NOT NULL REFERENCES users(user_id),
 created_at TEXT NOT NULL, PRIMARY KEY (image_id, user_id));
 CREATE TABLE IF NOT EXISTS share_links (
 token TEXT PRIMARY KEY, image_id TEXT NOT NULL REFERENCES images(image_id),
 user_id TEXT NOT NULL REFERENCES users(user_id), expires_at TEXT NOT NULL, created_at TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS idempotency (
 key TEXT PRIMARY KEY, fingerprint BLOB NOT NULL, response BLOB NOT NULL, expires_at INTEGER NOT NULL);
 `)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("create schema: %w", err)
	}
	// Additive migration preserves databases created by the earlier prototype.
	for _, col := range []struct{ name, ddl string }{
		{"object_key", "object_key TEXT NOT NULL DEFAULT ''"},
		{"likes_count", "likes_count INTEGER NOT NULL DEFAULT 0"},
		{"comments_count", "comments_count INTEGER NOT NULL DEFAULT 0"},
		{"shares_count", "shares_count INTEGER NOT NULL DEFAULT 0"},
		{"created_at", "created_at TEXT NOT NULL DEFAULT ''"},
	} {
		var name string
		err = db.QueryRow(`SELECT name FROM pragma_table_info('images') WHERE name = ?`, col.name).Scan(&name)
		if errors.Is(err, sql.ErrNoRows) {
			_, err = db.Exec(`ALTER TABLE images ADD COLUMN ` + col.ddl)
		}
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("migrate schema: %w", err)
		}
	}
	_, err = db.Exec(`CREATE INDEX IF NOT EXISTS images_by_owner ON images(owner_id, created_at DESC, image_id DESC)`)
	if err != nil {
		db.Close()
		return nil, err
	}
	return &Server{db: db}, nil
}
func (s *Server) Close() error { return s.db.Close() }

// PruneIdempotency bounds storage. TTL is the replay guarantee, not a promise of
// forever deduplication: after 24 hours callers must reconcile before retrying.
func (s *Server) PruneIdempotency(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM idempotency WHERE expires_at <= ?`, time.Now().UnixMilli())
	return err
}

// write commits BOTH the business change and its replay response atomically.
// A crash cannot leave a committed image without its idempotency result. There
// is no IN_PROGRESS lease to recover: SQLite rolls back uncommitted transactions.
// Requests with no key execute once per call; clients must not blindly retry them.
func write[T proto.Message](ctx context.Context, s *Server, method, key string, req proto.Message, out T, fn func(*sql.Tx) error) (T, error) {
	var zero T
	fingerprint, err := requestFingerprint(method, req)
	if err != nil {
		return zero, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return zero, dbError(ctx, err)
	}
	defer tx.Rollback()
	if key != "" {
		if len(key) > 200 {
			return zero, status.Error(codes.InvalidArgument, "idempotency key exceeds 200 bytes")
		}
		replayed, err := readReplay(ctx, tx, key, fingerprint, out)
		if err != nil {
			return zero, err
		}
		if replayed {
			return out, nil
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM idempotency WHERE key=?`, key); err != nil {
			return zero, dbError(ctx, err)
		}
	}
	if err = fn(tx); err != nil {
		return zero, dbError(ctx, err)
	}
	if key != "" {
		response, err := proto.Marshal(out)
		if err != nil {
			return zero, status.Error(codes.Internal, "encode saved response")
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO idempotency VALUES (?, ?, ?, ?)`, key, fingerprint[:], response, time.Now().Add(24*time.Hour).UnixMilli()); err != nil {
			return zero, dbError(ctx, err)
		}
	}
	if err = tx.Commit(); err != nil {
		return zero, dbError(ctx, err)
	}
	return out, nil
}

// ReplayCreateImage lets multipart uploads check a validated request before
// touching object storage. It is a lookup, not a reservation: concurrent fresh
// uploads can still write the same deterministic object key before SQL dedupes.
func (s *Server) ReplayCreateImage(ctx context.Context, req *pb.CreateImageRequest) (*pb.CreateImageResponse, bool, error) {
	fingerprint, err := requestFingerprint("CreateImage", req)
	if err != nil {
		return nil, false, err
	}
	out := &pb.CreateImageResponse{}
	found, err := readReplay(ctx, s.db, req.IdempotencyKey, fingerprint, out)
	return out, found, err
}
func requestFingerprint(method string, req proto.Message) ([32]byte, error) {
	canonical := proto.Clone(req)
	if r, ok := canonical.(*pb.CreateImageRequest); ok {
		r.ResponseDelayMs = 0
	}
	data, err := (proto.MarshalOptions{Deterministic: true}).Marshal(canonical)
	if err != nil {
		return [32]byte{}, status.Error(codes.Internal, "fingerprint request")
	}
	return sha256.Sum256(append([]byte(method+"\x00"), data...)), nil
}

type replayReader interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func readReplay(ctx context.Context, db replayReader, key string, fingerprint [32]byte, out proto.Message) (bool, error) {
	var storedHash, response []byte
	var expiry int64
	err := db.QueryRowContext(ctx, `SELECT fingerprint, response, expires_at FROM idempotency WHERE key=?`, key).Scan(&storedHash, &response, &expiry)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, dbError(ctx, err)
	}
	if expiry <= time.Now().UnixMilli() {
		return false, nil
	}
	if string(storedHash) != string(fingerprint[:]) {
		return false, status.Error(codes.InvalidArgument, "idempotency key reused with different method or payload")
	}
	if err := proto.Unmarshal(response, out); err != nil {
		return false, status.Error(codes.Internal, "decode saved response")
	}
	return true, nil
}

func dbError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return status.FromContextError(ctx.Err()).Err()
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	if errors.Is(err, sql.ErrNoRows) {
		return status.Error(codes.NotFound, "record not found")
	}
	// Do not expose SQL or database contents in API errors.
	return status.Error(codes.Internal, "metadata database operation failed")
}
func id(prefix string) string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return prefix + hex.EncodeToString(b[:])
}
func now() string                { return time.Now().UTC().Format("2006-01-02T15:04:05.000000000Z") }
func captionValid(c string) bool { return utf8.ValidString(c) && utf8.RuneCountInString(c) <= 1000 }

const imageColumns = `image_id, owner_id, caption, status, likes_count, comments_count, shares_count, object_key, created_at`

type scanner interface{ Scan(...any) error }

func scanImage(row scanner) (*pb.Image, error) {
	i := &pb.Image{}
	err := row.Scan(&i.ImageId, &i.OwnerId, &i.Caption, &i.Status, &i.LikesCount, &i.CommentsCount, &i.SharesCount, &i.ObjectKey, &i.CreatedAt)
	return i, err
}
func readImage(ctx context.Context, tx *sql.Tx, imageID string) (*pb.Image, error) {
	return scanImage(tx.QueryRowContext(ctx, `SELECT `+imageColumns+` FROM images WHERE image_id=?`, imageID))
}
func activeImage(ctx context.Context, tx *sql.Tx, imageID string) (*pb.Image, error) {
	i, err := readImage(ctx, tx, imageID)
	if err != nil {
		return nil, err
	}
	if i.Status == "DELETED" {
		return nil, status.Error(codes.FailedPrecondition, "image is deleted")
	}
	return i, nil
}

func (s *Server) CreateImage(ctx context.Context, r *pb.CreateImageRequest) (*pb.CreateImageResponse, error) {
	if r.IdempotencyKey == "" || r.OwnerId == "" || !captionValid(r.Caption) || r.ResponseDelayMs < 0 || r.ResponseDelayMs > 10000 {
		return nil, status.Error(codes.InvalidArgument, "key, owner and valid caption are required; demo delay must be 0..10000 ms")
	}
	created := false
	out := &pb.CreateImageResponse{}
	response, err := write(ctx, s, "CreateImage", r.IdempotencyKey, r, out, func(tx *sql.Tx) error {
		i := &pb.Image{ImageId: id("img-"), OwnerId: r.OwnerId, Caption: r.Caption, ObjectKey: r.ObjectKey, Status: "PENDING", CreatedAt: now()}
		// Keep the legacy NOT NULL column populated without making the key permanent:
		// the dedicated idempotency table now owns the 24-hour replay lifetime.
		_, err := tx.ExecContext(ctx, `INSERT INTO images(image_id,idempotency_key,owner_id,caption,status,object_key,created_at) VALUES(?,?,?,?,?,?,?)`, i.ImageId, i.ImageId, i.OwnerId, i.Caption, i.Status, i.ObjectKey, i.CreatedAt)
		out.Image = i
		created = true
		return err
	})
	// Fault AFTER commit: cancellation stops the wait, but cannot undo the write.
	if err == nil && created && r.ResponseDelayMs > 0 {
		timer := time.NewTimer(time.Duration(r.ResponseDelayMs) * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		case <-timer.C:
		}
	}
	return response, err
}
func (s *Server) GetImage(ctx context.Context, r *pb.GetImageRequest) (*pb.GetImageResponse, error) {
	i, err := scanImage(s.db.QueryRowContext(ctx, `SELECT `+imageColumns+` FROM images WHERE image_id=? AND status!='DELETED'`, r.ImageId))
	if err != nil {
		return nil, dbError(ctx, err)
	}
	return &pb.GetImageResponse{Image: i}, nil
}
func (s *Server) CompleteUpload(ctx context.Context, r *pb.CompleteUploadRequest) (*pb.GetImageResponse, error) {
	out := &pb.GetImageResponse{}
	return write(ctx, s, "CompleteUpload", r.IdempotencyKey, r, out, func(tx *sql.Tx) error {
		i, err := activeImage(ctx, tx, r.ImageId)
		if err != nil {
			return err
		}
		// Mock processing: no resizing worker. Real deployment verifies S3 and queues
		// PROCESSING -> READY; this prototype completes metadata synchronously.
		if i.Status == "PENDING" {
			if i.ObjectKey == "" {
				return status.Error(codes.FailedPrecondition, "upload an object before completion")
			}
			if _, err = tx.ExecContext(ctx, `UPDATE images SET status='READY' WHERE image_id=?`, r.ImageId); err != nil {
				return err
			}
			i.Status = "READY"
		}
		out.Image = i
		return nil
	})
}
func (s *Server) UpdateImage(ctx context.Context, r *pb.UpdateImageRequest) (*pb.GetImageResponse, error) {
	if r.IdempotencyKey == "" || !captionValid(r.Caption) {
		return nil, status.Error(codes.InvalidArgument, "key and caption of at most 1000 characters required")
	}
	out := &pb.GetImageResponse{}
	return write(ctx, s, "UpdateImage", r.IdempotencyKey, r, out, func(tx *sql.Tx) error {
		i, err := activeImage(ctx, tx, r.ImageId)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE images SET caption=? WHERE image_id=?`, r.Caption, r.ImageId); err != nil {
			return err
		}
		i.Caption = r.Caption
		out.Image = i
		return nil
	})
}
func (s *Server) DeleteImage(ctx context.Context, r *pb.DeleteImageRequest) (*pb.GetImageResponse, error) {
	out := &pb.GetImageResponse{}
	return write(ctx, s, "DeleteImage", r.IdempotencyKey, r, out, func(tx *sql.Tx) error {
		i, err := readImage(ctx, tx, r.ImageId)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE images SET status='DELETED' WHERE image_id=?`, r.ImageId); err != nil {
			return err
		}
		i.Status = "DELETED"
		out.Image = i
		return nil
	})
}

// Cursor includes both sort fields: timestamps alone can tie. This is keyset
// pagination, so new inserts do not shift offsets between page requests.
type cursor struct {
	CreatedAt string `json:"created_at"`
	ID        string `json:"id"`
}

func (s *Server) ListUserImages(ctx context.Context, r *pb.ListUserImagesRequest) (*pb.ListUserImagesResponse, error) {
	limit := r.Limit
	if limit == 0 {
		limit = 20
	}
	if r.OwnerId == "" || limit < 1 || limit > 100 {
		return nil, status.Error(codes.InvalidArgument, "owner and limit 1..100 required")
	}
	query := `SELECT ` + imageColumns + ` FROM images WHERE owner_id=? AND status='READY'`
	args := []any{r.OwnerId}
	if r.Cursor != "" {
		var c cursor
		b, err := base64.RawURLEncoding.DecodeString(r.Cursor)
		if err != nil || json.Unmarshal(b, &c) != nil || c.ID == "" {
			return nil, status.Error(codes.InvalidArgument, "invalid cursor")
		}
		query += ` AND (created_at < ? OR (created_at = ? AND image_id < ?))`
		args = append(args, c.CreatedAt, c.CreatedAt, c.ID)
	}
	query += ` ORDER BY created_at DESC, image_id DESC LIMIT ?`
	args = append(args, limit+1)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, dbError(ctx, err)
	}
	defer rows.Close()
	out := &pb.ListUserImagesResponse{}
	for rows.Next() {
		i, err := scanImage(rows)
		if err != nil {
			return nil, dbError(ctx, err)
		}
		out.Items = append(out.Items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, dbError(ctx, err)
	}
	if len(out.Items) > int(limit) {
		out.Items = out.Items[:limit]
		last := out.Items[len(out.Items)-1]
		b, _ := json.Marshal(cursor{CreatedAt: last.CreatedAt, ID: last.ImageId})
		out.NextCursor = base64.RawURLEncoding.EncodeToString(b)
	}
	return out, nil
}
func (s *Server) CreateUser(ctx context.Context, r *pb.CreateUserRequest) (*pb.CreateUserResponse, error) {
	if strings.TrimSpace(r.Username) == "" || utf8.RuneCountInString(r.Username) > 32 {
		return nil, status.Error(codes.InvalidArgument, "username must be 1..32 characters")
	}
	out := &pb.CreateUserResponse{}
	return write(ctx, s, "CreateUser", r.IdempotencyKey, r, out, func(tx *sql.Tx) error {
		var exists int
		err := tx.QueryRowContext(ctx, `SELECT 1 FROM users WHERE username=?`, r.Username).Scan(&exists)
		if err == nil {
			return status.Error(codes.AlreadyExists, "username already exists")
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		out.User = &pb.User{UserId: id("usr-"), Username: r.Username, CreatedAt: now()}
		_, err = tx.ExecContext(ctx, `INSERT INTO users VALUES(?,?,?)`, out.User.UserId, out.User.Username, out.User.CreatedAt)
		return err
	})
}
func userExists(ctx context.Context, tx *sql.Tx, userID string) error {
	var exists int
	return tx.QueryRowContext(ctx, `SELECT 1 FROM users WHERE user_id=?`, userID).Scan(&exists)
}
func (s *Server) CreateComment(ctx context.Context, r *pb.CreateCommentRequest) (*pb.CreateCommentResponse, error) {
	if strings.TrimSpace(r.Text) == "" || utf8.RuneCountInString(r.Text) > 1000 {
		return nil, status.Error(codes.InvalidArgument, "comment must be 1..1000 characters")
	}
	out := &pb.CreateCommentResponse{}
	return write(ctx, s, "CreateComment", r.IdempotencyKey, r, out, func(tx *sql.Tx) error {
		if _, err := activeImage(ctx, tx, r.ImageId); err != nil {
			return err
		}
		if err := userExists(ctx, tx, r.UserId); err != nil {
			return err
		}
		out.Comment = &pb.Comment{CommentId: id("cmt-"), ImageId: r.ImageId, UserId: r.UserId, Text: r.Text, CreatedAt: now()}
		if _, err := tx.ExecContext(ctx, `INSERT INTO comments VALUES(?,?,?,?,?)`, out.Comment.CommentId, r.ImageId, r.UserId, r.Text, out.Comment.CreatedAt); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE images SET comments_count=comments_count+1 WHERE image_id=?`, r.ImageId)
		return err
	})
}
func (s *Server) LikeImage(ctx context.Context, r *pb.LikeImageRequest) (*pb.LikeImageResponse, error) {
	out := &pb.LikeImageResponse{}
	return write(ctx, s, "LikeImage", "", r, out, func(tx *sql.Tx) error {
		i, err := activeImage(ctx, tx, r.ImageId)
		if err != nil {
			return err
		}
		if err := userExists(ctx, tx, r.UserId); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO likes VALUES(?,?,?)`, r.ImageId, r.UserId, now())
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE images SET likes_count=likes_count+? WHERE image_id=?`, n, r.ImageId); err != nil {
			return err
		}
		out.LikesCount = i.LikesCount + n
		return nil
	})
}
func (s *Server) CreateShareLink(ctx context.Context, r *pb.CreateShareLinkRequest) (*pb.CreateShareLinkResponse, error) {
	seconds := r.ExpiresInSeconds
	if seconds == 0 {
		seconds = 3600
	}
	if seconds < 1 || seconds > 604800 {
		return nil, status.Error(codes.InvalidArgument, "share expiry must be 1..604800 seconds")
	}
	out := &pb.CreateShareLinkResponse{}
	return write(ctx, s, "CreateShareLink", r.IdempotencyKey, r, out, func(tx *sql.Tx) error {
		if _, err := activeImage(ctx, tx, r.ImageId); err != nil {
			return err
		}
		if err := userExists(ctx, tx, r.UserId); err != nil {
			return err
		}
		out.Token = id("")
		out.ExpiresAt = time.Now().Add(time.Duration(seconds) * time.Second).UTC().Format(time.RFC3339)
		if _, err := tx.ExecContext(ctx, `INSERT INTO share_links VALUES(?,?,?,?,?)`, out.Token, r.ImageId, r.UserId, out.ExpiresAt, now()); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE images SET shares_count=shares_count+1 WHERE image_id=?`, r.ImageId)
		return err
	})
}
