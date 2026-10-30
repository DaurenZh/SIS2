// benchmark is a bounded closed-loop load generator. Run the server separately
// to avoid competing with it inside one Go scheduler / process.
package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DaurenZh/SIS2/internal/client"
	pb "github.com/DaurenZh/SIS2/internal/imagev1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type counter struct{ sent, received atomic.Int64 }
type countedConn struct {
	net.Conn
	count *counter
}

func (c countedConn) Read(b []byte) (int, error) {
	n, e := c.Conn.Read(b)
	c.count.received.Add(int64(n))
	return n, e
}
func (c countedConn) Write(b []byte) (int, error) {
	n, e := c.Conn.Write(b)
	c.count.sent.Add(int64(n))
	return n, e
}
func (c *counter) dial(ctx context.Context, address string) (net.Conn, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	return countedConn{conn, c}, nil
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	protocol := flag.String("protocol", "grpc", "grpc or rest")
	address := flag.String("grpc-addr", "127.0.0.1:9090", "server gRPC address")
	base := flag.String("rest-url", "http://127.0.0.1:8080", "server REST URL")
	workload := flag.String("workload", "get", "get, list, create, mix or response-loss")
	n := flag.Int("n", 2000, "measured logical operations")
	concurrency := flag.Int("concurrency", 1, "concurrent closed-loop workers")
	warmup := flag.Int("warmup", 100, "warmup operations (not measured)")
	timeout := flag.Duration("timeout", 300*time.Millisecond, "timeout per attempt")
	attempts := flag.Int("attempts", 4, "maximum attempts per operation")
	captionSize := flag.Int("caption", 80, "caption characters: 80 typical or 1000 edge")
	header := flag.Bool("header", true, "print CSV header")
	flag.Parse()
	if *n < 1 || *concurrency < 1 || *captionSize < 0 || *captionSize > 1000 || *warmup < 0 {
		return fmt.Errorf("invalid benchmark configuration")
	}
	if *workload != "get" && *workload != "list" && *workload != "create" && *workload != "mix" && *workload != "response-loss" {
		return fmt.Errorf("invalid workload")
	}
	wire := &counter{}
	var api *client.Client
	if *protocol == "grpc" {
		conn, err := grpc.NewClient(*address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(wire.dial), grpc.WithDisableRetry())
		if err != nil {
			return err
		}
		defer conn.Close()
		api = client.New(conn, *timeout, *attempts)
	} else if *protocol == "rest" {
		tr := &http.Transport{MaxIdleConns: *concurrency + 4, MaxIdleConnsPerHost: *concurrency + 4, MaxConnsPerHost: *concurrency + 4, DisableCompression: true, ForceAttemptHTTP2: false, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) { return wire.dial(ctx, address) }}
		defer tr.CloseIdleConnections()
		api = client.NewREST(strings.TrimRight(*base, "/"), &http.Client{Transport: tr}, *timeout, *attempts)
	} else {
		return fmt.Errorf("protocol must be grpc or rest")
	}
	ctx := context.Background()
	owner := fmt.Sprintf("bench-%d", time.Now().UnixNano())
	caption := strings.Repeat("a", *captionSize)
	// Isolated owner and keys per run ensure that every timed Create is a fresh
	// INSERT, while every timed Get/List reads the same size seeded dataset.
	ids := make([]string, 40)
	for i := range ids {
		r, err := api.CreateImage(ctx, &pb.CreateImageRequest{IdempotencyKey: fmt.Sprintf("%s-seed-%d", owner, i), OwnerId: owner, Caption: caption, ObjectKey: "mock/benchmark.jpg"})
		if err != nil {
			return fmt.Errorf("seed: %w", err)
		}
		ids[i] = r.Image.ImageId
		if err = api.Call(ctx, "CompleteUpload", &pb.CompleteUploadRequest{ImageId: ids[i]}, &pb.GetImageResponse{}); err != nil {
			return err
		}
	}
	makeCall := func(index int) (string, proto.Message, proto.Message) {
		chosen := *workload
		if chosen == "mix" {
			switch {
			case index%100 < 90:
				chosen = "get"
			case index%100 < 95:
				chosen = "list"
			default:
				chosen = "create"
			}
		}
		switch chosen {
		case "get":
			return "GetImage", &pb.GetImageRequest{ImageId: ids[index%len(ids)]}, &pb.GetImageResponse{}
		case "list":
			return "ListUserImages", &pb.ListUserImagesRequest{OwnerId: owner, Limit: 20}, &pb.ListUserImagesResponse{}
		default:
			delay := int32(0)
			if chosen == "response-loss" {
				delay = int32((*timeout / time.Millisecond) * 3)
			}
			return "CreateImage", &pb.CreateImageRequest{IdempotencyKey: fmt.Sprintf("%s-create-%d", owner, index), OwnerId: owner, Caption: caption, ResponseDelayMs: delay}, &pb.CreateImageResponse{}
		}
	}
	// Warm every worker connection. Reset counters only after all calls finish.
	var wg sync.WaitGroup
	var warmErr error
	var warmMu sync.Mutex
	for worker := 0; worker < *concurrency; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := worker; i < *warmup; i += *concurrency {
				m, r, o := makeCall(i + *n)
				if err := api.Call(ctx, m, r, o); err != nil {
					warmMu.Lock()
					warmErr = err
					warmMu.Unlock()
				}
			}
		}(worker)
	}
	wg.Wait()
	if warmErr != nil {
		return fmt.Errorf("warmup: %w", warmErr)
	}
	wire.sent.Store(0)
	wire.received.Store(0)
	api.Attempts.Store(0)
	api.Retries.Store(0)
	api.RequestBodyBytes.Store(0)
	api.ResponseBodyBytes.Store(0)
	durations := make([]time.Duration, *n)
	var errors atomic.Int64
	var next atomic.Int64
	var firstErr error
	var errorMu sync.Mutex
	start := time.Now()
	for worker := 0; worker < *concurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= *n {
					return
				}
				method, req, out := makeCall(i)
				began := time.Now()
				err := api.Call(ctx, method, req, out)
				durations[i] = time.Since(began)
				if err != nil {
					errors.Add(1)
					errorMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errorMu.Unlock()
					continue
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	percentile := func(p float64) string {
		index := int(float64(len(durations))*p+0.999999) - 1
		if index < 0 {
			index = 0
		}
		return fmt.Sprintf("%.3f", float64(durations[index])/float64(time.Millisecond))
	}
	w := csv.NewWriter(os.Stdout)
	if *header {
		w.Write([]string{"protocol", "workload", "concurrency", "caption_chars", "requests", "errors", "attempts", "retries", "seconds", "rps", "p50_ms", "p95_ms", "p99_ms", "max_ms", "body_request_bytes", "body_response_bytes", "wire_sent_bytes_per_op", "wire_received_bytes_per_op"})
	}
	w.Write([]string{*protocol, *workload, strconv.Itoa(*concurrency), strconv.Itoa(*captionSize), strconv.Itoa(*n), fmt.Sprint(errors.Load()), fmt.Sprint(api.Attempts.Load()), fmt.Sprint(api.Retries.Load()), fmt.Sprintf("%.3f", elapsed.Seconds()), fmt.Sprintf("%.1f", float64(*n)/elapsed.Seconds()), percentile(.50), percentile(.95), percentile(.99), percentile(1), fmt.Sprintf("%.1f", float64(api.RequestBodyBytes.Load())/float64(*n)), fmt.Sprintf("%.1f", float64(api.ResponseBodyBytes.Load())/float64(*n)), fmt.Sprintf("%.1f", float64(wire.sent.Load())/float64(*n)), fmt.Sprintf("%.1f", float64(wire.received.Load())/float64(*n))})
	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}
	if firstErr != nil {
		return fmt.Errorf("%d operations failed; first: %w", errors.Load(), firstErr)
	}
	return nil
}
