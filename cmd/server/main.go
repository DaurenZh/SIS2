package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/DaurenZh/SIS2/internal/observability"
	"github.com/DaurenZh/SIS2/internal/service"
	"github.com/DaurenZh/SIS2/internal/storage"
	"github.com/DaurenZh/SIS2/internal/transport"
)

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
func run() error {
	grpcAddr := flag.String("grpc-addr", ":9090", "gRPC address")
	restAddr := flag.String("rest-addr", ":8080", "REST address")
	metricsAddr := flag.String("metrics-addr", "", "optional Prometheus HTTP address, e.g. :9095 (disabled by default)")
	readyFile := flag.String("ready-file", "", "optional PID file written after all listeners bind")
	dbPath := flag.String("db", "sis2.db", "SQLite file")
	objectRoot := flag.String("objects", "objects", "local object directory if MINIO_ENDPOINT unset")
	faultMethod := flag.String("fault-method", "CreateImage", "operation to inject faults into")
	before := flag.Int64("fail-before", 0, "first N matching calls fail before execution")
	after := flag.Int64("fail-after", 0, "next N matching calls lose their response AFTER commit")
	delay := flag.Duration("delay", 0, "cancellable delay before each matching call")
	flag.Parse()
	svc, err := service.NewPersistentServer(*dbPath)
	if err != nil {
		return err
	}
	defer svc.Close()
	objects, err := configuredObjectStore(*objectRoot)
	if err != nil {
		return err
	}
	f := &transport.Faults{Method: *faultMethod, Delay: *delay, FailBefore: *before, FailAfter: *after}
	listener, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		return err
	}
	defer listener.Close()
	// Bind every port before declaring startup successful. A busy REST/metrics
	// port must fail the whole process rather than leave a partial deployment.
	restListener, err := net.Listen("tcp", *restAddr)
	if err != nil {
		return fmt.Errorf("bind REST: %w", err)
	}
	defer restListener.Close()
	var metricsListener net.Listener
	if *metricsAddr != "" {
		metricsListener, err = net.Listen("tcp", *metricsAddr)
		if err != nil {
			return fmt.Errorf("bind metrics: %w", err)
		}
		defer metricsListener.Close()
	}
	var metrics *observability.Metrics
	var metricsServer *http.Server
	var restHandler http.Handler = transport.NewRESTHandler(svc, objects, f)
	if *metricsAddr != "" {
		metrics = observability.New()
		restHandler = metrics.HTTP(restHandler)
		mux := http.NewServeMux()
		mux.Handle("GET /metrics", metrics.Handler())
		metricsServer = transport.NewHTTPServer(*metricsAddr, mux)
	}
	gs := transport.NewGRPCServerWithMetrics(svc, f, metrics)
	hs := transport.NewHTTPServer(*restAddr, restHandler)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errors := make(chan error, 3)
	if metricsServer != nil {
		go func() {
			log.Printf("Prometheus metrics %s/metrics", *metricsAddr)
			errors <- metricsServer.Serve(metricsListener)
		}()
	}
	go func() { log.Printf("gRPC %s (Protobuf)", *grpcAddr); errors <- gs.Serve(listener) }()
	go func() { log.Printf("REST %s (JSON)", *restAddr); errors <- hs.Serve(restListener) }()
	go func() {
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := svc.PruneIdempotency(ctx); err != nil {
					log.Printf("idempotency cleanup: %v", err)
				}
			}
		}
	}()
	var serveErr error
	if *readyFile != "" {
		// The benchmark uses a fresh directory and checks this exact child PID.
		// A different process serving the same URL cannot satisfy this handshake.
		if err := os.WriteFile(*readyFile, []byte(strconv.Itoa(os.Getpid())+"\n"), 0600); err != nil {
			serveErr = fmt.Errorf("write readiness: %w", err)
		} else {
			defer os.Remove(*readyFile)
		}
	}
	if serveErr == nil {
		select {
		case <-ctx.Done():
		case err := <-errors:
			serveErr = fmt.Errorf("listener stopped unexpectedly: %v", err)
		}
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if metricsServer != nil {
		metricsServer.Shutdown(shutdown)
	}
	hs.Shutdown(shutdown)
	done := make(chan struct{})
	go func() { gs.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-shutdown.Done():
		gs.Stop()
	}
	return serveErr
}
func configuredObjectStore(root string) (storage.Store, error) {
	if endpoint := os.Getenv("MINIO_ENDPOINT"); endpoint != "" {
		ssl, err := strconv.ParseBool(env("MINIO_USE_SSL", "false"))
		if err != nil {
			return nil, err
		}
		log.Printf("object storage: MinIO at %s, bucket %s", endpoint, os.Getenv("MINIO_BUCKET"))
		objects, err := storage.NewMinIO(endpoint, os.Getenv("MINIO_ACCESS_KEY"), os.Getenv("MINIO_SECRET_KEY"), os.Getenv("MINIO_BUCKET"), ssl)
		if err != nil {
			return nil, err
		}
		if os.Getenv("MINIO_CREATE_BUCKET") == "true" {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := objects.EnsureBucket(ctx); err != nil {
				return nil, err
			}
		}
		return objects, nil
	}
	log.Printf("object storage: local directory %s", root)
	return storage.NewLocal(root, env("PUBLIC_BASE_URL", "http://localhost:8080"))
}
func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
