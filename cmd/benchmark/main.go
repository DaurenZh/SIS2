package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"time"

	"github.com/DaurenZh/SIS2/internal/client"
	"github.com/DaurenZh/SIS2/internal/imagev1"
	"github.com/DaurenZh/SIS2/internal/service"
	"github.com/DaurenZh/SIS2/internal/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type stats struct {
	values                      []time.Duration
	requestBytes, responseBytes int
}

func main() {
	svc := service.NewServer()
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	grpcServer := transport.NewGRPCServer(svc)
	go grpcServer.Serve(listener)
	defer grpcServer.Stop()
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	grpcClient := client.New(conn, time.Second, 3)
	rest := httptest.NewServer(transport.NewRESTHandler(svc))
	defer rest.Close()

	var grpcStats, restStats stats
	for i := 0; i < 1000; i++ {
		req := &imagev1.CreateImageRequest{IdempotencyKey: fmt.Sprintf("bench-%d", i), OwnerID: "owner-1", Caption: "benchmark image"}
		start := time.Now()
		response, err := grpcClient.CreateImage(context.Background(), req)
		if err != nil {
			panic(err)
		}
		grpcStats.values = append(grpcStats.values, time.Since(start))
		grpcStats.requestBytes += encodedSize(req)
		grpcStats.responseBytes += encodedSize(response)

		body, _ := json.Marshal(req)
		start = time.Now()
		httpResponse, err := http.Post(rest.URL+"/v1/images", "application/json", bytes.NewReader(body))
		if err != nil {
			panic(err)
		}
		var restResponse imagev1.CreateImageResponse
		json.NewDecoder(httpResponse.Body).Decode(&restResponse)
		httpResponse.Body.Close()
		restStats.values = append(restStats.values, time.Since(start))
		restStats.requestBytes += len(body)
		restStats.responseBytes += encodedSize(&restResponse)
	}
	fmt.Println("protocol, p50, p95, p99, avg_request_bytes, avg_response_bytes")
	printStats("gRPC", grpcStats)
	printStats("REST/JSON", restStats)
}

func encodedSize(v any) int { b, _ := json.Marshal(v); return len(b) }

func printStats(name string, s stats) {
	sort.Slice(s.values, func(i, j int) bool { return s.values[i] < s.values[j] })
	percentile := func(p float64) time.Duration { return s.values[int(float64(len(s.values)-1)*p)] }
	n := len(s.values)
	fmt.Printf("%s, %s, %s, %s, %.1f, %.1f\n", name, percentile(.50), percentile(.95), percentile(.99), float64(s.requestBytes)/float64(n), float64(s.responseBytes)/float64(n))
}
