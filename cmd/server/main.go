package main

import (
	"log"
	"net"
	"net/http"

	"github.com/DaurenZh/SIS2/internal/service"
	"github.com/DaurenZh/SIS2/internal/transport"
)

func main() {
	svc, err := service.NewPersistentServer("sis2.db")
	if err != nil {
		log.Fatal(err)
	}
	defer svc.Close()
	grpcListener, err := net.Listen("tcp", ":9090")
	if err != nil {
		log.Fatal(err)
	}
	grpcServer := transport.NewGRPCServer(svc)
	go func() {
		log.Println("gRPC listening on :9090")
		if err := grpcServer.Serve(grpcListener); err != nil {
			log.Fatal(err)
		}
	}()
	log.Println("REST listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", transport.NewRESTHandler(svc)))
}
