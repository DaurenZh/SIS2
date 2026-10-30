.PHONY: test check run-server minio bench proto tools

test:
	go test -race ./...

check: test
	go vet ./...

run-server:
	go run ./cmd/server

minio:
	docker compose up -d

bench:
	./scripts/measure.sh

# Generated .pb.go files are committed: ordinary builds need no protoc.
# Install protoc separately, or use the project-local .tools/protoc/bin/protoc.
PROTOC ?= $(shell command -v protoc 2>/dev/null || echo .tools/protoc/bin/protoc)
proto: tools
	PATH="$(CURDIR)/.tools/bin:$$PATH" $(PROTOC) --go_out=. --go_opt=module=github.com/DaurenZh/SIS2 --go-grpc_out=. --go-grpc_opt=module=github.com/DaurenZh/SIS2 api/image/v1/image.proto

tools:
	mkdir -p .tools/bin
	GOBIN="$(CURDIR)/.tools/bin" go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6
	GOBIN="$(CURDIR)/.tools/bin" go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

.PHONY: monitoring-up monitoring-down load-rest load-grpc
monitoring-up:
	docker compose --profile monitoring up -d prometheus grafana

# Stop only monitoring; leave MinIO and stored data available.
monitoring-down:
	docker compose stop prometheus grafana

load-rest:
	docker compose run --rm -e PROTOCOL=rest k6

load-grpc:
	docker compose run --rm -e PROTOCOL=grpc k6
