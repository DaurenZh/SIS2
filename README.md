# SIS #2: gRPC protocol experiment

This repository contains a small, runnable Go implementation of the protocol
part of SIS #2. It deliberately does not implement the complete image
platform. The scope is:

- a versioned gRPC contract for creating and reading image metadata;
- idempotent writes using a client-supplied idempotency key;
- client-side per-attempt timeouts and bounded retries;
- a REST/JSON baseline and a benchmark that reports latency and payload size.

The server stores metadata in SQLite so it survives restarts without
provisioning a database cluster or object store. Tests use an in-memory SQLite
database. The `.proto` file is the wire-contract artifact; this prototype uses
a small JSON gRPC codec so it can be built without installing `protoc`.

## Run

```sh
go test ./...
go run ./cmd/server
go run ./cmd/benchmark
```

The benchmark starts both servers locally and prints p50/p95/p99 latency and
average request/response payload sizes. Results depend on the machine and
should be reported with the command, Go version, and workload.

## Retry and idempotency

`CreateImage` requires an idempotency key. The server records the first result
for that key and returns the same image on subsequent attempts. The client
uses a bounded exponential backoff and retries only transient gRPC status
codes. A request may commit on the server while its response is lost; retrying
the same key then returns the original result instead of creating a duplicate.

The test suite includes this response-loss scenario and deadline behavior.
