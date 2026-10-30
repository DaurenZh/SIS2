#!/bin/sh
# Runs separate server/client processes. No MinIO is needed for metadata tests.
set -eu
cd "$(dirname "$0")/.."
mkdir -p .tools/bin results
export GOMAXPROCS=2
unset MINIO_ENDPOINT
go build -o .tools/bin/ ./cmd/server ./cmd/benchmark
run_dir=$(mktemp -d "$PWD/.tools/measure.XXXXXX")
.tools/bin/server -grpc-addr 127.0.0.1:19090 -rest-addr 127.0.0.1:18080 -ready-file "$run_dir/ready.pid" -db "$run_dir/metadata.db" -objects "$run_dir/objects" > "$run_dir/server.log" 2>&1 &
server_pid=$!
trap 'kill "$server_pid" 2>/dev/null || true; wait "$server_pid" 2>/dev/null || true' EXIT INT TERM
# Readiness, not a fixed startup guess.
ready=0
for attempt in 1 2 3 4 5 6 7 8 9 10; do
  if ! kill -0 "$server_pid" 2>/dev/null; then
    cat "$run_dir/server.log" >&2
    echo 'Benchmark server exited before readiness.' >&2
    exit 1
  fi
  if [ -f "$run_dir/ready.pid" ] && [ "$(cat "$run_dir/ready.pid")" = "$server_pid" ] &&
     curl --max-time 1 --connect-timeout 1 -s -o /dev/null http://127.0.0.1:18080/v1/images/not-found &&
     kill -0 "$server_pid" 2>/dev/null; then
    ready=1
    break
  fi
  sleep 1
done
if [ "$ready" != 1 ]; then cat "$run_dir/server.log" >&2; exit 1; fi
{
  date -u '+UTC: %Y-%m-%dT%H:%M:%SZ'
  go version
  uname -sm
  sysctl -n machdep.cpu.brand_string 2>/dev/null || true
  echo 'GOMAXPROCS=2 for server and each client; cleartext loopback; no compression'
  echo '3 repetitions; 100 warmup operations; closed-loop; separate processes'
  echo 'Protocol order alternates in repetition 2; same server/database; isolated owners'
  echo '40 seeded READY images per isolated owner; lists return 20; SQLite WAL'
} > results/environment.txt
: > results/raw.csv
header=true
for run in 1 2 3; do
  protocols="grpc rest"
  if [ "$run" = 2 ]; then protocols="rest grpc"; fi
  for protocol in $protocols; do
    for caption in 80 1000; do
      for concurrency in 1 16; do
        # Edge-size trials focus on response size, with one client.
        if [ "$caption" = 1000 ] && [ "$concurrency" = 16 ]; then continue; fi
        for workload in get list create mix; do
          if [ "$caption" = 1000 ] && { [ "$workload" = create ] || [ "$workload" = mix ]; }; then continue; fi
          echo "run=$run protocol=$protocol workload=$workload c=$concurrency caption=$caption" >&2
          .tools/bin/benchmark -protocol "$protocol" -grpc-addr 127.0.0.1:19090 -rest-url http://127.0.0.1:18080 -workload "$workload" -concurrency "$concurrency" -caption "$caption" -n 2000 -warmup 100 -header="$header" >> results/raw.csv
          header=false
        done
      done
    done
  done
done
# Intentionally force every new Create to outlive its first 30 ms attempt.
# Replay has no delay; rows commit once, but each operation needs a retry.
for protocol in grpc rest; do
  .tools/bin/benchmark -protocol "$protocol" -grpc-addr 127.0.0.1:19090 -rest-url http://127.0.0.1:18080 -workload response-loss -timeout 30ms -concurrency 1 -n 100 -warmup 5 -header=false >> results/raw.csv
done
python3 scripts/summarize.py
