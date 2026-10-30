# Measured results

Each regular row is the median of three runs of 2,000 logical calls.
Response-loss rows have one run of 100 calls. Times include retries and backoff.

| Protocol | Workload | Clients | Caption | RPS | p50 ms | p95 ms | p99 ms | Response body B | Wire sent/received B per call | Retries |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| grpc | create | 1 | 80 | 7137.000 | 0.117 | 0.150 | 0.278 | 191.000 | 255.300/274.000 | 0.000 |
| grpc | create | 16 | 80 | 9716.000 | 1.055 | 5.110 | 7.394 | 191.000 | 254.700/273.000 | 0.000 |
| grpc | get | 1 | 1000 | 16195.300 | 0.059 | 0.078 | 0.132 | 1130.000 | 116.100/1213.000 | 0.000 |
| grpc | get | 1 | 80 | 16353.500 | 0.059 | 0.076 | 0.131 | 209.000 | 116.100/292.000 | 0.000 |
| grpc | get | 16 | 80 | 61838.800 | 0.209 | 0.609 | 0.930 | 209.000 | 81.400/257.400 | 0.000 |
| grpc | list | 1 | 1000 | 8809.800 | 0.105 | 0.185 | 0.227 | 22724.000 | 120.000/22816.000 | 0.000 |
| grpc | list | 1 | 80 | 10166.800 | 0.094 | 0.133 | 0.198 | 4304.000 | 107.000/4387.000 | 0.000 |
| grpc | list | 16 | 80 | 19352.200 | 0.604 | 2.231 | 3.300 | 4304.000 | 101.000/4381.300 | 0.000 |
| grpc | mix | 1 | 80 | 14306.200 | 0.060 | 0.134 | 0.171 | 412.900 | 122.700/495.900 | 0.000 |
| grpc | mix | 16 | 80 | 44687.200 | 0.250 | 1.008 | 1.645 | 412.900 | 91.200/464.500 | 0.000 |
| grpc | response-loss | 1 | 80 | 23.700 | 42.364 | 51.458 | 52.663 | 191.000 | 483.400/316.100 | 100.000 |
| rest | create | 1 | 80 | 7389.600 | 0.114 | 0.148 | 0.262 | 262.000 | 408.900/376.000 | 0.000 |
| rest | create | 16 | 80 | 9689.400 | 1.040 | 4.914 | 7.694 | 262.000 | 408.900/376.000 | 0.000 |
| rest | get | 1 | 1000 | 17294.900 | 0.055 | 0.070 | 0.124 | 1215.000 | 178.000/1325.000 | 0.000 |
| rest | get | 1 | 80 | 17867.100 | 0.054 | 0.068 | 0.109 | 295.000 | 178.000/404.000 | 0.000 |
| rest | get | 16 | 80 | 56219.500 | 0.210 | 0.757 | 1.136 | 295.000 | 178.000/404.000 | 0.000 |
| rest | list | 1 | 1000 | 5764.900 | 0.161 | 0.239 | 0.289 | 24290.000 | 190.000/24419.000 | 0.000 |
| rest | list | 1 | 80 | 7726.900 | 0.122 | 0.184 | 0.227 | 5890.000 | 190.000/6019.000 | 0.000 |
| rest | list | 16 | 80 | 15078.900 | 0.783 | 2.818 | 4.201 | 5890.000 | 190.000/6019.000 | 0.000 |
| rest | mix | 1 | 80 | 15288.100 | 0.054 | 0.144 | 0.180 | 573.100 | 190.100/683.400 | 0.000 |
| rest | mix | 16 | 80 | 38226.500 | 0.273 | 1.205 | 1.928 | 573.100 | 190.100/683.400 | 0.000 |
| rest | response-loss | 1 | 80 | 23.800 | 42.001 | 51.994 | 53.021 | 265.800 | 855.600/389.400 | 100.000 |

Environment:

```text
UTC: 2026-10-30T17:54:16Z
go version go1.27.1 darwin/arm64
Darwin arm64
Apple M5 Pro
GOMAXPROCS=2 for server and each client; cleartext loopback; no compression
3 repetitions; 100 warmup operations; closed-loop; separate processes
Protocol order alternates in repetition 2; same server/database; isolated owners
40 seeded READY images per isolated owner; lists return 20; SQLite WAL
```

Raw samples: `raw.csv`; medians: `summary.csv`.
