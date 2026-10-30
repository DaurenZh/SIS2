"""Medians across complete repetitions, not pooled latency samples."""
import csv
import statistics
from collections import defaultdict
from pathlib import Path

groups = defaultdict(list)
with Path("results/raw.csv").open() as source:
    for row in csv.DictReader(source):
        key = tuple(row[k] for k in ("protocol", "workload", "concurrency", "caption_chars"))
        groups[key].append(row)

columns = ["protocol", "workload", "concurrency", "caption_chars", "runs", "errors",
           "rps", "p50_ms", "p95_ms", "p99_ms", "body_request_bytes", "body_response_bytes",
           "wire_sent_bytes_per_op", "wire_received_bytes_per_op", "retries"]
with Path("results/summary.csv").open("w", newline="") as target:
    writer = csv.writer(target)
    writer.writerow(columns)
    for key, rows in sorted(groups.items()):
        medians = [f"{statistics.median(float(r[c]) for r in rows):.3f}" for c in columns[6:]]
        writer.writerow([*key, len(rows), sum(int(r["errors"]) for r in rows), *medians])

lines = ["# Measured results", "", "Each regular row is the median of three runs of 2,000 logical calls.",
         "Response-loss rows have one run of 100 calls. Times include retries and backoff.", "",
         "| Protocol | Workload | Clients | Caption | RPS | p50 ms | p95 ms | p99 ms | Response body B | Wire sent/received B per call | Retries |",
         "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
with Path("results/summary.csv").open() as source:
    for r in csv.DictReader(source):
        lines.append(f"| {r['protocol']} | {r['workload']} | {r['concurrency']} | {r['caption_chars']} | "
                     f"{r['rps']} | {r['p50_ms']} | {r['p95_ms']} | {r['p99_ms']} | {r['body_response_bytes']} | "
                     f"{r['wire_sent_bytes_per_op']}/{r['wire_received_bytes_per_op']} | {r['retries']} |")
lines.extend(["", "Environment:", "", "```text", Path("results/environment.txt").read_text().strip(), "```", "",
              "Raw samples: `raw.csv`; medians: `summary.csv`."])
Path("results/REPORT.md").write_text("\n".join(lines) + "\n")
print("Wrote results/summary.csv and results/REPORT.md")

# Keep the README's selected results synchronized when the experiment is rerun.
with Path("results/summary.csv").open() as source:
    rows = list(csv.DictReader(source))
selected = [("get", "1"), ("list", "1"), ("create", "1"),
            ("get", "16"), ("list", "16"), ("mix", "16")]
table = ["Measured on the machine in `results/environment.txt`; typical 80-character captions.", "",
         "| Workload | Clients | Protocol | p50 ms | p99 ms | RPS | Response body B |",
         "|---|---:|---|---:|---:|---:|---:|"]
for workload, clients in selected:
    for protocol in ("grpc", "rest"):
        r = next(r for r in rows if (r["protocol"], r["workload"], r["concurrency"], r["caption_chars"]) == (protocol, workload, clients, "80"))
        table.append(f"| {workload} | {clients} | {protocol} | {r['p50_ms']} | {r['p99_ms']} | {r['rps']} | {r['body_response_bytes']} |")

def result(protocol, workload, clients="1", caption="80"):
    return next(r for r in rows if (r["protocol"], r["workload"], r["concurrency"], r["caption_chars"]) == (protocol, workload, clients, caption))

gget, rget = result("grpc", "get"), result("rest", "get")
glist, rlist = result("grpc", "list"), result("rest", "list")
gmix, rmix = result("grpc", "mix", "16"), result("rest", "mix", "16")
gedge, redge = result("grpc", "list", caption="1000"), result("rest", "list", caption="1000")
saving = 100 * (1 - float(glist["body_response_bytes"]) / float(rlist["body_response_bytes"]))
edge_saving = 100 * (1 - float(gedge["body_response_bytes"]) / float(redge["body_response_bytes"]))
table.extend(["",
    f"In this run, single-client Get p50 was **{gget['p50_ms']} ms** for gRPC and **{rget['p50_ms']} ms** for REST.",
    f"The 20-image list p50 was **{glist['p50_ms']} ms** versus **{rlist['p50_ms']} ms**; the Protobuf response",
    f"body was about **{saving:.1f}% smaller**. At 16 clients the mixed workload completed",
    f"**{float(gmix['rps']):,.0f} RPS** with gRPC versus **{float(rmix['rps']):,.0f} RPS** with REST.",
    "These are observations, not universal protocol rankings. Single-client Create was close;",
    f"for 1,000-character captions the list-body saving shrank to **{edge_saving:.1f}%**,",
    "consistent with caption text dominating both encodings.", ""])
for protocol in ("grpc", "rest"):
    fault = result(protocol, "response-loss")
    original = groups[(protocol, "response-loss", "1", "80")][0]
    table.append(f"The {protocol} response-loss trial completed **{original['requests']} logical calls, {original['attempts']} attempts, {original['retries']} retries, {original['errors']} errors**; p50 was **{fault['p50_ms']} ms**.")
table.extend(["The deadline and backoff dominate recovery latency; idempotency protects the write's correctness.", ""])

start = "<!-- MEASURED_TABLE_START -->"
end = "<!-- MEASURED_TABLE_END -->"
readme = Path("README.md").read_text()
if start in readme and end in readme:
    before, rest = readme.split(start, 1)
    _, after = rest.split(end, 1)
    Path("README.md").write_text(before + start + "\n\n" + "\n".join(table) + "\n\n" + end + after)
