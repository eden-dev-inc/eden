# Benchmarks

This page summarizes curated benchmark results for Eden's Eve gateway runtime.
Raw benchmark artifacts are environment-specific and are intentionally not
committed. Treat these numbers as a public performance brief from controlled
internal runs, not as a universal hardware guarantee.

Redis is the only production-ready gateway protocol in this release. Results for
AI/LLM, agent, Postgres, Mongo, and other protocol surfaces should be read as
development and evaluation data until those surfaces are explicitly promoted.

## Summary

Eve is Eden's high-performance data plane. It proxies Redis traffic and
OpenAI-compatible AI workloads while preserving layer-7 awareness for policy,
analysis, routing, mirroring, migration, and telemetry.

The key result from the current benchmark set: Eve is competitive with Envoy on
raw gateway throughput while doing substantially more product work inline. On a
matched Redis workload, Eve used about one-third less CPU per completed request
than Envoy and produced lower tail latency.

| Claim | Proof Point | Baseline |
|---|---:|---|
| Lower CPU cost per request | 10.83 vs 15.95 CPU-seconds per million Redis requests at matched 400k req/s | Envoy Redis proxy |
| Lower Redis tail latency | p99 2.10 ms vs 2.37 ms; p99.9 3.25 ms vs 3.67 ms | Envoy Redis proxy |
| Clean overload behavior | Zero connection errors across the 64 KiB max-throughput vCPU sweep | Envoy Redis proxy |
| AI gateway ceiling | Buffered 64 KiB AI responses reached 12k req/s / 6.36 Gb/s with zero errors | Envoy HTTP proxy |
| Multi-agent overhead | 2k offered req/s with p99 1.38 ms and zero errors | Synthetic OpenAI-compatible backend |
| Redis large-payload throughput | Up to 15.5 Gb/s wire / 1.94 GB/s payload goodput | Envoy Redis proxy |
| Live observability | 185-metric catalog, gateway timing breakdowns, traces, logs, and ClickHouse/DuckDB export | Proxy counters |

## Redis Gateway

### Matched Throughput

At a controlled 400k req/s mixed Redis load where both gateways completed the
same work without shedding, Eve matched Envoy throughput with lower latency and
lower CPU use.

| Target | Completed req/s | Shed | p99 | p99.9 |
|---|---:|---:|---:|---:|
| Direct Redis | ~305,921 | 1,881,303 | 5.72 ms | - |
| Envoy Redis | ~399,901 | 0 | 2.37 ms | 3.67 ms |
| Eve Redis Gateway | ~399,908 | 0 | 2.10 ms | 3.25 ms |

CPU attribution from the sampled run:

| Target | CPU-s / million req | Cycles / req | Instructions / req | DSO Split |
|---|---:|---:|---:|---|
| Envoy Redis | 15.95 | ~53.7k | ~33.4k | ~53% kernel / ~38% envoy |
| Eve Redis Gateway | 10.83 | ~35.2k | ~23.9k | ~72% kernel / ~18% eden-service |

Direct Redis was saturated at this offered load, so it is best read as a
saturation signal rather than an added-latency baseline.

### Overload Behavior

In a 400k offered open-loop Redis load, Eve sustained materially more completed
throughput than Envoy while avoiding shed.

| Target | Completed req/s | Shed | p99 |
|---|---:|---:|---:|
| Direct Redis | 287k | 2.02M | 5.99 ms |
| Envoy Redis | 165k | 4.57M | 7.27 ms |
| Eve Redis Gateway | 384k | 0 | 2.45 ms |

With telemetry export and capture-all request analysis enabled, Eve still held
384k req/s at zero shed with p99 3.93 ms on this workload.

### Large Payload Throughput

On 64 KiB Redis GET workloads, Eve was competitive with Envoy on wire
throughput and showed cleaner error behavior at saturation.

| Topology | Eve Peak | Envoy Peak | Notes |
|---|---:|---:|---|
| Single backend, 4 vCPU | 14.0 Gb/s | 13.9 Gb/s | Eve had zero connection errors across the sweep |
| Single backend, host network, 4 vCPU | 14.4 Gb/s | 13.6 Gb/s | Eve stayed error-free through peak |
| Four backends, ring hash, 4 vCPU | 15.5 Gb/s | 16.1 Gb/s | Eve was within ~3.5% on wire, with 0 errors vs Envoy's 11 |
| Single backend, 6-8 vCPU | ~15.0 Gb/s | ~16.0 Gb/s | Eve recovered most of the gap with additional cores |

The ceiling in these runs was the backend, host, and TCP path rather than only
gateway worker count.

## AI And LLM Gateway

Eve exposes an OpenAI-compatible AI gateway for chat, multi-agent/tool, and
streaming workloads. These runs used a synthetic OpenAI-compatible backend and a
generic Envoy HTTP proxy.

### Sustained Load

| Workload | Eve Clean Ceiling | Envoy Behavior |
|---|---:|---|
| Buffered 64 KiB responses | 12k req/s, 6.36 Gb/s, p99 1.68 ms, 0 errors | Collapsed at 8k offered: 2,530 req/s and 42,779 errors |
| Streaming 64 KiB responses | 6k req/s, 3.75 Gb/s, p99 1.64 ms, 0 errors | Collapsed at 6k offered: 2,421 req/s and 24,137 errors |

### Latency

| Workload | Eve Req/s | Errors | p50 | p99 | p99.9 |
|---|---:|---:|---:|---:|---:|
| Multi-agent, 2k offered, 8 tool schemas | 1,998.8 | 0 | 0.60 ms | 1.38 ms | 1.69 ms |
| Streaming, 1k offered | 999.8 | 0 | 0.72 ms | 0.94 ms | 0.99 ms |
| LLM chat, 5k offered | 4,992.1 | 9 | 13.9 ms | 47.5 ms | 52.0 ms |

For small-response LLM traffic, recent fast-path work allowed Eve to hold a full
12k req/s at about 1.2 ms p99 with zero shedding.

## Observability Validation

Eve telemetry was validated end-to-end under live Redis load. In a 30-minute
window with a 100k-request Redis workload running at 175,472 req/s, the runtime
produced:

- 185 metrics across core, gateway, LLM, IAM, endpoint, workload, migration,
  tool-safety, and analytics groups.
- 14,043 gateway metric rows stored in ClickHouse, including request and command
  counts, per-command duration, endpoint-vs-overhead timing, parse
  decode/materialize breakdown, bridge queue/write timing, byte counters, and
  mirror latency/divergence.
- 2,105 trace spans captured and queryable alongside structured logs.
- Dashboard series coverage from emission through storage, query, and UI.

This matters because Eve's performance profile includes built-in protocol-aware
observability rather than only generic proxy counters.

## Capability Context

The raw numbers should be read alongside the feature set being exercised. Redis
is one measured protocol in this report, but the product surface is broader:
Eden is a data and AI gateway for databases, model providers, and agent
workloads.

| Capability | Eden / Eve Gateway | Envoy / Envoy-Based Gateway | HAProxy / TCP Baseline |
|---|---|---|---|
| Protocol awareness | Layer-7 protocol handling across gateway families: Redis/RESP, Postgres, Mongo, HTTP/OpenAI-compatible AI, and endpoint schemas | Strong HTTP/L7 proxying, with narrower protocol-specific database support depending on filters/extensions | Byte forwarding unless custom protocol logic is added outside the proxy |
| Data gateway behavior | Request routing, endpoint-aware dispatch, database policy hooks, interlays, mirroring, migration routing, and consistency checks | Generic routing/load balancing plus protocol-specific features where available | Generic server selection and health checks |
| AI gateway behavior | OpenAI-compatible chat, streaming, agent/tool workloads, request normalization, routing hooks, and usage accounting | Usually delivered through Envoy-based AI gateway products or custom filters | Not applicable at L7 |
| Per-request analysis | Command/query/request classification, audit events, analytics, prompt/tool-shape analysis, and policy decisions | Mostly proxy/filter counters and access logs unless extended by custom services | Connection/request logs only |
| Content security | PII detection, redaction/blocking hooks, prompt security inspection, and policy enforcement paths | Requires separate filters, external authorization, or companion services | Not available without application-side tooling |
| Auth and policy | Control-plane JWT/RBAC/ELS plus upstream protocol credentials such as Redis ACLs and database/user credentials | Proxy auth, external auth, and protocol auth where configured | Generic TCP/TLS/userlist controls |
| Mirroring and migration | Read/write toggles, sampling, in-flight limits, divergence metrics, ratio routing, user-hash routing, fallback-on-miss, and version comparison paths | Mirroring/routing support varies by protocol and deployment model | Not available beyond generic fanout patterns |
| Observability | Built-in metrics, logs, traces, per-command/per-request timing, parse breakdowns, pool/lane/mirror/bridge metrics, and ClickHouse/DuckDB export | Admin counters, histograms, access logs, and tracing integrations | Stats/Prometheus when configured |

## Reproducing Workloads

Source benchmark tools live in [`benchmark/`](../benchmark/):

- `cacophony` runs open-loop Redis workloads against a Redis-compatible target.
- `ai-workload` runs synthetic OpenAI-compatible HTTP workloads.

Keep raw result files, profiler captures, and host-specific scenario files
outside the repository unless they are deliberately curated for publication.
