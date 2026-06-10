# Benchmarks

This directory contains source-only benchmark tools. It intentionally does not
include deployment manifests, hosted benchmark results, host-specific scenarios,
or third-party proxy configurations.

## Tools

- `cacophony`: open-loop Redis load generator for gateway and backend testing.
- `ai-workload`: synthetic OpenAI-compatible backend and HTTP load generator for
  LLM and agent-shaped workloads.

Both tools are normal Cargo packages in the workspace:

```bash
cargo run --release -p cacophony -- --help
cargo run --release -p ai-workload -- --help
```

## Redis Workloads

`cacophony` runs a TOML scenario against a user-supplied Redis-compatible target.
Create the scenario outside the repository or in an ignored local scratch path:

```toml
[meta]
name = "local steady-state"

[keyspace]
size = 10000
prefix = "bench:"

[[phase]]
name = "warmup"
duration = "5s"
connections = 16
pipeline_depth = 8
commands = { get = 0.8, set = 0.2 }

[phase.arrival]
mode = "deterministic"
rate = 10000

[phase.payload]
mode = "fixed"
size = 256
```

Run it with:

```bash
cargo run --release -p cacophony -- \
  --scenario /path/to/scenario.toml \
  --target 127.0.0.1:6379
```

JSON results are written to stdout. Progress and diagnostics are written to
stderr.

## AI Workloads

Start the synthetic OpenAI-compatible backend:

```bash
cargo run --release -p ai-workload -- serve --listen 127.0.0.1:18181
```

Run a load phase against any compatible gateway or backend:

```bash
cargo run --release -p ai-workload -- load \
  --target http://127.0.0.1:18181/v1/chat/completions \
  --workload llm \
  --rate 100 \
  --duration 30s
```

Set `AI_WORKLOAD_BEARER` or pass `--bearer` when the target requires a bearer
token.

## Result Policy

Benchmark outputs are environment-specific. Store raw results, generated
reports, profiling captures, and local scenario files outside the repository
unless they are deliberately curated for publication.
