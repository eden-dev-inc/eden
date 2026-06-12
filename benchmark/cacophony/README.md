# Cacophony

Open-loop Redis load generator for gateway and backend testing.

Unlike closed-loop tools, Cacophony schedules commands at controlled arrival
rates independent of response times. That makes it useful for observing queue
buildup, latency inflection, and overload behavior that closed-loop clients can
hide by slowing down with the system under test.

Cacophony precomputes each phase's RESP requests before the timed section
starts. Writer tasks replay those in-memory buffers directly, so request
formatting does not compete with the gateway during measurement.

## Usage

```bash
cargo run --release -p cacophony -- \
  --scenario /path/to/scenario.toml \
  --target 127.0.0.1:6379
```

JSON results go to stdout. Progress and diagnostics go to stderr.

## Architecture

```text
Scheduler task           Shared channel           Worker tasks
one per phase      ->    bounded queue      ->    one per connection
```

The scheduler precomputes RESP bytes and emits arrivals at the configured rate.
The bounded channel records shed load when the target cannot keep up. Workers
dispatch prebuilt requests through Redis connections and record wire bytes,
payload bytes, response status, and latency.

## Latency Model

| Metric | Measures | Start | End |
| --- | --- | --- | --- |
| Service | Proxy or server time | socket write | response read |
| Sojourn | End-to-end user time | scheduled arrival | response read |
| Queue delay | Wait for a connection slot | scheduled arrival | socket write |

Success and error latencies are tracked separately so fast errors do not make an
overloaded service look healthier than it is.

## Error Classification

| Category | Meaning |
| --- | --- |
| `shed` | Cacophony dropped the command before dispatch because the queue was full |
| `redis_errors` | Target returned a valid RESP error |
| `connection_errors` | TCP-level failure, EOF, broken pipe, or write error |
| `integrity_failures` | GET returned an unexpected value after a unique planned SET |
| `integrity_race_suspects` | GET mismatch where multiple SETs make ordering ambiguous |

## Scenario Shape

Scenarios are TOML files with one or more phases. Each phase controls
connections, pipeline depth, arrival rate, command mix, and payload size.

```toml
[meta]
name = "local steady-state"

[keyspace]
size = 10000
prefix = "bench:"

[[phase]]
name = "steady"
duration = "30s"
connections = 32
pipeline_depth = 8
commands = { get = 0.8, set = 0.2 }

[phase.arrival]
mode = "deterministic"
rate = 20000

[phase.payload]
mode = "fixed"
size = 256
```
