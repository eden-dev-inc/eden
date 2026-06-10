# resp-wire Fuzz Testing

Fuzz tests for the RESP protocol parser using [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz).

## Requirements

- Nightly Rust toolchain
- cargo-fuzz: `cargo install cargo-fuzz`

## Fuzz Targets

| Target | Description | Memory |
|--------|-------------|--------|
| `fuzz_dynamic` | All RESP2/RESP3 types via Dynamic parser | 512MB |
| `fuzz_pipeline` | Pipeline parser for message sequences | 512MB |
| `fuzz_integer` | Integer parsing (overflow, signs, i128) | 512MB |
| `fuzz_double` | Double parsing (UTF-8, inf/nan, precision) | 512MB |
| `fuzz_bulk_string` | Bulk string parsing (lengths, null, binary) | 512MB |
| `fuzz_array` | Array parsing (nesting, recursion) | 1GB |

## Running

Quick run (60 seconds each):

```bash
cd wire-protocol/resp-wire/fuzz

cargo +nightly fuzz run fuzz_dynamic -- -rss_limit_mb=512 -max_total_time=60
cargo +nightly fuzz run fuzz_pipeline -- -rss_limit_mb=512 -max_total_time=60
cargo +nightly fuzz run fuzz_integer -- -rss_limit_mb=512 -max_total_time=60
cargo +nightly fuzz run fuzz_double -- -rss_limit_mb=512 -max_total_time=60
cargo +nightly fuzz run fuzz_bulk_string -- -rss_limit_mb=512 -max_total_time=60
cargo +nightly fuzz run fuzz_array -- -rss_limit_mb=1024 -max_total_time=60
```

Extended run (remove time limit):

```bash
cargo +nightly fuzz run fuzz_dynamic -- -rss_limit_mb=512
```

## Caveats

- **fuzz_array requires 1GB memory** due to deeply nested structure testing. With 512MB, you may see spurious OOM errors from fuzzer infrastructure rather than actual bugs.

- **Empty OOM artifacts** (hash `da39a3ee...`) indicate the fuzzer ran out of memory during corpus management, not while processing a specific input. Increase memory limit or ignore these.

- **Corpus growth**: The corpus directories grow over time. Clear them periodically for fresh runs: `rm -rf corpus/fuzz_*/`

## Reproducing Crashes

```bash
cargo +nightly fuzz run fuzz_dynamic artifacts/fuzz_dynamic/crash-XXXXX
```

## Minimizing Test Cases

```bash
cargo +nightly fuzz tmin fuzz_dynamic artifacts/fuzz_dynamic/crash-XXXXX
```
