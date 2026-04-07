# Stonebreaker Benchmark

`stonebreaker` is a Stonebraker-style benchmark run for ADAM.

It uses exactly 5 live sources:
- `postgres`
- `mongodb`
- `redis`
- `clickhouse`
- `weaviate`

`weaviate` carries both:
- retail review embeddings
- a compact benchmark document corpus sourced from `G4KMU/t2-ragbench` (`ConvFinQA`, `turn_0` by default)

`localfs` can also be enabled as an auxiliary system for the same raw benchmark
documents. It is not part of the five-source benchmark universe and does not
change the task manifest or distractor counts.

Every benchmark task is designed so:
- exactly 2 sources are required
- the other 3 sources are distractors
- the join key is explicit, usually `brand_id`, `user_id`, or `product_id`

The machine-readable task list lives at [`benchmarks/stonebreaker.tasks.json`](./benchmarks/stonebreaker.tasks.json).

## Running

```bash
make up VERTICAL=stonebreaker
make app VERTICAL=stonebreaker
```

To also register the raw document corpus as a `localfs` endpoint, point
`STONEBREAKER_LOCALFS_ROOT` at `init/data/stonebreaker/localfs` as seen by your
Eden process before starting the app.

This run reuses the retail data plane, but unlike the default retail demo it enables all five sources by default so the benchmark source universe stays fixed.

## Current Scope

The first cut ships:
- a dedicated `stonebreaker` vertical
- a five-source compose stack
- benchmark-style two-source evidence query pairs
- a document corpus inside Weaviate for document retrieval experiments
- an optional localfs view of the raw benchmark documents
- a manifest describing the source requirements and distractors for each task

It does not yet ship manually verified gold answers or automatic source-selection scoring. The manifest is structured so those can be added on top of the run without changing the connector layout.
