# Feature Flags

Eden uses Cargo features to keep focused builds small while still supporting a
full service binary. This page summarizes the public feature surfaces; the
`Cargo.toml` files remain authoritative.

## Production Status

Redis is the only production-ready gateway protocol in this release. The other
gateway and endpoint features are published for development, evaluation, API
review, and contribution until they are explicitly promoted.

## Service Bundles

These features live on `eden-service`.

| Feature | Purpose |
|---|---|
| `default` | Enables API usage tracking and telemetry exporters for normal service builds. |
| `server-runtime` | Full service bundle with all endpoints, LLM support, OpenAPI, ClickHouse polling, embedded dashboard assets, and telemetry export. |
| `openapi` | Enables generated OpenAPI docs and Swagger UI. This intentionally enables the full endpoint schema set and LLM API docs. |
| `embedded-db` | Builds the service against the local embedded database path instead of the external database runtime. |
| `embedded-dashboard` | Serves prebuilt dashboard assets from the service binary. |
| `telemetry-export` | Enables telemetry exporters and tracing integration. |
| `api-usage-tracking` | Enables per-request usage accounting hooks. |
| `poll-clickhouse` | Enables endpoint metadata polling export through the ClickHouse push path. |
| `infra-tests` | Enables Redis and Postgres support for infrastructure-oriented tests. |
| `test-utils` | Enables test helper dependencies. |

`rate-limiting` and `stream` are reserved gates for code paths that are already
shaped for feature control.

## Gateway Features

Protocol gateway features can be enabled independently. These are the preferred
build flags when working on one hot path.

| Feature | Enables | Status |
|---|---|---|
| `redis` | Redis endpoint/runtime code and the Redis gateway. | Production-ready |
| `postgres` | Postgres endpoint/runtime code and the Postgres gateway. | Development/evaluation |
| `mongo` | Mongo endpoint/runtime code and the Mongo gateway. | Development/evaluation |
| `llm` | LLM endpoint/runtime code, LLM gateway, and agent gateway support. | Development/evaluation |

Focused examples:

```bash
cargo check -p eden-service --no-default-features --features redis
cargo check -p eden-service --no-default-features --features postgres
cargo check -p eden-service --no-default-features --features mongo
cargo check -p eden-service --no-default-features --features llm
```

The `openapi` feature is intentionally broad. Use it when validating the public
API reference, not when measuring a minimal protocol build.

## Endpoint Features

Endpoint features propagate through `eden-service`, `ep-runtime`, `endpoints`,
`endpoint-core`, `endpoint-schema`, `database`, and optionally
`endpoint-openapi`.

| Feature | Endpoint area |
|---|---|
| `aws` | AWS endpoint family. Lower layers also enable related AWS-adjacent endpoint support where required. |
| `azure` | Azure APIs. |
| `cassandra` | Cassandra. |
| `clickhouse` | ClickHouse. |
| `databricks` | Databricks. |
| `datadog` | Datadog. |
| `elasticache` | ElastiCache, layered on Redis support. |
| `eraser` | Eraser endpoint support. |
| `function` | Function endpoint support. |
| `gitlab` | GitLab. |
| `gworkspace` | Google Workspace. |
| `http` | Generic HTTP endpoint support. |
| `llm` | LLM endpoint and gateway support. |
| `mongo` | MongoDB. |
| `mssql` | Microsoft SQL Server. |
| `mysql` | MySQL. |
| `oracle` | Oracle. |
| `pinecone` | Pinecone. |
| `postgres` | PostgreSQL. |
| `posthog` | PostHog. |
| `rds` | RDS, layered on Postgres support. |
| `redis` | Redis-compatible endpoints. |
| `s3` | S3. |
| `salesforce` | Salesforce. |
| `snowflake` | Snowflake. |
| `tavily` | Tavily. |
| `weaviate` | Weaviate. |

`all-endpoints` enables the complete endpoint set. Use it for broad integration
checks and generated API docs; avoid it for focused edit/compile loops.

## Common Build Shapes

Minimal Redis service check:

```bash
cargo check -p eden-service --no-default-features --features redis --locked
```

Redis service with embedded database support:

```bash
cargo check -p eden-service --no-default-features --features "redis embedded-db" --locked
```

Generated OpenAPI reference:

```bash
cargo check -p eden-service --no-default-features --features openapi --locked
```

Full server runtime:

```bash
cargo check -p eden-service --features server-runtime --locked
```

## Documentation Map

- API routes and schemas: [OpenAPI](OPENAPI.md)
- API workflows and request examples: [API documentation](api_docs.md)
- Telemetry names and labels: [Metrics](METRICS.md)
- Analytics data flow: [Analytics architecture](ANALYTICS.md)
- AI gateway behavior: [AI gateway architecture](AI_GATEWAY_ARCHITECTURE.md)
