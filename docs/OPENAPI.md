# OpenAPI

Eden exposes generated OpenAPI documentation from the service binary when the
`openapi` feature is enabled.

## Routes

- `GET /api-docs/openapi.json` serves the OpenAPI specification.
- `GET /swagger-ui/` serves Swagger UI for browsing the same specification.

The generated routes are mounted by `eden-service`, and route paths are prefixed
with `/api/v1` where appropriate by the OpenAPI modifier in
[`eden_service/src/apidocs.rs`](../eden_service/src/apidocs.rs).

## Build

Check the OpenAPI feature set:

```bash
cargo check -p eden-service --no-default-features --features openapi --locked
```

Run a service binary with the generated docs enabled:

```bash
cargo run -p eden-service --no-default-features --features openapi
```

The standard server runtime feature bundle also enables OpenAPI:

```bash
cargo run -p eden-service --features server-runtime
```

## Feature Scope

The OpenAPI feature enables the full endpoint schema set and LLM API docs so the
published specification is complete for the public service surface. Endpoint
request schemas are generated through the `endpoint-openapi` crate.

Hand-written API docs should explain workflows and examples. The generated
OpenAPI document should be treated as the route and schema reference.
