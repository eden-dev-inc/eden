# AI Gateway Architecture

## Purpose

The AI gateway is Eden's data-plane entry point for external LLM and agent
traffic. `eden_service` exposes Eden's product and control-plane APIs; it does
not own hot-path gateway enforcement.

## Boundaries

`eden_gateway` owns:

- Wire protocol parsing for LLM and agent traffic.
- Gateway caller authentication.
- Request identity extraction, including API key and agent identity.
- Hot-path policy enforcement.
- Model routing, price arbitrage, cache lookup, and upstream invocation.
- Response inspection, redaction, blocking, and cache storage.
- Per-key, per-org, and per-agent rate limiting and usage accounting.
- Fast and durable telemetry emission.

`eden_service` owns:

- CRUD APIs for gateway keys, policies, model registry, pricing settings, and
  agent gateway administration.
- Persistence for gateway configuration, exact response-cache entries, route
  rollups, and monthly usage rollups.
- Analytics query APIs, dashboards, and reporting.
- Back-office operations such as key rotation, policy rollout, and pricing
  snapshot review.

Shared core crates own:

- Stable key, policy, telemetry, pricing, cache, and agent identity shapes.
- Provider/model capability vocabulary.
- Traits for cache, rate-limit, budget, route-stats, and telemetry sinks.

## Runtime Pipeline

Every LLM request handled by `eden_gateway` should move through the same ordered
pipeline:

1. Parse the incoming wire protocol.
2. Authenticate the caller.
3. Resolve organization, endpoint, key, and policy context.
4. Extract agent identity when supplied.
5. Inspect request payload for shape, tools, prompt traits, and sensitive data.
6. Apply request policy.
7. Enforce rate limits and token budgets.
8. Check exact, semantic, and KV/prefix-cache opportunities.
9. Select a provider/model route.
10. Invoke the upstream model.
11. Inspect, redact, or block the response.
12. Store cache entries where policy allows.
13. Record token, cost, cache, route, and agent usage.
14. Emit canonical telemetry.

## Implementation Phases

1. Establish shared gateway primitives in `llm-core`.
2. Expose service-managed gateway key and policy APIs.
3. Hydrate gateway runtime configuration from service-managed state.
4. Enforce gateway authentication in the LLM data plane.
5. Move pricing, route optimization, and cache execution into gateway runtime.
6. Wire agent identity and per-agent admission into LLM requests.
7. Add agent data-plane transports for WebSocket, MCP, and A2A HTTP.
8. Expand protocol coverage beyond chat completions.
9. Complete dashboards and analytics for cost, latency, cache, PII, policy, and
   per-agent seat/fingerprint usage.

## Compatibility

New gateway API keys use the `eden-gateway-` prefix. The legacy `eden-proxy-`
prefix remains accepted during the compatibility window so existing service API
keys can be rotated safely.

Before service-managed policy hydration is complete, the gateway can bootstrap
auth from environment configuration:

- `EDEN_LLM_GATEWAY_AUTH=disabled|observe|enforce`
- `EDEN_LLM_GATEWAY_KEYS_SHA256=<comma-separated sha256 hex digests>`

This bootstrap path is intentionally shaped like the shared gateway policy so
isolated gateway runs can keep using env configuration.

## Control Plane Hydration

`eden_service` exposes a gateway control-plane snapshot from the LLM API key
state. The snapshot contains auth mode, hashed gateway keys, per-key
`LlmGatewayPolicy`, model catalog metadata, and route-stat rollups. The data
plane can consume the same shape from either:

- `EDEN_LLM_GATEWAY_CONTROL_PLANE_SNAPSHOT_JSON`
- `EDEN_LLM_GATEWAY_CONTROL_PLANE_SNAPSHOT_PATH`

The JSON env var is for isolated gateway runs and tests. The file path is the
deployment handoff path and is hot-reloaded on a short interval. If no snapshot
is configured, the gateway keeps using the existing `EDEN_LLM_GATEWAY_*` env
policy variables so standalone development remains simple.

Gateway keys, exact response-cache entries, route rollups, and monthly usage
rollups are durable service-managed state. `eden_service` persists gateway keys
in `llm_gateway_api_keys`, response-cache entries in
`llm_gateway_response_cache`, route observations in
`llm_gateway_route_rollups`, and token/cost accounting in
`llm_gateway_usage_rollups`. Startup hydration restores keys, route rollups,
and api-key budget windows before snapshots are published.

`eden_service` can publish the deployment handoff file directly when
`EDEN_LLM_GATEWAY_CONTROL_PLANE_SNAPSHOT_PUBLISH_PATH` is set. Operators should
mount that file into the standalone gateway and point
`EDEN_LLM_GATEWAY_CONTROL_PLANE_SNAPSHOT_PATH` at the same path. The publish
interval defaults to 5 seconds and can be changed with
`EDEN_LLM_GATEWAY_CONTROL_PLANE_SNAPSHOT_PUBLISH_SECS`.

## Route Decisions

The gateway emits a shared route decision for every provider-resolved LLM
request. The decision includes requested and selected provider/model, route
class, price source, estimated baseline and selected cost, estimated savings,
price-arbitrage mode, route-optimization mode, selected route stat sample
count, selected route latency/throughput/error-rate rollups, and the reason for
the choice.

Model rewrites are intentionally conservative: the data-plane gateway rewrites
the request model only when the upstream provider is OpenRouter and
`EDEN_LLM_GATEWAY_PRICE_ARBITRAGE` is enabled. Other providers still emit route
decision telemetry but keep the requested model unchanged until service-managed
route hydration provides safe multi-provider execution.

The data-plane gateway records rolling route observations after each chat
completion, keyed by endpoint, provider, model, and route class. The service
proxy persists those observations in `llm_gateway_route_rollups` and hydrates
them on startup. When `EDEN_LLM_GATEWAY_ROUTE_OPTIMIZATION` is `latency`,
`throughput`, or `balanced`, OpenRouter selection uses those observations when
available and falls back to cost selection when the process has not seen enough
route data. Regional, weighted, and canary routing are still policy-level next
slices on top of those durable rollups.

## Cache And Usage Accounting

The service proxy now uses a durable exact response cache for eligible
non-streaming requests. Entries store the response JSON, token counts, estimated
cost, request hash, expiry, and prompt fingerprint in
`llm_gateway_response_cache`. The prompt fingerprint is deliberately metadata:
it enables a semantic-cache lookup path without storing prompt text or forcing a
vector implementation into the first durable cache slice.

Monthly gateway usage is recorded in `llm_gateway_usage_rollups` for:

- organization
- API key
- downstream user when `x-eden-user-id` is supplied
- downstream agent when `x-eden-agent-id` is supplied
- downstream consumer when `x-eden-consumer-id` is supplied

Proxy-key token budgets are enforced today. Org, user, and agent budget policy
objects can be layered onto the same durable rollups in the next slice.

## Dashboard Surface

`GET /api/v1/llm/gateway/dashboard` returns an authenticated summary of:

- in-memory API key, cache, route, and budget state
- durable monthly usage rollups
- durable route rollups
- agent gateway connection, fingerprint, transport, and usage-window totals
- capability status labels that distinguish foundations from full
  customer-facing features

## Model Catalog

The LLM gateway exposes `GET /v1/models` as an OpenAI-compatible model list with
Eden metadata nested under each model's `eden` field. The built-in catalog is
derived from shared `llm-core` static pricing data and includes provider, region,
token pricing, context-window estimate, modalities, operation families, tool and
streaming support, fallback group, and lifecycle state.

This static catalog is the bootstrap path. The long-term control-plane path is
for `eden_service` to persist model catalog records and hydrate `eden_gateway`
with org-specific catalog visibility, pricing snapshots, lifecycle rollout
state, and regional availability.
