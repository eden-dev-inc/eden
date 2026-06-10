# Endpoint-Level Security (ELS): Design Document

**Last updated:** 2026-03-27

## 1. Overview

Eden is a unified API layer that proxies requests to databases, cloud
providers, and web services on behalf of users and AI agents. Endpoint-Level
Security (ELS) adds **per-user credential isolation** to every proxied
endpoint, so that each user or machine identity authenticates to target systems
with its own credentials, without those credentials ever entering an AI
model's context window.

## 2. Problem Statement

Without ELS, every user hitting an endpoint shares the same connection
credentials. This is insufficient for organizations that need:

- **Per-user credential isolation**: distinct database users, IAM roles, or
  API keys for each human or machine identity accessing an endpoint.
- **Session-variable injection**: PostgreSQL `SET role`, MySQL `SET @user_id`,
  ClickHouse `SET` settings, or Snowflake `ALTER SESSION SET` to activate
  target-side row-level controls such as PostgreSQL RLS or ClickHouse row
  policies.
- **Credential lifecycle management**: drafting, promoting, rolling back, and
  auditing credential configurations without downtime.
- **Encryption at rest**: stored and cached credentials must never be
  plaintext on disk or in memory caches.

ELS addresses all four concerns. The feature was originally named "RLS"
(Row-Level Security) but was renamed because its scope extends beyond row
filtering to any per-user authentication context at the endpoint level.

## 3. Design Goals

| Goal | Mechanism |
|------|-----------|
| Credentials never reach the LLM context | Service-layer injection at request time |
| Per-user auth for all 22 endpoint types | Typed auth trait with per-strategy implementations |
| Zero-downtime credential rotation | Version lifecycle with optimistic-lock promotion |
| Defense in depth for stored secrets | Envelope encryption (org key > DEK > config) |
| API responses never leak secrets | Redacted response types omit credential fields at compile time |
| Revoked access stays revoked | RBAC tombstone tables prevent grant resurrection |

## 4. Architecture Overview

```
                         ┌─────────────┐
                         │ API Request │
                         │ Bearer JWT  │
                         └──────┬──────┘
                                │
                         ┌──────▼──────┐
                         │  JWT Parse  │  org, user, subject type
                         └──────┬──────┘
                                │
                         ┌──────▼──────┐
                         │ RBAC Check  │  control plane or data plane
                         └──────┬──────┘
                                │
                    ┌───────────▼───────────┐
                    │   ELS Resolution      │
                    │  1. Cache (fast path)  │
                    │  2. DB (cache miss)    │
                    │  3. Decrypt config     │
                    │  4. Resolve typed auth │
                    └───────────┬───────────┘
                                │
                    ┌───────────▼───────────┐
                    │  Credential Injection  │
                    │  SQL:  SET statements  │
                    │  HTTP: auth headers    │
                    │  Conn: credential swap │
                    └───────────┬───────────┘
                                │
                         ┌──────▼──────┐
                         │   Engine    │  execute against target
                         └─────────────┘
```

**Key property:** The AI model only describes *what* operation to perform. The
service layer decides *how* to authenticate. Credentials are injected after the
model's context boundary, so even a fully compromised agent cannot extract
them.

Authorization mode is exclusive per `(user, endpoint)`:

- A given user on a given endpoint is either in **RBAC mode** or **ELS mode**.
- Different users on the same endpoint may be in different modes.
- When an ELS assignment exists for a user on an endpoint, that user runs in
  **ELS mode** on that endpoint and Eden skips RBAC read/write gating there.
- When no ELS assignment exists for that user on that endpoint, the request
  remains in **RBAC mode** and uses the endpoint's shared credentials.

### 4.1 Why ELS Bypasses Data-Plane RBAC

When a user is in ELS mode, Eden's data-plane permission bits (`r`, `w`, `x`)
are intentionally not enforced. This is a deliberate security design, not an
omission:

1. **The target system enforces permissions.** ELS credentials carry the user's
   identity to the target (e.g., a PostgreSQL role with RLS policies, a Redis
   ACL user, an HTTP endpoint with scoped API keys). The target system itself
   decides what the user can read, write, or execute based on those credentials.

2. **Double gating creates a false sense of security.** If Eden checked both
   RBAC and ELS, an admin might grant a user `rw` RBAC data-plane permissions
   *and* assign ELS credentials scoped to read-only. The user would appear to
   have write access in the Eden UI, but the target would reject writes. This
   mismatch is confusing and error-prone.

3. **Preventing circumvention.** If a user without proper ELS credentials could
   fall back to shared credentials via RBAC, a malicious actor could
   potentially bypass credential isolation by removing their own ELS assignment
   or exploiting race conditions. The strict mutual exclusivity means that once
   a user has an ELS assignment, there is no alternate data-plane path.

4. **Fail-closed behavior.** If an ELS assignment exists but credentials cannot
   be resolved or applied, the request fails; it never falls back to shared
   credentials. This prevents accidental privilege escalation.

**Control-plane permissions still apply.** A user's control-plane bits
(`R/C/P/G/D/A`) continue to govern what they can *configure* regardless of
their data-plane mode. A user with the needed control-plane permissions can
manage ELS policies whether or not they themselves have an ELS assignment.

## 5. Data Model

### 5.1 Core Entities

**Policies**: Named credential configurations scoped to an endpoint.

| Column | Type | Notes |
|--------|------|-------|
| `uuid` | UUID PK | |
| `org_uuid` | UUID | Tenant isolation |
| `endpoint_uuid` | UUID | |
| `name` | VARCHAR(255) | Human-readable label |
| `strategy` | VARCHAR(50) | Endpoint type (postgres, mysql, aws, http, ...) |
| `config` | JSONB | Encrypted credential payload |
| `created_at` / `updated_at` | TIMESTAMPTZ | |

Unique constraint: `(endpoint_uuid, name)`.

**Assignments**: Binds a user (or machine identity) to a policy on an
endpoint. One assignment per user per endpoint.

| Column | Type | Notes |
|--------|------|-------|
| `endpoint_uuid, user_uuid` | PK | |
| `policy_uuid` | UUID FK | |
| `mode` | VARCHAR(10) | `sync` or `copy` |
| `strategy_snapshot` | VARCHAR(50) | Frozen strategy (copy mode only) |
| `config_snapshot` | JSONB | Frozen config (copy mode only) |

**Versions**: Immutable snapshots of a policy's credential config.

| Column | Type | Notes |
|--------|------|-------|
| `policy_uuid, version` | PK | Monotonically increasing |
| `strategy` | VARCHAR(50) | |
| `config` | JSONB | Encrypted |
| `status` | VARCHAR(16) | `draft`, `active`, `superseded`, or `rejected` |
| `created_by` | UUID | Audit trail |

**Pointers**: Tracks which version of a policy is currently active.

| Column | Type | Notes |
|--------|------|-------|
| `policy_uuid` | UUID PK | |
| `active_version` | INTEGER | Nullable until first promotion |
| `activated_by` | UUID | |
| `activated_at` | TIMESTAMPTZ | |

### 5.2 Assignment Modes

- **Sync**: The user's effective config is always the policy's current active
  version. When a new version is promoted, all sync'd users pick up the change
  immediately (the cache is re-warmed).
- **Copy**: A snapshot of the policy config is taken at assignment time. The
  user is independent of future policy changes. Useful for freezing credentials
  for CI pipelines and machine identities.

### 5.3 Version Lifecycle

```
         create               promote              promote next
  ───────►  Draft  ──────────►  Active  ──────────►  Superseded
            │                                            │
            │ reject                                     │ rollback
            ▼                                            │
         Rejected         ◄──────────────────────────────┘
```

Promotion and rollback use **optimistic concurrency control**: the caller
supplies `expected_current` (the version they believe is currently active). If
it doesn't match, the operation returns a conflict error. This prevents race
conditions when multiple admins act simultaneously.

Promote and rollback are wrapped in a database transaction. On commit, all
sync-mode assignments are re-cached with the new config.

## 6. Encryption at Rest

### 6.1 Key Hierarchy (Envelope Encryption)

```
Infrastructure (env var / K8s Secrets / KMS)
  └── Org Key  (one per tenant, 256-bit AES)
        └── DEK  (one per endpoint, randomly generated, wrapped by org key)
              └── ELS config  (encrypted by DEK)
```

### 6.2 Cipher

- **Algorithm:** AES-256-GCM (authenticated encryption)
- **Nonce:** Random 12 bytes (96-bit), prepended to ciphertext
- **Birthday bound:** ~2^48 encryptions per DEK before nonce-collision risk.
  For ELS configs (encryptions only on draft creation), this is well within
  safe margins.

### 6.3 Storage Format

| Location | Format |
|----------|--------|
| Database `config` column | `{"__encrypted": "<base64(nonce || ciphertext)>"}` |
| Cache values | `ENC:<base64(nonce || ciphertext)>` |

Legacy plaintext configs are detected transparently (absence of the
`__encrypted` sentinel or `ENC:` prefix), so existing deployments can enable
encryption without schema changes: reads handle both formats; only new writes
are encrypted.

### 6.4 Key Providers

An `OrgKeyProvider` trait abstracts key retrieval. The initial implementation
reads a hex-encoded 256-bit key from an environment variable. Provider stubs
exist for:

- Kubernetes Secrets (requires etcd encryption at rest)
- AWS KMS
- Azure Key Vault
- GCP KMS
- HashiCorp Vault

### 6.5 DEK Lifecycle

- **Lazy generation:** A DEK is created on the first ELS policy write for an
  endpoint, wrapped with the org key, and stored in the database.
- **Cache propagation:** The wrapped DEK is stored alongside cached policies so
  the service can decrypt without a database round-trip on cache hits.
- **Rotation:** DEK version is currently pinned to 1. Key rotation (re-wrap
  with new org key, or re-generate DEK and re-encrypt all versions) is planned.

## 7. Typed Endpoint Authentication

A type-safe `EpAuth` trait provides per-endpoint-type authentication with
compile-time guarantees. A resolver function deserializes `(strategy, config)`
pairs into the correct concrete type with validation.

### 7.1 Supported Strategies and Validation

Each strategy enforces required fields at policy-creation time:

| Strategy | Required Fields |
|----------|----------------|
| Postgres | `variables` (object) |
| MySQL, MSSQL, ClickHouse | `username` or `variables` (or both) |
| Snowflake | One of: `private_key`, `oauth_token`, `variables` |
| Oracle, MongoDB, Cassandra, ElastiCache | `username` |
| Redis | Exactly one of `username` or `endpoint_uuid` |
| AWS, RDS | `access_key_id` + `secret_access_key` |
| HTTP | `headers` (object) |
| Salesforce | `access_token` |
| Databricks | `token` |
| Datadog, Pinecone, Weaviate, Tavily, LLM | `api_key` |
| Function | Any valid JSON object (free-form) |

Invalid configs are rejected with descriptive errors before any credential is
stored.

For ClickHouse specifically, ELS does not implement filtering inside Eden.
Instead, Eden authenticates the request as the configured ClickHouse user and
optionally applies session settings; any row filtering is enforced by the
target ClickHouse server, typically via `ROW POLICY`.

### 7.2 Credential Injection by Endpoint Family

| Family | Endpoints | Mechanism |
|--------|-----------|-----------|
| **SQL session variables** | Postgres, MySQL, ClickHouse, Snowflake | Prepend `SET` / `ALTER SESSION SET` statements with escaped identifiers and literals |
| **HTTP headers** | HTTP, Salesforce, Databricks, Datadog, Pinecone, Weaviate, Tavily, LLM | Inject `Authorization`, `X-Api-Key`, or custom headers |
| **Connection override** | Oracle, MongoDB, Redis (ACL mode), Cassandra, AWS, RDS, ElastiCache | Replace connection credentials for the request |
| **Endpoint switch** | Redis (dedicated-endpoint mode) | Execute the request against another Redis endpoint in the same org |

Redis therefore has two distinct ELS modes:

- **ACL mode** uses `username` (and optional `password`) and opens a
  credential-specific connection override for the request.
- **Dedicated-endpoint mode** uses `endpoint_uuid` and dispatches the request
  through another Redis endpoint's normal pooled path.

### 7.3 SQL Injection Prevention

All SQL session-variable injection uses dedicated escaping functions per
dialect:

- **PostgreSQL:** Double-quote identifier escaping + single-quote literal
  escaping
- **MySQL:** Backtick identifier escaping + single-quote literal escaping
- **ClickHouse / Snowflake:** Equivalent quoted-identifier escaping

### 7.4 Debug Redaction

All auth types use a macro that replaces sensitive fields (passwords, keys,
tokens) with `[REDACTED]` in `Debug` output, preventing accidental credential
leakage in application logs.

## 8. RBAC Enhancements

### 8.1 Control Plane / Data Plane Split

Eden separates access control into two independent planes:

**Control plane**: governs who can *configure and manage* Eden resources.

| Bit | Name | Purpose |
|-----|------|---------|
| `R` | Read | View endpoint configuration and metadata |
| `C` | Configure | Edit settings, draft ELS policies, create workflows |
| `P` | Promote | Activate or rollback versioned changes |
| `G` | Grant | Manage other users' permissions; view credential secrets |
| `D` | Destroy | Irreversible operations (delete endpoint, transfer ownership) |
| `A` | Audit | View decision logs, version history, authorization records |

Control-plane checks generally hit the database directly for consistency.
The one exception is bearer-token revalidation, which uses a narrow
process-local ShardMap positive cache for `(organization, subject)` membership
and falls back to the database on cache miss. That cache is invalidated on the
local process for org membership revoke/remove paths. Multi-replica deployments
need a distributed cache invalidation/backend layer for fleet-wide revocation
propagation.

**Data plane**: governs what *operations* a user can execute at runtime.

| Bit | Name | Purpose |
|-----|------|---------|
| `r` | read | SELECT queries, GET requests |
| `w` | write | INSERT, UPDATE, DELETE, POST/PUT/PATCH requests |
| `x` | execute | DDL, GRANT, administrative operations |

Data-plane checks use process-local ShardMap for low-latency cache-aside
enforcement on the hot path, with the database as the source of truth.

**Key distinction:** These planes use different permission systems.
Control-plane bits are uppercase (`R`, `C`, `P`, `G`, `D`, `A`). Data-plane
bits are lowercase (`r`, `w`, `x`). Control-plane `R` (read configuration)
has no relation to data-plane `r` (SELECT queries). A user can have
control-plane `G` (Grant) to manage who has access without being able to run
queries themselves, and vice versa.

**Independence.** A user's control-plane permissions determine what they can
*configure*. Their data-plane access determines what they can *do* at runtime.
These are fully independent: a user can manage endpoint settings without
having data-plane access, and a user with read-only data-plane access cannot
modify endpoint configuration.

**Interaction with ELS:** Data-plane permission bits (`r`, `w`, `x`) are only
enforced for users in Shared (RBAC) mode (no ELS assignment). For users in
Personal (ELS) mode, the target system enforces permissions via the injected
credentials. Control-plane permissions apply regardless of data-plane mode.

**Grant escalation protection:** To grant permission bits to another user, you
must hold `G` (Grant) plus all the bits you are granting. You cannot grant
bits you don't have yourself. To revoke a user's permissions, you must hold
`G` plus all the bits the target currently has.

ELS management routes require `C` (Configure) on the endpoint. Promoting ELS
policy versions requires `P` (Promote). Managing ELS user assignments
requires `G` (Grant).

See `docs/SECURITY_MODEL.md` for the full two-plane architecture diagram.

### 8.2 Tombstone Tables

Four tombstone tables prevent **grant resurrection**, a security issue where:

1. An admin revokes a user's access (grant deleted).
2. A stale or replayed sync re-creates the grant from an outdated replica.

Tombstones record the highest delete version seen for the sync pipeline. They
are replay guards, not the canonical mutation history for the RBAC domain
model. Any grant whose version is older than the tombstone is rejected, while a
newer version can still re-provision access. Tombstones are purged after a
configurable retention period (default 90 days) once the replay window has
passed.

## 9. API Surface

All routes are scoped under `/{endpoint}/els/` and require Admin access.

### 9.1 Policy CRUD

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/policies` | Create policy (validated against endpoint type) |
| `POST` | `/validate` | Validate a strategy/config pair for the endpoint |
| `GET` | `/policies` | List policies (redacted) |
| `GET` | `/policies/{uuid}` | Get policy (redacted) |
| `PUT` | `/policies/{uuid}` | Update policy config |
| `DELETE` | `/policies/{uuid}` | Delete policy (cascades to assignments) |
| `DELETE` | `/policies` | Delete all policies for endpoint |

### 9.2 User Assignments

| Method | Path | Description |
|--------|------|-------------|
| `PUT` | `/users/{user_uuid}` | Assign policy to user (sync or copy) |
| `GET` | `/users/{user_uuid}` | Get user's effective policy (redacted) |
| `GET` | `/users` | List all assignments (redacted) |
| `POST` | `/users/{user_uuid}/refresh` | Refresh a copy-mode snapshot from the policy's current effective config |
| `POST` | `/users/unassign` | Unassign selected users |
| `DELETE` | `/users/{user_uuid}` | Unassign user |
| `DELETE` | `/users` | Unassign all users |

### 9.3 Version Lifecycle

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/policies/{uuid}/versions` | Create draft version |
| `GET` | `/policies/{uuid}/versions` | List all versions (redacted, newest first) |
| `GET` | `/policies/{uuid}/versions/active` | Get the currently active version directly (redacted) |
| `GET` | `/policies/{uuid}/versions/{v}` | Get specific version (redacted) |
| `GET` | `/policies/{uuid}/pointer` | Get active version pointer |
| `POST` | `/policies/{uuid}/versions/{v}/promote` | Promote draft to active (optimistic lock) |
| `POST` | `/policies/{uuid}/versions/{v}/reject` | Reject a draft version |
| `POST` | `/policies/{uuid}/versions/{v}/rollback` | Rollback to superseded version (optimistic lock) |

### 9.4 Redacted Response Types

All API responses use redacted variants that omit the `config` field:

- **PolicyRedacted**: policy metadata without credentials
- **AssignmentRedacted**: assignment metadata without credentials
- **VersionRedacted**: version metadata without credentials

This is a compile-time guarantee: the conversion from internal to redacted
types drops credentials structurally, so there is no code path that
accidentally serializes them in an API response.

## 10. Caching

### 10.1 Layout

Each endpoint gets a cache hash keyed by user UUID. The hash also stores the
wrapped DEK and org key reference so that decryption can happen without a
database round-trip.

### 10.2 Invalidation Triggers

| Event | Action |
|-------|--------|
| Policy update | Re-cache all sync-mode assignments |
| User assigned | Cache the resolved policy |
| User unassigned | Remove from cache |
| Version promoted / rolled back | Re-cache all sync-mode assignments with new config |
| Endpoint deleted | Clear entire endpoint hash |

### 10.3 Resolution Path

1. **Cache hit**: decrypt (if encrypted) and resolve to typed auth. Done.
2. **Cache miss**: query assignments + policies from database, decrypt,
   resolve, cache, done.
3. **No assignment**: return `None`; request uses default endpoint
   credentials.

### 10.4 Auth-Mode Exclusivity

- **Scope:** exclusivity is per `(user, endpoint)`, not per endpoint globally.
- **ELS mode:** ELS assignment itself is the authorization for that user on
  that endpoint. Eden does **not** enforce data-plane permission bits (`r`, `w`,
  `x`) for ELS users; the target system enforces permissions using the
  injected credentials (see section 4.1 for rationale).
- **RBAC mode:** shared endpoint credentials plus Eden-enforced RBAC, which
  gates requests using the data-plane permission bits.
- **Precedence:** if a user has both a legacy RBAC grant and an ELS assignment
  on the same endpoint, data-plane requests resolve to ELS for that user.
- **Failure mode:** if an ELS assignment exists but its credentials cannot be
  applied to the request shape, Eden fails closed instead of falling back to
  shared credentials. This prevents accidental privilege escalation.
- **Metadata:** metadata introspection is unavailable for users in ELS mode.
- **LLM chat:** LLM endpoint chat uses the assigned user's ELS credentials in
  ELS mode.
- **Robots:** Robot callers always use RBAC mode. ELS assignments are
  currently user-only (see section 12.6).

## 11. Threat Model

| Threat | Mitigation |
|--------|-----------|
| LLM prompt injection extracts credentials | Credentials never enter model context; injected at the service layer after the context boundary |
| API response leaks credentials | Redacted types enforced at compile time; no code path serializes `config` |
| Database breach exposes credentials | Envelope encryption (AES-256-GCM); attacker needs both the database dump and the org key |
| Cache breach exposes credentials | Cached values encrypted; DEK wrapped by org key |
| Stale RBAC sync resurrects revoked access | Tombstone tables with version ordering block old grants |
| Race condition on version promotion | Optimistic concurrency control via `expected_current` |
| SQL injection via session variables | Dedicated identifier/literal escaping per SQL dialect |
| Debug logging leaks secrets | Macro-based redaction on all auth types |
| Org key loss | All DEKs become unwrappable and encrypted configs are unrecoverable (see Known Limitations) |

## 12. Known Limitations

These are understood trade-offs in the current implementation. They are
documented here to set expectations and guide future work.

### 12.1 No Pagination on List Endpoints

List endpoints (policies, assignments, versions) return all results without
pagination. For endpoints with a large number of policies or user assignments,
responses may be large.

### 12.2 Sequential Cache Re-Warming

When a policy is promoted or updated, all sync-mode assignments are re-cached
sequentially. For policies with many assigned users, this can make the promote
or update API call slow. Cache writes are not batched or parallelized.

### 12.3 Connection Pool Bypass

For endpoint types that use connection-override injection (Oracle, MongoDB,
Redis ACL mode, Cassandra, AWS, RDS, ElastiCache), each ELS-authenticated
request creates a new connection to the target system rather than reusing a
pooled connection. This is inherent to per-user credentials but means that
high-traffic ELS endpoints place more connection load on the target. Redis
endpoint-switch policies are different: they reuse the destination endpoint's
normal connection pool and therefore continue to respect its configured pool
limits. This distinction is intentional: only Redis dedicated-endpoint mode
inherits pool caps from the destination endpoint; Redis ACL mode still uses
the one-off connection path.

### 12.4 Cache Miss During Database Outage

If the cache does not contain a user's resolved policy and the database is
unreachable, the request fails. There is no "serve stale" fallback. Cache hits
continue to work during database outages.

### 12.5 No Key Rotation Tooling

DEK rotation (re-wrapping with a new org key, or re-generating DEKs and
re-encrypting all policy versions) is not yet implemented. If the org key is
lost, all encrypted configs become permanently unrecoverable. Operators should
treat the org key with the same care as a database master password and ensure
it is backed up in their secrets management infrastructure.

### 12.6 Robot-Specific ELS Support

ELS assignment APIs are currently user-oriented. Robot callers remain RBAC-only
until robot-specific assignment semantics are added.

## 13. Future Work

- **DEK rotation**: re-wrap DEKs when the org key is rotated; re-encrypt all
  policy versions under the new DEK.
- **KMS provider implementations**: AWS KMS, Azure Key Vault, GCP KMS,
  HashiCorp Vault.
- **Audit log**: record who promoted/rolled back which version, with the
  Audit permission gating read access.
- **Policy templates**: pre-built ELS configs for common patterns (e.g.,
  "PostgreSQL RLS with `SET role`").
- **Pagination**: cursor-based pagination for list endpoints.
- **Parallel cache warming**: batch or pipeline cache writes during promotion.
- **Connection pooling per ELS identity**: pool connections keyed by
  (endpoint, user) for connection-override strategies.
