# Eden Security Model

This document describes Eden's security architecture for AI agent interactions
with external services. It is intended for developers working on Eden and for
users evaluating Eden's security posture.

## Credential Isolation

Eden enforces a strict separation between the AI model's decision-making and
credential management. This is the most important security property of the
system.

### The Problem

When an AI agent calls external services (databases, APIs, cloud providers), it
needs authentication. A naive design passes credentials directly to the agent
so it can make authenticated requests. This creates a critical vulnerability:

- A prompt-injection attack could cause the agent to leak credentials in its
  output, log them, or send them to an attacker-controlled service.
- Credentials that enter the model's context window become part of the
  conversation history and may persist in caches, logs, or downstream tool
  calls.
- Even without malicious intent, the model has no reason to see credentials;
  it only needs to describe *what* it wants to do, not *how* to authenticate.

### Eden's Design

Eden separates the concerns into two layers:

1. **Model-facing layer (tools):** The AI agent describes the operation it
   wants to perform, for example, "run this SQL query" or "call this REST
   endpoint". It specifies the request payload and the required access level
   (read vs write). It has no access to credentials.

2. **Service layer (credential injection):** When the tool makes an outbound
   request, the service layer transparently attaches the appropriate
   authentication context. Depending on endpoint type, this may be HTTP
   headers, SQL session state, a credential-specific connection override, or
   routing to a dedicated destination endpoint. The model never sees these
   credentials, and the tool code has no mechanism to read or override them.

This means that even if an AI agent is fully compromised through prompt
injection, the attacker can request operations (subject to access control
enforcement) but **cannot extract the underlying credentials**: API keys,
tokens, passwords, and other secrets never enter the model's context.

### Access Level Semantics

The model can indicate what access level it believes a request requires (e.g.,
read-only vs write). This is treated as **access context**, not as an
authorization decision. The actual permission enforcement happens at the
service layer when credentials are injected and when the target endpoint
validates the request against the user's granted permissions.

## Access Control

Eden enforces access control at multiple points:

- **Authentication:** Every request requires a valid token. Tokens are
  validated on each request, not just at session initialization.

- **Organization isolation:** All operations are scoped to an organization.
  Agents cannot access endpoints belonging to other organizations.

- **Command safety classification:** Each endpoint type classifies operations
  by safety level (safe, moderate, dangerous). Dangerous operations require
  explicit approval and can be permanently blocked by policy.

- **Blocked patterns:** Administrators can define regex patterns that are
  unconditionally rejected, regardless of safety classification or user
  permissions.

### Control Plane vs Data Plane

Eden separates authorization into two independent planes with different
permission systems, scoping, and enforcement mechanisms.

```
CONTROL PLANE: "Who can configure Eden"
  Permission bits: R (Read), C (Configure), P (Promote),
                   G (Grant), D (Destroy), A (Audit)
  Enforcement: per-request, directly against stored bits (no cache)
  Grant protection: you can only grant bits you hold yourself

DATA PLANE: "What operations execute at runtime"
  Mode 1: Shared (RBAC), default
    Permission bits: r (read), w (write), x (execute)
    Eden enforces r/w/x before forwarding; shared credentials used

  Mode 2: Personal (ELS), when an ELS policy is assigned
    No Eden-side r/w/x gating
    User's own credentials injected at service layer
    Target system enforces access (PostgreSQL RLS, Redis ACLs, etc.)
```

**Key properties:**

- **Dashboard setup flow.** The endpoint security UI now presents two simple
  tables: one for control-plane access and one for data-plane access. Admins
  add or edit access from the relevant row, and the data-plane table shows
  whether each user is in shared `RBAC` mode or personal `ELS` mode. If the
  viewer cannot inspect the full directory, the UI falls back to showing only
  that viewer's own access and points them to the docs for the rest. Endpoint
  discovery is also consolidated under a single dashboard Endpoints page, with
  Endpoint Groups used as the narrowing mechanism instead of separate per-type
  endpoint pages. Account administration follows the same pattern: the
  dashboard now uses a single Access page with an in-page Humans or Agents
  switcher rather than separate navigation pages for each identity type.

- **Independence.** Control-plane permissions (`R/C/P/G/D/A`) are completely
  separate from data-plane access (`r/w/x` or ELS). A user with `G` (Grant)
  can manage who has access without being able to run queries. A user with
  data-plane `r` can query the endpoint without being able to change config.

- **Different bit systems.** Control-plane bits are uppercase (`R`, `C`, `P`,
  `G`, `D`, `A`). Data-plane bits are lowercase (`r`, `w`, `x`). Control-plane
  `R` (read config) has no relation to data-plane `r` (SELECT queries).

- **Different enforcement.** Control-plane checks generally hit the database
  directly for consistency. The one narrow exception is bearer-token
  revalidation, which uses a process-local ShardMap positive cache for
  `(organization, subject)` membership and falls back to the database on cache
  miss. Data-plane Shared-mode checks use process-local ShardMap as a cache
  over Postgres for low-latency enforcement. In Personal (ELS) mode, the
  target system itself is the enforcer. Multi-replica deployments require a
  distributed cache invalidation/backend layer for fleet-wide revocation
  propagation; the embedded cache only shares state inside one service process.

- **Control plane always applies.** Regardless of data-plane mode, a user's
  control-plane bits govern what they can configure. A user can manage ELS
  policies whether or not they have an ELS assignment themselves.
- **Resource-specific grants are enforced at runtime.** Template, workflow,
  and API handlers honor either organization-wide control-plane bits or the
  corresponding resource-specific grant for the concrete object being read,
  updated, executed, or deleted.
- **Operational admin routes are RBAC-gated.** Trigger management and other
  privileged control-plane operations require explicit authorization;
  organization membership alone is not sufficient to invoke them.

**Data-plane modes in detail:**

1. **Shared (RBAC) mode**: the default. The user authenticates using the
   endpoint's shared credentials. Eden enforces data-plane permission bits
   (`r`, `w`, `x`) via RBAC before the request reaches the target.

2. **Personal (ELS) mode**: activated when an admin assigns an Endpoint-Level
   Security policy to a user. The user authenticates with their own credentials
   (injected transparently by Eden). The target system itself enforces what the
   user can do. Eden intentionally does not apply data-plane RBAC gating in
   this mode; the ELS assignment is the authorization, and the target's own
   access controls are the source of truth.

These modes are mutually exclusive: assigning an ELS policy switches a user to
personal mode; removing it switches them back to shared mode. If ELS
credentials cannot be resolved, the request fails closed; it never falls back
to shared credentials.

## Agent Execution

Agents (autonomous AI tasks) operate with the same credential isolation and
access control as interactive sessions:

- Agent credentials are scoped per agent, not inherited from a user session.
- Persistent agent management is restricted to organization administrators.
- Agent-to-agent delegation and task status APIs are restricted to
  organization administrators.
- Each agent is configured with a specific set of endpoints it can access.
- Referenced robots, managed endpoints, and user-registered tool endpoints are
  validated against the agent's organization and creator before they are
  persisted or executed.
- Endpoint access is verified against the agent's organization.

## Secret Storage

Application-managed secrets are encrypted at rest before they are written to
the database:

- LLM provider API keys
- Human-owned tool endpoint bearer tokens
- Per-user database passwords

User-registered tool endpoints are human-scoped assets. Robot subjects can use
organization-approved tool endpoints, but they do not read, create, or delete
human-owned tool endpoint registrations.

New secrets are encrypted against the owning organization's dedicated key
reference. Multi-tenant deployments must provide a distinct org key for each
tenant; Eden no longer falls back to a shared process-wide secret for new
writes. Legacy plaintext rows are still readable during upgrade, and older
single-key ciphertext remains readable only when an explicit compatibility key
is configured.

### Orchestrated Execution

When an agent decomposes a task into sub-tasks (orchestration), each sub-task:

- Runs with tools scoped to its parent's permitted endpoints.
- Cannot reference endpoints outside the parent invocation's scope.
- Receives predecessor results as data context, not as trusted instructions.

## PII Protection

When the AI model queries a database or calls an API through a tool, the
response may contain personally identifiable information (PII) belonging to
end users: email addresses, phone numbers, Social Security numbers, credit
card numbers, API keys, or passwords.

Eden automatically masks PII in every tool result before it reaches the model:

- **Pattern-based detection:** Response text is scanned for common PII formats
  (emails, phone numbers, SSNs, credit card numbers, API keys). Matches are
  replaced with `[REDACTED:<type>]` markers.

- **Field-name detection:** When the response is structured JSON, fields with
  sensitive names (password, token, secret, ssn, credit_card, etc.) are fully
  redacted regardless of their value format.

- **Automatic enforcement:** PII masking is applied at the relay layer, before
  tool code processes the response. There is no opt-out from model-facing
  code; the masking cannot be bypassed by prompt injection or tool
  manipulation.

This means the model receives the structure and non-sensitive content of query
results, but never sees raw PII values. The original unmasked data remains
available to the service layer for legitimate application use; only the
model-facing path is masked.

### LLM Gateway PII Governance

The relay-layer masking above protects the model from PII in *tool results*.
The LLM gateway applies a complementary control on the *outbound* path
(prompts leaving Eden for a third-party model provider), governed per agent.

Each agent (API key) carries a PII policy with four levels:

- **Allow**: scan for telemetry only; do not mutate or block.
- **Audit only**: record detected PII without mutating the prompt.
- **Redact**: mask built-in PII (emails, phones, SSNs, payment cards) in the
  prompt in place before it is forwarded to the provider.
- **Block**: reject the request before it leaves Eden when PII is present.

New agents default to **Redact**, so PII is enforced out of the box; operators
can relax or tighten the policy per agent.

**Custom PII dictionary.** Each agent may define additional literal terms
(case-insensitive), e.g. internal project codenames, each with its own action
(audit / redact / block). A matched `block` term rejects the request; a matched
`redact` term is masked even when the agent's base policy is audit/allow. These
checks run pre-egress in the proxy's governance evaluator, so the redacted or
rejected request is what reaches the provider, not the original.

The per-agent policy (and built-in redact/block) also propagates to standalone
data-plane gateway processes via the control-plane snapshot, keyed by API-key
hash.

## Network Security

- Eden services communicate over authenticated HTTP. TLS is expected in
  production deployments.
- Tool endpoints relay requests through Eden's service layer; they do not
  connect directly to external databases or APIs. This means the AI model
  never holds connection strings, database passwords, or service credentials.

## Target-Side Data Security

Eden's ELS layer is responsible for choosing the correct authenticated
identity for each request. Data filtering itself can still be enforced by the
target system. For example:

- PostgreSQL can enforce native RLS policies after Eden sets per-user session
  context.
- ClickHouse can enforce `ROW POLICY` rules for the authenticated user or
  active session settings.
- Redis can either authenticate with ACL credentials or switch to a dedicated
  Redis endpoint for the request.

For Redis, these two ELS modes have different connection behavior:

- ACL mode uses a per-request connection override.
- Dedicated-endpoint mode reuses the destination endpoint's normal connection
  pool and therefore continues to honor its configured pool limits.

## What Eden Does Not Do

For transparency, here are security properties Eden does **not** currently
enforce:

- **Container-level isolation per agent:** Agents run within the service
  process. Process-level sandboxing is the responsibility of the deployment
  environment (e.g., Kubernetes pod security policies).

- **Prompt injection detection:** Eden does not scan model inputs or outputs
  for prompt injection patterns. The credential isolation design mitigates the
  impact of successful prompt injection, but does not prevent it.

- **Credential rotation:** Credential lifecycle management (rotation,
  expiration) is the responsibility of the credential owner, not the relay
  layer.
