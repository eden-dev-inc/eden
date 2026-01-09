# Eden Database Migration Architecture

Eden enables zero-downtime database migrations by proxying application traffic and handling data replication
transparently. This document describes the production architecture using DNS-based traffic routing.

## Overview

Eden sits between your applications and databases. By pointing your applications to Eden via DNS, Eden can:

1. Proxy all traffic to your current database (transparent)
2. Replicate data to a new database in the background
3. Switch to the new database instantly (zero downtime)

Applications never know a migration happened.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Customer VPC                                   │
│                                                                             │
│   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                         │
│   │   App 1     │  │   App 2     │  │   App N     │                         │
│   │             │  │             │  │             │                         │
│   │ REDIS_HOST= │  │ REDIS_HOST= │  │ REDIS_HOST= │                         │
│   │ redis.int   │  │ redis.int   │  │ redis.int   │                         │
│   └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                         │
│          │                │                │                                │
│          └────────────────┼────────────────┘                                │
│                           │                                                 │
│                           ▼                                                 │
│                 ┌───────────────────┐                                       │
│                 │   DNS Resolution  │                                       │
│                 │   redis.internal  │                                       │
│                 └─────────┬─────────┘                                       │
│                           │                                                 │
│                           ▼                                                 │
│                 ┌───────────────────┐                                       │
│                 │                   │                                       │
│                 │   Eden Proxy      │                                       │
│                 │                   │                                       │
│                 └─────────┬─────────┘                                       │
│                           │                                                 │
│              ┌────────────┴────────────┐                                    │
│              │                         │                                    │
│              ▼                         ▼                                    │
│   ┌───────────────────┐     ┌───────────────────┐                           │
│   │  Source Database  │     │  Target Database  │                           │
│   │  (current)        │     │  (migration dest) │                           │
│   └───────────────────┘     └───────────────────┘                           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Migration Phases

### Phase 1: Onboarding (DNS Change)

Customer updates their DNS or application config to point to Eden.

**Before:**

```
redis.internal → 10.0.1.50 (Source Redis)
```

**After:**

```
redis.internal → 10.0.2.100 (Eden Proxy)
```

At this point:

- All traffic flows through Eden
- Eden forwards everything to Source Redis
- **No migration yet** — just proxying

```
┌─────┐      ┌──────┐      ┌────────────┐
│ App │ ───▶ │ Eden │ ───▶ │ Source DB  │
└─────┘      └──────┘      └────────────┘
```

### Phase 2: Replication

Eden begins replicating data from Source to Target.

```
┌─────┐      ┌──────┐      ┌────────────┐
│ App │ ───▶ │ Eden │ ───▶ │ Source DB  │
└─────┘      └──────┘      └────────────┘
                 │
                 │  replication
                 ▼
             ┌────────────┐
             │ Target DB  │
             └────────────┘
```

- Reads: served from Source
- Writes: written to Source, replicated to Target
- Eden tracks replication lag

### Phase 3: Validation

Before cutover, Eden validates:

- [ ] Replication lag is zero (fully caught up)
- [ ] Target database is healthy
- [ ] Data integrity checks pass

```bash
# Eden API
curl https://eden-api/migrations/abc123/status

{
  "phase": "validating",
  "replication_lag_ms": 0,
  "source": "healthy",
  "target": "healthy",
  "ready_for_cutover": true
}
```

### Phase 4: Cutover

Eden switches the upstream from Source to Target.

```
┌─────┐      ┌──────┐      ┌────────────┐
│ App │ ───▶ │ Eden │ ─ ✗  │ Source DB  │  (disconnected)
└─────┘      └──────┘      └────────────┘
                 │
                 │  now primary
                 ▼
             ┌────────────┐
             │ Target DB  │
             └────────────┘
```

- **Zero downtime**: TCP connections stay open, Eden buffers briefly during switch
- **Instant rollback**: Eden can switch back to Source if needed

### Phase 5: Completion

After validation on Target:

1. Eden continues proxying to Target
2. Customer can optionally update DNS to point directly to Target
3. Decommission Source database

---

## Deployment Models

### Model A: Eden in Customer VPC

Eden deployed as a VM or container in customer's VPC.

```
Customer VPC
┌─────────────────────────────────────────────────────┐
│                                                     │
│   ┌──────┐      ┌──────┐      ┌──────────────┐      │
│   │ Apps │ ───▶ │ Eden │ ───▶ │ Redis        │      │
│   └──────┘      └──────┘      └──────────────┘      │
│                                                     │
└─────────────────────────────────────────────────────┘
```

**Benefits:** Low latency, traffic stays in VPC

### Model B: Eden Sidecar

Eden runs alongside each application (same VM or Kubernetes pod).

```
┌─────────────────────────────┐
│  App VM / Pod               │
│  ┌─────┐      ┌─────┐       │
│  │ App │ ───▶ │Eden │ ─────-┼───▶ Redis
│  └─────┘      └─────┘       │
│           localhost         │
└─────────────────────────────┘
```

**Benefits:** No DNS change needed (localhost), lowest latency

---

## DNS Configuration

### Option 1: Internal DNS Record

Create a DNS record in your private hosted zone:

```bash
# AWS Route53 example
aws route53 change-resource-record-sets \
  --hosted-zone-id Z123456 \
  --change-batch '{
    "Changes": [{
      "Action": "UPSERT",
      "ResourceRecordSet": {
        "Name": "redis.internal",
        "Type": "A",
        "TTL": 30,
        "ResourceRecords": [{"Value": "10.0.2.100"}]
      }
    }]
  }'
```

### Option 2: CNAME to Eden

```
redis.internal CNAME eden-proxy.internal
```

### Option 3: Application Environment Variable

No DNS change — update app config directly:

```yaml
# Before
REDIS_HOST: myredis.redis.cache.azure.com

# After
REDIS_HOST: eden-proxy.internal
```

---

## Rollback

At any point, rollback is a DNS change away:

### During Proxy Phase (no migration started)

```
redis.internal → Source Redis IP
```

Apps reconnect to Source directly. Eden removed from path.

### During Replication Phase

```
redis.internal → Source Redis IP
```

Replication stops. Source still has all data. No data loss.

### After Cutover

```bash
# Option A: DNS rollback to Source (if Source still has data)
redis.internal → Source Redis IP

# Option B: Eden rollback (switch upstream back to Source)
curl -X POST https://eden-api/migrations/abc123/rollback
```

---

[//]: # (## High Availability)

[//]: # ()

[//]: # (For production, run multiple Eden instances behind a load balancer:)

[//]: # ()

[//]: # (```)

[//]: # (                    ┌─────────────────┐)

[//]: # (                    │  Load Balancer  │)

[//]: # (                    │  redis.internal │)

[//]: # (                    └────────┬────────┘)

[//]: # (                             │)

[//]: # (               ┌─────────────┼──────────────┐)

[//]: # (               ▼             ▼              ▼)

[//]: # (          ┌────────┐     ┌────────┐    ┌────────┐)

[//]: # (          │ Eden 1 │     │ Eden 2 │    │ Eden 3 │)

[//]: # (          └────┬───┘     └───┬-───┘    └────┬───┘)

[//]: # (               │             │              │)

[//]: # (               └─────────────┴──────────-───┘)

[//]: # (                             │)

[//]: # (                             ▼)

[//]: # (                     ┌──────────────┐)

[//]: # (                     │    Redis     │)

[//]: # (                     └──────────────┘)

[//]: # (```)

[//]: # ()

[//]: # (Eden instances share state via:)

[//]: # ()

[//]: # (- Shared PostgreSQL for migration metadata)

[//]: # (- Coordination for cutover &#40;leader election&#41;)

[//]: # ()

[//]: # (---)

## Security Considerations

### TLS Termination

Eden can terminate TLS from apps and establish new TLS to databases:

```
App ──TLS──▶ Eden ──TLS──▶ Redis
```

### Authentication Passthrough

Eden passes through AUTH commands transparently. No credential storage needed.

### Network Isolation

Eden should be deployed in a private subnet with:

- Ingress: Only from application subnets
- Egress: Only to database endpoints

---

## Monitoring

Key metrics to monitor:

| Metric                    | Description                | Alert Threshold |
|---------------------------|----------------------------|-----------------|
| `eden_proxy_latency_ms`   | Added latency from proxy   | > 1ms           |
| `eden_replication_lag_ms` | How far behind Target is   | > 1000ms        |
| `eden_connections_active` | Current client connections | Baseline        |
| `eden_throughput_ops`     | Operations per second      | Baseline        |
| `eden_errors_total`       | Connection/protocol errors | > 0             |

---

## Example: Redis Migration from Self-Hosted to Azure

### Starting State

- Self-hosted Redis on EC2: `10.0.1.50:6379`
- Apps configured with: `REDIS_HOST=redis.internal`
- DNS: `redis.internal → 10.0.1.50`

### Step 1: Deploy Eden and Configure Endpoints

```bash
# Deploy Eden VM at 10.0.2.100
eden-server --listen 0.0.0.0:6379

# Create source endpoint
curl http://10.0.2.100:8000/api/v1/endpoints \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "ec2_redis",
    "uri": "redis://10.0.1.50:6379"
  }'

# Create target endpoint
curl http://10.0.2.100:8000/api/v1/endpoints \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "azure_redis",
    "uri": "redis://myredis.redis.cache.azure.com:6380"
  }'
```

### Step 2: Point DNS to Eden

```bash
# Update DNS
redis.internal → 10.0.2.100
```

Validate:

```bash
redis-cli -h redis.internal PING
# Returns: PONG (via Eden)
```

### Step 3: Create Migration with Canary Strategy

```bash
curl http://10.0.2.100:8000/api/v1/migrations \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "ec2_to_azure",
    "description": "Migrate from EC2 Redis to Azure Cache",
    "strategy": {
      "type": "canary",
      "read_percentage": 0.05,
      "write_mode": {
        "mode": "dual_write",
        "policy": "old_authoritative"
      }
    },
    "data": {
      "type": "scan",
      "replace": "replace"
    },
    "failure_handling": "rollback_all"
  }'

# Add API/Interlay to migration
curl http://10.0.2.100:8000/api/v1/migrations/ec2_to_azure/interlay/redis_interlay \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "endpoint": "azure_redis"
  }'
```

### Step 4: Execute and Monitor Migration

```bash
# Start migration
curl http://10.0.2.100:8000/api/v1/migrations/ec2_to_azure/migrate \
  -H "Authorization: Bearer $TOKEN" \
  -X POST

# Monitor status
curl http://10.0.2.100:8000/api/v1/migrations/ec2_to_azure \
  -H "Authorization: Bearer $TOKEN"
```

### Step 5: Gradual Rollout

```bash
# Increase to 25% read traffic
curl http://10.0.2.100:8000/api/v1/migrations/ec2_to_azure \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -X PATCH \
  -d '{
    "strategy": {
      "type": "canary",
      "read_percentage": 0.25,
      "write_mode": {"mode": "dual_write", "policy": "old_authoritative"}
    }
  }'

# Increase to 75%
curl http://10.0.2.100:8000/api/v1/migrations/ec2_to_azure \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -X PATCH \
  -d '{
    "strategy": {
      "type": "canary",
      "read_percentage": 0.75,
      "write_mode": {"mode": "dual_write", "policy": "old_authoritative"}
    }
  }'
```

### Step 6: Complete Cutover

```bash
# Full cutover to Azure
curl http://10.0.2.100:8000/api/v1/migrations/ec2_to_azure \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -X PATCH \
  -d '{
    "strategy": {
      "type": "canary",
      "read_percentage": 1.0,
      "write_mode": {"mode": "cutover", "write_percentage": 1.0}
    }
  }'

# Verify completion
curl http://10.0.2.100:8000/api/v1/migrations/ec2_to_azure \
  -H "Authorization: Bearer $TOKEN"
```

### Step 7: (Optional) Direct DNS to Azure

```bash
# Remove Eden from path after validation
redis.internal → myredis.redis.cache.azure.com
```

---

## FAQ

**Q: What latency does Eden add?**
A: Typically < 1ms for in-VPC deployments. Eden is a pass-through proxy with minimal processing.

**Q: What if Eden crashes during migration?**
A: Apps reconnect automatically. If during replication, resume from last checkpoint. If during cutover, Eden coordinates
to ensure atomicity.

**Q: Can I migrate between different database types?**
A: Eden supports protocol-compatible migrations (Redis → Redis, PostgreSQL → PostgreSQL). Cross-database migrations
require application changes.

**Q: Do apps need to be restarted?**
A: No. DNS TTL expiry + connection pool refresh handles it. For long-lived connections, a rolling restart may speed up
the transition.

**Q: What about connection limits?**
A: Eden multiplexes connections. 1000 app connections may only need 10 connections to the database.
