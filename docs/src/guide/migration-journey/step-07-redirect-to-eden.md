# Step 7: Redirect to Eden

**Phase**: Migration
**Previous**: [Step 6: Shadow Test](./step-06-shadow-test.md)
**Next**: [Step 8: Canary Migration](./step-08-canary-migration.md)

---

## Proxy Activated

Update your connection string to point at Eden. Eden proxies everything to the source database while **mirroring all writes to the target**.

## Architecture

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

At this point:

- All traffic flows through Eden
- Eden forwards everything to Source
- Writes replicated to Target

## Networking Setup Options

Choose the approach that fits your infrastructure:

### Option 1: Eden & DB on the Same Machine

If Eden runs on the same machine as the database (or in containers on the same host), use nftables to redirect traffic.

**Prerequisites:**

```bash
sudo nft -V  # Verify nftables is available
```

**Setup nftables rules:**

```bash
# Get Eden's internal IP (if running in Docker)
docker inspect eden-mdbs_eden_1 | grep IPAddress
# Example output: "IPAddress": "172.24.2.1"

# Create nftables rules to forward port 6379 to Eden interlay
sudo nft add table ip edenswitch 2>/dev/null || true
sudo nft 'add chain ip edenswitch prerouting { type nat hook prerouting priority -110; }' 2>/dev/null || true

# Forward external traffic on port 6379 to Eden interlay on 6366
sudo nft add rule ip edenswitch prerouting \
  iifname "eth0" ip daddr <EXTERNAL_IP> tcp dport 6379 \
  dnat to 172.24.2.1:6366
```

This forwards `<EXTERNAL_IP>:6379` → `172.24.2.1:6366` (Eden interlay).

Eden connects to the actual Redis on `localhost:6379` via loopback, which is unaffected by the iptables rule.

### Option 2: Eden on a Different Machine (DNS Change)

When Eden runs on a different machine than the source database, combine DNS changes with nftables on the Eden machine.

**Setup:**

1. **Create DNS records:**

```
redis-old   → <SOURCE_IP>   (legacy record for Eden to connect to source)
redis       → <EDEN_IP>     (main record - will point to Eden)
```

2. **On the Eden machine, setup nftables** to forward port `6379` to the interlay:

```bash
sudo nft add table ip edenswitch 2>/dev/null || true
sudo nft 'add chain ip edenswitch prerouting { type nat hook prerouting priority -110; }' 2>/dev/null || true

sudo nft add rule ip edenswitch prerouting \
  iifname "eth0" ip daddr <EDEN_EXTERNAL_IP> tcp dport 6379 \
  dnat to 172.24.2.1:6366
```

3. **Connect the source endpoint** using the legacy DNS name (`redis-old`) so it won't be affected by the DNS change.

4. **Change the main DNS record** to point to Eden:

```
redis → <EDEN_IP>
```

Now clients connect to `redis:6379` → Eden machine → nftables forwards to interlay on `6366` → forwards to `redis-old:6379`.

**DNS propagation:** Wait for TTL to expire (Cloudflare default is 5 minutes) before proceeding.

### Option 3: Application Config Change

If you can update application config, point directly to Eden's interlay port:

```yaml
# Before
REDIS_HOST: myredis.redis.cache.azure.com
REDIS_PORT: 6379

# After
REDIS_HOST: eden-proxy.internal
REDIS_PORT: 6366  # Eden interlay port
```

This avoids nftables but requires application changes.

## Force Client Reconnection

After DNS change, existing connections may still point to the old IP. Use `conntrack` to drop connections and force reconnection:

**Prerequisites:**

```bash
sudo conntrack -V  # Verify conntrack is available
```

**Drop existing Redis connections:**

```bash
sudo conntrack -D -p tcp --orig-port-dst 6379 2>/dev/null || true
```

Run this after DNS TTL expires to ensure all clients reconnect through Eden.

## Create Interlay

```bash
curl -X POST http://localhost:8000/api/v1/interlays \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "id": "redis-interlay",
    "endpoint": "'$SOURCE_UUID'",
    "port": 6366,
    "description": "Redis proxy for migration",
    "tls": null,
    "settings": {}
  }'
```

## Create Migration

```bash
curl -X POST http://localhost:8000/api/v1/migrations \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "redis-migration",
    "description": "Migrate from source Redis to target Redis",
    "strategy": {
      "type": "canary",
      "read_percentage": 0.05,
      "write_mode": {
        "mode": "dual_write",
        "policy": "OldAuthoritative"
      }
    },
    "data": {"Scan": {"replace": "Replace"}},
    "failure_handling": null,
    "tests": []
  }'
```

## Add Interlay to Migration

```bash
curl -X POST http://localhost:8000/api/v1/migrations/redis-migration/interlay/redis-interlay \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "redis-migration",
    "endpoint": "'$TARGET_UUID'",
    "description": "Interlay migration config",
    "migration_strategy": {
      "type": "canary",
      "read_percentage": 0.05,
      "write_mode": {
        "mode": "dual_write",
        "policy": "OldAuthoritative"
      }
    },
    "migration_data": {"Scan": {"replace": "Replace"}},
    "migration_rules": {
      "traffic": {
        "read": "Old",
        "write": {"Replicated": {"policy": "OldAuthoritative"}}
      },
      "error": "DoNothing",
      "rollback": "Ignore",
      "completion": {"milestone": "Immediate", "require_manual_approval": false}
    }
  }'
```

## Start Migration

```bash
curl -X POST http://localhost:8000/api/v1/migrations/redis-migration/migrate \
  -H "Authorization: Bearer $TOKEN"
```

## Rollback

At any point, rollback by reversing the networking change:

**If using nftables:**

```bash
# Remove the forwarding rule
sudo nft -a list chain ip edenswitch prerouting
# Find the rule handle, then:
sudo nft delete rule ip edenswitch prerouting handle <N>
```

**If using DNS:**

```
redis.mycompany.com → Source Redis IP
```

Apps reconnect to Source directly. Eden removed from path.

## What Changes Next

In [Step 8: Canary Migration](./step-08-canary-migration.md), you'll gradually shift read traffic from source to target.

---

**Navigation**: [← Step 6](./step-06-shadow-test.md) | [Overview](./overview.md) | **Step 7** | [Step 8 →](./step-08-canary-migration.md)
