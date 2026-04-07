# Step 1: Current State

**Phase**: Setup
**Previous**: None
**Next**: [Step 2: Deploy Eden](./step-02-deploy-eden.md)

---

## Direct Connection

Your applications connect directly to your existing database. No proxy, no intermediary.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Customer VPC                             │
│                                                                  │
│   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
│   │   App 1     │  │   App 2     │  │   App N     │              │
│   │             │  │             │  │             │              │
│   │ REDIS_HOST= │  │ REDIS_HOST= │  │ REDIS_HOST= │              │
│   │ redis.int   │  │ redis.int   │  │ redis.int   │              │
│   └──────┬──────┘  └──────┬──────┘  └──────┬──────┘              │
│          │                │                │                     │
│          └────────────────┼────────────────┘                     │
│                           │                                      │
│                           ▼                                      │
│                ┌───────────────────┐                             │
│                │  Source Database  │                             │
│                │  (e.g., Redis)    │                             │
│                └───────────────────┘                             │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## What Exists

At this stage, your infrastructure consists of:

- **Applications** with DNS-based or direct connection strings
- **DNS record** pointing to database (e.g., `redis.internal → 10.0.1.50`)
- **Production database** serving all traffic directly

## Example Starting State

```
DNS:  redis.internal → 10.0.1.50
Apps: REDIS_HOST=redis.internal
DB:   Self-hosted Redis on 10.0.1.50:6379
```

## What Changes Next

In [Step 2: Deploy Eden](./step-02-deploy-eden.md), you'll deploy Eden and configure endpoints for your source and target databases.

---

**Navigation**: [Overview](./overview.md) | **Step 1** | [Step 2 →](./step-02-deploy-eden.md)
