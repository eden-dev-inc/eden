# Step 2: Deploy Eden

**Phase**: Setup
**Previous**: [Step 1: Current State](./step-01-current-state.md)
**Next**: [Step 3: Analyze](./step-03-analyze.md)

---

## Passive Listener

Eden deploys alongside your existing stack. At this stage, Eden is deployed but not yet receiving production traffic.

## Deployment Models

See [Deployment & DNS Architecture](../eden-dns-architecture.md) for complete details.

### Model A: Eden in Customer VPC

Eden deployed as a VM or container in customer's VPC.

```
Customer VPC
┌─────────────────────────────────────────────────────┐
│                                                     │
│   ┌──────┐      ┌──────┐      ┌──────────────┐      │
│   │ Apps │      │ Eden │      │ Redis        │      │
│   └──────┘      └──────┘      └──────────────┘      │
│                                                     │
└─────────────────────────────────────────────────────┘
```

**Benefits:** Low latency, traffic stays in VPC

### Model B: Eden Sidecar

Eden runs alongside each application (same VM or Kubernetes pod).

```
┌─────────────────────────────────┐
│  App VM / Pod                   │
│  ┌─────┐      ┌─────┐           │
│  │ App │ ───▶ │Eden │ ──────────┼───▶ Redis
│  └─────┘      └─────┘           │
│           localhost             │
└─────────────────────────────────┘
```

**Benefits:** No DNS change needed (localhost), lowest latency

## Initial Setup

### 1. Deploy Eden

Eden exposes:

- `8000` - Eden API
- Interlay ports (e.g., `6366`) - configurable ports for proxying database traffic

### 2. Create Organization

```bash
curl -X POST http://localhost:8000/api/v1/new \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $EDEN_NEW_ORG_TOKEN" \
  -d '{
    "id": "MyOrg",
    "description": "Production organization",
    "super_admins": [
      {
        "username": "admin",
        "password": "secure-password",
        "description": null
      }
    ]
  }'
```

### 3. Login

```bash
curl -X POST http://localhost:8000/api/v1/auth/login \
  -H "X-Org-Id: MyOrg" \
  -H "Authorization: Basic $(echo -n 'admin:password' | base64)"
```

**Response:**

```json
{
  "token": "eyJhbGciOiJIUzI1NiJ9..."
}
```

Save this token for subsequent API calls.

## Verification

At this stage:

- Eden is running but **not** intercepting traffic
- Applications still connect directly to the database
- No production impact

## What Changes Next

In [Step 3: Analyze](./step-03-analyze.md), you'll assess your current traffic patterns and plan the target configuration.

---

**Navigation**: [← Step 1](./step-01-current-state.md) | [Overview](./overview.md) | **Step 2** | [Step 3 →](./step-03-analyze.md)
