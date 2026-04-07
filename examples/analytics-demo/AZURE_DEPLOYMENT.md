# Deploying `analytics-server` and `traffic-client` on Azure

This guide shows the easiest Azure-native way to run the demo with:

- Azure Container Apps for `analytics-server`
- Azure Container Apps for `traffic-client`
- Azure Container Registry for the image
- Azure Database for PostgreSQL for the system of record
- Redis on Azure for the cache tier
- Datadog only for cloud observability

This guide intentionally does **not** deploy Prometheus or Grafana in Azure.

## Recommended Azure Shape

Use one Azure Container Apps environment and deploy:

- `analytics-server` as the customer-facing API
- `traffic-client` as a scalable worker pool that calls the server over HTTP
- PostgreSQL as the source of truth
- Redis as the cache tier
- Azure Native Datadog for Azure resource telemetry

Why this shape works well for the demo:

- `analytics-server` looks like a real backend service
- `traffic-client` can scale up and down independently
- both apps can live in the same Container Apps environment and talk over internal service discovery
- Azure manages the container runtime, ingress, scaling, and logs

## Important Behavior Notes

- The server and client are the same container image. The server runs the default command, and the client overrides the command to `./target/release/traffic-client`.
- `traffic-client` runtime config from `PATCH /config` is stored in memory per replica. If you scale the client to more than one replica, a `PATCH /config` call only changes the replica that handled that request.
- For a multi-replica client pool, the cleanest way to change traffic shape is to update the Container App environment variables and let Azure roll a new revision.
- This repo emits structured Datadog-friendly logs by default and can also export DogStatsD metrics and OTLP spans when shared telemetry endpoints are configured.

## Telemetry Model in Azure

This Azure deployment is Datadog-only, but there are two different telemetry layers:

1. Azure platform telemetry

- Use Azure Native Datadog to bring Azure resource metrics and logs into Datadog for Container Apps, PostgreSQL, Redis, and related resources.

2. Application telemetry

- Set `TELEMETRY_ENABLED=true` and `TELEMETRY_PROVIDER=datadog` on both `analytics-server` and `traffic-client` so they emit structured JSON activity logs to stdout.
- Set `TELEMETRY_DOGSTATSD_ENDPOINT` if you want `fast-telemetry` metrics forwarded to an existing shared Datadog Agent or DogStatsD-compatible endpoint.
- Set `TELEMETRY_OPENTELEMETRY_ENDPOINT` if you want the app's inbound and outbound HTTP spans exported over OTLP to an existing shared Datadog Agent or collector.

In other words: this guide gives you a clean Azure deployment plus Datadog-based cloud observability, and you can opt into shared-endpoint metrics and spans without putting a Datadog Agent in every container.

## Prerequisites

- An Azure subscription
- The Azure CLI installed and authenticated with `az login`
- A Datadog organization
- Permission to create resource groups, Container Apps, ACR, PostgreSQL, Redis, and Datadog resources

This guide uses the easiest demo-first path:

- public Azure endpoints for PostgreSQL and Redis
- ACR admin credentials for image pull
- optional external ingress on `traffic-client` if you want to use `/config` directly

That keeps the first deployment simple. A hardening checklist is included at the end.

Before you push an image or roll a new Azure revision, run:

```bash
cargo test --lib --bins
cargo test --test runtime_coverage -- --test-threads=1
```

That test gate boots the real server and client binaries locally and verifies the core health, config, validation, read, and write paths before you ship a new Azure revision.

## 1. Set Shell Variables

```bash
export RG=rg-analytics-demo
export LOCATION=eastus2
export ACA_ENV=analytics-demo-env
export ACR=analyticsdemoreg12345
export IMAGE=analytics-demo
export IMAGE_TAG=v1

export SERVER_APP=analytics-server
export CLIENT_APP=traffic-client

export PG_SERVER=analytics-demo-pg
export PG_DB=analytics
export PG_USER=analyticsadmin
export PG_PASSWORD='ChangeMe-Strong-Password-123!'

export REDIS_NAME=analytics-demo-redis

export TELEMETRY_ENV=demo
export TELEMETRY_VERSION=0.1.0
```

Notes:

- `ACR` must be globally unique and lowercase.
- Pick an Azure region that supports Container Apps, PostgreSQL, Redis, and your Datadog setup.

## 2. Create the Resource Group and Registry

```bash
az group create \
  --name "$RG" \
  --location "$LOCATION"

az acr create \
  --name "$ACR" \
  --resource-group "$RG" \
  --location "$LOCATION" \
  --sku Basic \
  --admin-enabled true
```

Build and push the demo image directly in Azure:

```bash
az acr build \
  --registry "$ACR" \
  --image "$IMAGE:$IMAGE_TAG" \
  --file examples/analytics-demo/Dockerfile \
  examples/analytics-demo
```

Capture the registry connection details:

```bash
export ACR_LOGIN_SERVER="$(az acr show --name "$ACR" --query loginServer -o tsv)"
export ACR_USERNAME="$(az acr credential show --name "$ACR" --query username -o tsv)"
export ACR_PASSWORD="$(az acr credential show --name "$ACR" --query 'passwords[0].value' -o tsv)"
```

## 3. Create the Container Apps Environment

```bash
az containerapp env create \
  --name "$ACA_ENV" \
  --resource-group "$RG" \
  --location "$LOCATION"
```

This is the shared environment for `analytics-server` and `traffic-client`.

## 4. Provision PostgreSQL

For the easiest demo deployment, use a small public Flexible Server that allows Azure-hosted workloads to connect.

```bash
az postgres flexible-server create \
  --resource-group "$RG" \
  --name "$PG_SERVER" \
  --location "$LOCATION" \
  --admin-user "$PG_USER" \
  --admin-password "$PG_PASSWORD" \
  --sku-name Standard_B1ms \
  --tier Burstable \
  --version 16 \
  --storage-size 32 \
  --public-access 0.0.0.0 \
  --yes

az postgres flexible-server db create \
  --resource-group "$RG" \
  --server-name "$PG_SERVER" \
  --database-name "$PG_DB"
```

Set the hostname used by the app:

```bash
export PG_HOST="${PG_SERVER}.postgres.database.azure.com"
```

If your generated Azure connection string uses a different username form, map that exact username into `POSTGRES_USER` when you create the Container App.

## 5. Provision Redis

The app only needs a standard Redis endpoint with TLS and a password.

The quickest Azure CLI path is:

```bash
az redis create \
  --resource-group "$RG" \
  --name "$REDIS_NAME" \
  --location "$LOCATION" \
  --sku Basic \
  --vm-size c0
```

Capture the hostname and key:

```bash
export REDIS_HOST="$(az redis show --resource-group "$RG" --name "$REDIS_NAME" --query hostName -o tsv)"
export REDIS_KEY="$(az redis list-keys --resource-group "$RG" --name "$REDIS_NAME" --query primaryKey -o tsv)"
export REDIS_URL="rediss://:${REDIS_KEY}@${REDIS_HOST}:6380"
```

If your organization standardizes on Azure Managed Redis instead of `az redis create`, keep the same application contract: provide a TLS-enabled Redis URL in `REDIS_URL`.

## 6. Link Azure to Datadog

Use Azure Native Datadog before you deploy the apps:

- Create or link a Datadog resource in Azure
- Connect it to the subscription or resource group that will hold this demo
- Enable the Azure log and metric forwarding you want in Datadog

This is the easiest way to get Azure platform telemetry for Container Apps, PostgreSQL, Redis, and related resources without running Prometheus or Grafana.

Recommended app env vars for the demo:

- `TELEMETRY_ENABLED=true`
- `TELEMETRY_PROVIDER=datadog`
- `TELEMETRY_ENV=$TELEMETRY_ENV`
- `TELEMETRY_VERSION=$TELEMETRY_VERSION`
- `TELEMETRY_SERVICE=analytics-server` or `traffic-client`
- `TELEMETRY_DOGSTATSD_ENDPOINT=<shared-agent-host:8125>` when DogStatsD metrics are enabled
- `TELEMETRY_OPENTELEMETRY_ENDPOINT=http://<shared-agent-or-collector>:4318` when OTLP span export is enabled

Those env vars do not require a per-container Datadog Agent. They make the demo emit structured logs by default and optionally ship DogStatsD metrics and OTLP spans to a shared telemetry endpoint.

## 7. Deploy `analytics-server`

Create the server as an externally reachable Container App:

```bash
az containerapp create \
  --name "$SERVER_APP" \
  --resource-group "$RG" \
  --environment "$ACA_ENV" \
  --image "$ACR_LOGIN_SERVER/$IMAGE:$IMAGE_TAG" \
  --registry-server "$ACR_LOGIN_SERVER" \
  --registry-username "$ACR_USERNAME" \
  --registry-password "$ACR_PASSWORD" \
  --ingress external \
  --target-port 3000 \
  --transport auto \
  --cpu 0.5 \
  --memory 1.0Gi \
  --min-replicas 1 \
  --max-replicas 3 \
  --secrets pgpwd="$PG_PASSWORD" redisurl="$REDIS_URL" \
  --env-vars \
    REDIS_ENABLED=true \
    REDIS_URL=secretref:redisurl \
    POSTGRES_ENABLED=true \
    POSTGRES_HOST="$PG_HOST" \
    POSTGRES_PORT=5432 \
    POSTGRES_USER="$PG_USER" \
    POSTGRES_PASSWORD=secretref:pgpwd \
    POSTGRES_DB_NAME="$PG_DB" \
    INTERNAL_WORKLOAD_ENABLED=false \
    BIND_ADDRESS=0.0.0.0:3000 \
    TELEMETRY_ENABLED=true \
    TELEMETRY_PROVIDER=datadog \
    TELEMETRY_SERVICE=analytics-server \
    TELEMETRY_ENV="$TELEMETRY_ENV" \
    TELEMETRY_VERSION="$TELEMETRY_VERSION" \
    RUST_LOG=analytics_server=info,sqlx=warn
```

Get the public URL:

```bash
export SERVER_FQDN="$(az containerapp show --resource-group "$RG" --name "$SERVER_APP" --query properties.configuration.ingress.fqdn -o tsv)"
```

Smoke check:

```bash
curl -s "https://$SERVER_FQDN/health" | jq
curl -s "https://$SERVER_FQDN/api/v1/organizations" | jq
```

## 8. Deploy `traffic-client`

The client uses the same image but overrides the startup command.

The simplest first deployment keeps `traffic-client` externally reachable so you can call `/config` directly:

```bash
az containerapp create \
  --name "$CLIENT_APP" \
  --resource-group "$RG" \
  --environment "$ACA_ENV" \
  --image "$ACR_LOGIN_SERVER/$IMAGE:$IMAGE_TAG" \
  --registry-server "$ACR_LOGIN_SERVER" \
  --registry-username "$ACR_USERNAME" \
  --registry-password "$ACR_PASSWORD" \
  --command ./target/release/traffic-client \
  --ingress external \
  --target-port 3100 \
  --transport auto \
  --cpu 0.5 \
  --memory 1.0Gi \
  --min-replicas 1 \
  --max-replicas 1 \
  --env-vars \
    CLIENT_NAME=traffic-client \
    CLIENT_PROFILE=balanced \
    CLIENT_BIND_ADDRESS=0.0.0.0:3100 \
    TARGET_BASE_URL=http://analytics-server \
    QUERIES_PER_SECOND=180 \
    EVENTS_PER_SECOND=40 \
    QUERY_WORKERS=8 \
    EVENT_WORKERS=4 \
    ORGANIZATION_FETCH_LIMIT=100 \
    ORGANIZATION_REFRESH_INTERVAL_SECONDS=30 \
    REQUEST_TIMEOUT_MS=5000 \
    TELEMETRY_ENABLED=true \
    TELEMETRY_PROVIDER=datadog \
    TELEMETRY_SERVICE=traffic-client \
    TELEMETRY_ENV="$TELEMETRY_ENV" \
    TELEMETRY_VERSION="$TELEMETRY_VERSION" \
    RUST_LOG=traffic_client=info,reqwest=warn
```

Why `TARGET_BASE_URL=http://analytics-server` works:

- Both apps live in the same Container Apps environment.
- Azure Container Apps supports service-to-service calls by app name inside the same environment.

Get the client URL:

```bash
export CLIENT_FQDN="$(az containerapp show --resource-group "$RG" --name "$CLIENT_APP" --query properties.configuration.ingress.fqdn -o tsv)"
```

Smoke check:

```bash
curl -s "https://$CLIENT_FQDN/health" | jq
curl -s "https://$CLIENT_FQDN/config" | jq
```

## 9. Scale `traffic-client` Up or Down

If you want a fixed number of client replicas, pin `min-replicas` and `max-replicas` to the same value.

Examples:

```bash
# Pin the client pool to 4 replicas
az containerapp update \
  --resource-group "$RG" \
  --name "$CLIENT_APP" \
  --min-replicas 4 \
  --max-replicas 4

# Shrink back to 1 replica
az containerapp update \
  --resource-group "$RG" \
  --name "$CLIENT_APP" \
  --min-replicas 1 \
  --max-replicas 1
```

You can also change the client-wide traffic profile by updating env vars:

```bash
az containerapp update \
  --resource-group "$RG" \
  --name "$CLIENT_APP" \
  --set-env-vars \
    CLIENT_PROFILE=write-heavy \
    QUERIES_PER_SECOND=240 \
    EVENTS_PER_SECOND=120 \
    QUERY_WORKERS=12 \
    EVENT_WORKERS=8
```

This creates a new revision with a consistent config across the pool, which is usually a better fit than calling `/config` once the app has multiple replicas.

## 10. Using the Client `/config` API

If `traffic-client` is running with exactly one replica, you can change its live traffic mix directly:

```bash
curl -s -X PATCH "https://$CLIENT_FQDN/config" \
  -H 'content-type: application/json' \
  -d '{
    "queries_per_second": 220,
    "events_per_second": 90,
    "query_distribution": {
      "storefront": 30,
      "catalog": 25,
      "dashboard": 15,
      "cart_detail": 10
    },
    "write_distribution": {
      "cart_create": 30,
      "cart_add_item": 30,
      "cart_checkout": 20,
      "event_ingest": 20
    },
    "event_distribution": {
      "page_view": 45,
      "click": 25,
      "conversion": 15,
      "sign_up": 10,
      "purchase": 5
    }
  }' | jq
```

For more than one replica, treat `/config` as a per-replica debug tool, not as the primary control plane.

## 11. Operational Checks

Useful commands:

```bash
az containerapp logs show \
  --resource-group "$RG" \
  --name "$SERVER_APP" \
  --follow

az containerapp logs show \
  --resource-group "$RG" \
  --name "$CLIENT_APP" \
  --follow
```

Useful HTTP checks:

```bash
curl -s "https://$SERVER_FQDN/health" | jq
curl -s "https://$SERVER_FQDN/api/v1/organizations" | jq
curl -s "https://$CLIENT_FQDN/health" | jq
curl -s "https://$CLIENT_FQDN/config" | jq
```

For a live validation pass, run the standalone validator:

```bash
cargo run --manifest-path examples/analytics-demo/Cargo.toml --bin runtime-validator -- \
  --server-base-url "https://$SERVER_FQDN" \
  --client-base-url "https://$CLIENT_FQDN" \
  --require-postgres true \
  --require-redis true
```

That validates the live HTTP surface, including health, storefront/catalog/dashboard reads, event ingest, and the cart/create/add-item/checkout flow.

Datadog-side checks:

- confirm the Azure subscription is linked to your Datadog resource
- confirm Container Apps, PostgreSQL, and Redis resource telemetry is visible in Datadog
- confirm the app logs contain the structured fields emitted by the demo, such as `event_name`, `status`, `tags`, `latency_us`, and `error_type`

## 12. Production Hardening After the First Demo Deployment

Once the easiest path is working, tighten it:

- move Container Apps, PostgreSQL, and Redis onto private networking
- replace ACR admin credentials with managed identity pull
- move database and Redis secrets into Key Vault-backed Container App secrets
- disable external ingress on `traffic-client` unless you actively need `/config`
- split `traffic-client` into multiple client pools if you want different long-lived traffic personalities
- add Datadog in-container or sidecar instrumentation if you want traces, direct log collection, and custom metrics from the app containers

## References

- Azure Container Apps overview: https://learn.microsoft.com/en-us/azure/container-apps/overview
- Communicate between container apps in Azure Container Apps: https://learn.microsoft.com/en-us/azure/container-apps/connect-apps
- Azure Container Registry introduction: https://learn.microsoft.com/en-us/azure/container-registry/container-registry-intro
- Azure Database for PostgreSQL overview: https://learn.microsoft.com/en-us/azure/postgresql/flexible-server/overview
- Azure Managed Redis overview: https://learn.microsoft.com/en-us/azure/redis/overview
- Azure Native Integrations overview: https://learn.microsoft.com/en-us/azure/partner-solutions/overview
- Datadog on Azure Native Integrations: https://learn.microsoft.com/en-us/azure/partner-solutions/datadog/
- Datadog Azure Container Apps docs: https://docs.datadoghq.com/serverless/azure_container_apps/
