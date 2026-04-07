# ADAM Demo — Autonomous Database Activity Monitor

ADAM simulates real-world **data silos** across industry verticals, generating continuous query traffic against multiple databases through Eden endpoints. Each vertical represents a different customer type with domain-specific datasets pulled from HuggingFace.

## Quick Start

```bash
# 1. Configure
cp .env.example .env
# Edit .env: set VERTICAL and optional API keys

# 2. Pre-download datasets (~10 GB total)
cd init
pip install datasets pandas pyarrow tqdm huggingface_hub
python3 download_datasets.py          # all verticals
python3 download_datasets.py finance  # or just one
cd ..

# 3. Optionally pre-download curated document corpora
python3 init/download_datasets.py documents

# 4. Start databases + load data (pick a vertical)
make up VERTICAL=finance             # banking: fraud, credit cards, trading, SEC filings
make up VERTICAL=stonebreaker        # benchmark: 5 retail-backed sources, exact two-source tasks
make up VERTICAL=tech                # SaaS: network security, billing, CVEs
make up VERTICAL=healthcare          # clinical: patients, conditions, labs, billing
make up VERTICAL=insurance           # risk: policies, accidents, claims
make up VERTICAL=bird                # benchmark: BIRD SQLite imported into Postgres
make up                              # default: retail e-commerce

# 5. Watch data loading progress
make logs VERTICAL=finance

# 6. Start the query engine (requires Eden API + Docker running)
make app VERTICAL=finance

# 7. Check status
make status VERTICAL=finance         # service health
curl localhost:3000/health            # app health
curl localhost:3000/status            # JSON: QPS, errors, endpoints
curl localhost:3000/metrics           # Prometheus metrics
```

### Running with Docker Compose directly

If you prefer not to use `make`:

```bash
# Start a vertical (e.g., tech)
docker compose -f docker-compose.tech.yml up -d

# Watch data loading
docker compose -f docker-compose.tech.yml logs -f data-init

# Start the query app (after data is loaded)
docker compose -f docker-compose.tech.yml --profile app up -d adam-app

# Tail app logs
docker compose -f docker-compose.tech.yml logs -f adam-app

# Stop everything
docker compose -f docker-compose.tech.yml --profile app down

# Stop and delete all data volumes
docker compose -f docker-compose.tech.yml --profile app down -v
```

For retail, use `docker-compose.yml` (the default).

### BIRD dataset prep

The `bird` vertical expects a local BIRD benchmark checkout or extracted dataset
under `examples/adam-demo/init/data/bird`.

If that directory is empty, `make up VERTICAL=bird` will automatically download
the official BIRD archive once, extract it into that folder, and reuse the
cached files on later runs.

Supported layouts:

```text
init/data/bird/dev.json
init/data/bird/dev_databases/<db_id>/<db_id>.sqlite
```

or the upstream repo-style layout:

```text
init/data/bird/data/dev.json
init/data/bird/data/dev_databases/<db_id>/<db_id>.sqlite
```

Then start the demo with:

```bash
BIRD_DB_ID=california_schools make up VERTICAL=bird
make app VERTICAL=bird
```

The `bird` app profile also supports the same optional external-service env vars
as the other ADAM verticals, including `TAVILY_API_KEY`, `ERASER_API_KEY`, the
Azure ARM credentials used for `adam_azure`, and the GitLab token used for
`adam_gitlab`.

If `BIRD_DB_ID` is omitted, the demo defaults to `california_schools` when it is
available in the selected split; otherwise it falls back to the database with
the most questions. The loader writes a validated query manifest to
`init/data/bird/validated_queries.json`. See [BIRD.md](./BIRD.md) for the
expected staging layout.

---

## Verticals

Each vertical deploys **multiple databases of the same type** to simulate real data silos across departments/teams. This is exactly the problem Eden solves — unified access across fragmented data.

## Curated Documents

You can also pre-download a lightweight bundle of raw document datasets with:

```bash
python3 init/download_datasets.py documents
```

This currently stages:

- `huggingface/policy-docs` — public policy PDFs from Hugging Face
- `huggingface-legal/takedown-notices` — takedown notices plus manifest metadata

The files are saved under `examples/adam-demo/init/data/documents/` with a `manifest.csv` so we can wire them into future document-focused silos without re-curating sources.

---

### Retail (default)

E-commerce platform with product catalog, user behavior, and reviews.

| System | What's in it | Key tables/collections |
|---|---|---|
| **Postgres** | 500K users, brands, 250K marketplace items, 1.8M marketplace events, orders, invoices, payments, coupons, categories | `users`, `brands`, `marketplace_items`, `marketplace_events`, `orders`, `order_items`, `invoices`, `payments` |
| **MongoDB** | 250K retail product catalog (brand_id, category, price), retail shopping events | `retail_items`, `retail_events` |
| **Redis** | Offer engagement cache, leaderboards (top products, top users), session data, inventory alerts, real-time stats | `stats:*`, `leaderboard:*`, `alerts:*`, `offers:*` |
| **ClickHouse** | Marketplace event analytics, clickstream (page views → cart → purchase), purchase events, daily revenue rollups, conversion funnels | `analytics.marketplace_events`, `analytics.clickstream_events`, `analytics.purchase_events`, `analytics.revenue_daily` |
| **Weaviate** | Review text embeddings with user_id and rating for semantic search | `Review` class |

**Cross-DB keys:** `user_id`, `brand_id`, `order_id` — shared across all systems

**Source:** [t-tech/T-ECD](https://huggingface.co/datasets/t-tech/T-ECD) (~574 MB)

**Example questions to ask:**
- "Which brands have the highest revenue but lowest review ratings?"
- "Show me the full user journey for our top 10 spenders — what did they browse, add to cart, and purchase?"
- "What products are frequently viewed but rarely purchased? What's the cart abandonment rate by category?"
- "Which marketplace items from T-ECD are trending in click events but not yet generating orders?"
- "Compare real-time Redis leaderboard rankings with actual ClickHouse revenue — are they consistent?"

---

### Stonebreaker

Stonebraker-style benchmark mode over the retail connector ecosystem.

| System | What's in it | Role in benchmark |
|---|---|---|
| **Postgres** | Marketplace OLTP tables and synthetic order history | Structured evidence source |
| **MongoDB** | Retail item catalog and retail events | Document evidence source |
| **Redis** | Real-time leaderboards, sessions, alerts, and counters | Operational evidence source |
| **ClickHouse** | Purchase analytics and clickstream aggregates | OLAP evidence source |
| **Weaviate** | Review embeddings plus a benchmark document corpus | Semantic evidence source |

**Method:** every task requires exactly 2 of these 5 sources, leaving 3 distractors available.

**Task manifest:** [`benchmarks/stonebreaker.tasks.json`](./benchmarks/stonebreaker.tasks.json)

**Notes:**
- `stonebreaker` reuses the retail dataset and synthetic retail generator.
- It also loads a small document corpus from `G4KMU/t2-ragbench` into Weaviate for document-style retrieval.
- During `data-init` it also materializes the raw document corpus into `init/data/stonebreaker/localfs/` as markdown files.
- If you set `STONEBREAKER_LOCALFS_ROOT` to that directory as seen by Eden, the app also registers an auxiliary `localfs` endpoint for the raw files.
- Unlike the default retail run, it enables ClickHouse by default so the 5-source benchmark universe stays stable.
- The benchmark workload spec is described in [STONEBREAKER.md](./STONEBREAKER.md).

---

### Tech / SaaS

Tech company with SecOps, billing, product analytics, and vulnerability management.

| System | What's in it | Key tables/collections |
|---|---|---|
| **Postgres #1** (SecOps) | 2.5M network flows from UNSW-NB15 (source/dest IPs, protocols, attack categories, packet sizes, jitter), 50K security alerts generated from attack flows | `network_flows`, `security_alerts` |
| **Postgres #2** (Finance) | Synthetic SaaS billing: orgs (up to 500K), users, plans, subscriptions, invoices, payments, API keys, daily API usage | `organizations`, `users`, `plans`, `subscriptions`, `invoices`, `payments`, `api_keys`, `api_usage_daily` |
| **ClickHouse** | 50M+ user behavior events (view/cart/purchase) with `org_id` derived from user_id for billing correlation. Funnel summaries. | `analytics.user_events`, `analytics.funnel_daily` |
| **MongoDB** | 300K CVE vulnerability records (1999-2025) with CVE ID, CWE ID, CVSS scores, severity, description | `cves` collection |
| **Redis** | 50K sessions (with IP, org_id, user_agent), rate limits, feature flags, IP→org reverse index, org→session counts, MRR leaderboard | `session:*`, `ip_orgs:*`, `org_sessions:*`, `feature:*`, `rate_limit:*`, `leaderboard:org_mrr` |
| **Weaviate** | CVE description embeddings for semantic vulnerability search | `Vulnerability` class |

**Cross-DB keys:** `org_id` (billing ↔ ClickHouse ↔ Redis), IP address (network flows → sessions → orgs)

**Example questions to ask:**
- "We're seeing Backdoor attacks from 10.1.1.x — which of our customer organizations have active sessions from those IPs? What's their ARR?"
- "Which organizations have the highest API usage but the most past-due invoices?"
- "Show me the top 10 attack categories hitting our network right now, and find CVEs matching those attack patterns"
- "What's our product engagement (views, carts, purchases) broken down by organization size tier?"
- "Which feature flags are enabled, and how many active sessions do our enterprise customers have?"
- "Find all critical CVEs related to authentication bypass — are any of those attack types showing up in our network flows?"

---

### Finance / Banking

Financial institution with fraud detection, credit cards, trading, and compliance.

| System | What's in it | Key tables/collections |
|---|---|---|
| **Postgres #1** (Core Banking) | 21M mobile money transactions (PaySim-style): CASH_IN, CASH_OUT, DEBIT, PAYMENT, TRANSFER with fraud labels, sender/receiver accounts, balances before/after | `transactions` |
| **Postgres #2** (Credit) | 1.85M credit card transactions: merchant, category, amount, cardholder demographics (name, gender, city, state, job, DOB), geo coordinates, fraud labels | `credit_transactions` |
| **ClickHouse** | 10M stock OHLCV bars from TroveLedger: symbol, trade_time, open/high/low/close, volume | `analytics.stock_bars`, `analytics.daily_ohlcv` |
| **MongoDB** | 245K SEC 10-K annual filings with full text, company name/CIK, word count, filing year | `sec_filings` collection |
| **Redis** | 100K customer profiles linking bank accounts to credit cards, fraud scores (0-100), account balances, reverse indexes (account→customer, cc→customer), top transactor leaderboard | `customer:*`, `fraud_score:*`, `balance:*`, `account_customer:*`, `cc_customer:*`, `leaderboard:top_transactors` |
| **Weaviate** | SEC filing text embeddings for compliance topic search | `ComplianceDocument` class |

**Cross-DB keys:** `customer:{id}` in Redis bridges `name_orig` (core banking) ↔ `cc_num` (credit scoring) ↔ `fraud_score`

**Example questions to ask:**
- "Show me the top 20 largest fraudulent transactions — then look up those accounts' credit card fraud history and real-time fraud scores"
- "Which states have the highest credit card fraud rates, and which merchants are most frequently involved?"
- "Find SEC filings that mention 'cybersecurity breach' or 'data loss' — which companies filed those?"
- "What's the most volatile stock today? Show me the intraday OHLCV pattern and volume-weighted average price"
- "Compare the fraud rate across TRANSFER vs CASH_OUT transaction types — are the balance-change patterns different for fraudulent vs legitimate?"
- "Look up customer #42 — show their bank account, credit card, fraud score, and recent transaction history across both systems"

---

### Healthcare

Hospital system with patient records, clinical data, lab results, and claims.

| System | What's in it | Key tables/collections |
|---|---|---|
| **Postgres #1** (EHR) | 575K patients (demographics, address, income, healthcare expenses), 5M encounters (class, provider, payer, claim cost, reason) | `patients`, `encounters` |
| **Postgres #2** (Billing) | Payer organizations (name, coverage amounts, revenue, member months), claims linked to encounters | `payers`, `claims` |
| **MongoDB #1** (Clinical) | 3M conditions (patient, encounter, diagnosis code, description), 3M medications (patient, encounter, drug, dosage) | `conditions`, `medications` collections |
| **MongoDB #2** (Lab) | 5M observations/lab results (patient, encounter, category, code, value, units — vital signs, lab panels) | `observations` collection |
| **ClickHouse** | Encounter event analytics: encounter_id, patient_id, class, cost, payer coverage, event_day. Daily cost summaries. | `analytics.encounter_events`, `analytics.daily_cost_summary` |
| **Redis** | Bed availability by department (Emergency, ICU, Pediatrics, etc.), patient priority alerts, critical lab alerts, aggregate stats | `beds:*`, `alerts:patient_priority`, `alerts:critical_labs`, `stats:*` |
| **Weaviate** | Clinical condition description embeddings with patient_id for semantic diagnosis search | `ClinicalCondition` class |

**Cross-DB key:** `patient_id` (UUID) — present in every single system

**Example questions to ask:**
- "Find patients with healthcare expenses over $50K — how many conditions do they have, and what are their most common diagnoses?"
- "Which encounter types cost the most? Break it down by payer — who's covering the most?"
- "Show me patients with 3+ chronic conditions and their medication lists — are there any polypharmacy risks?"
- "What are the most common diagnoses for patients over 65? How do their lab observation volumes compare?"
- "Find conditions similar to 'diabetes' in the clinical knowledge base — which patients have those conditions and what are their healthcare expenses?"
- "How many ICU beds are available right now? What's the daily cost trend for emergency encounters this month?"
- "Compare coverage gaps: which payers have the highest uncovered encounter ratio?"

---

### Insurance

Insurance company with policy management, risk scoring, and claims.

| System | What's in it | Key tables/collections |
|---|---|---|
| **Postgres #1** (Policy) | 678K motor liability policies (freMTPL2): exposure, vehicle power/age/brand/fuel, driver age, bonus-malus, claim count, French region mapped to US state | `policies`, `claims` |
| **Postgres #2** (Risk) | 595K driver risk prediction records (Porto Seguro schema): 57 anonymized features (individual, registration, car, calculated), binary target (filed claim or not) | `driver_risk` |
| **ClickHouse** | 2.85M US traffic accidents: severity (1-4), state, city, weather, temperature, humidity, visibility, wind speed, distance, day/night, traffic signals, junctions | `analytics.accidents`, `analytics.daily_severity` |
| **MongoDB** | Same 2.85M accidents as rich documents with full descriptions, street addresses, geo coordinates, weather conditions | `accidents` collection |
| **Redis** | 50K policy status cache, 10K claim statuses with assigned agents, policy-claim bidirectional mapping, per-state policy counts, agent resolution leaderboard, loss ratio stats | `policy:*`, `claim:*`, `policy_claims:*`, `claim_policy:*`, `state_policies:*`, `leaderboard:agent_resolutions` |
| **Weaviate** | Accident description embeddings for semantic similarity search | `AccidentReport` class |

**Cross-DB keys:** `us_state` (policies ↔ accidents), `claim_id` → `policy_id` (Redis mapping)

**Example questions to ask:**
- "Which US states have both high policy claim rates AND high accident severity? Is there a correlation?"
- "Show me claim #1 — which policy is it linked to, what's the agent assigned, and find similar historical accidents"
- "What's the claim frequency for young drivers (under 25) vs experienced drivers? How does bonus-malus score affect it?"
- "Which weather conditions cause the most severe accidents? Compare rain vs fog vs snow vs clear"
- "Find the worst intersections — where do severity 4 accidents cluster geographically?"
- "Compare our risk model predictions (target=1 in driver_risk) against actual claim rates (claim_nb in policies) — is the model accurate?"
- "Search for accidents similar to 'multi-vehicle highway collision in rain' — what states and severity levels come back?"
- "What's our current loss ratio? Which agents are resolving the most claims?"

---

### BIRD

Text-to-SQL benchmark mode using one imported BIRD SQLite database replayed as
Postgres queries through Eden.

| System | What's in it | Key tables/collections |
|---|---|---|
| **Postgres** | One selected BIRD benchmark database imported from SQLite, plus a validated subset of benchmark SQL queries chosen from `dev.json` | Tables vary by `db_id` |

**Source:** [AlibabaResearch/DAMO-ConvAI/tree/main/bird](https://github.com/AlibabaResearch/DAMO-ConvAI/tree/main/bird)

**Notes:**
- Set `BIRD_DB_ID=<db_id>` to choose a specific benchmark database.
- The loader validates queries against Postgres and only replays the subset that succeeds after import.
- The generated manifest lives at `init/data/bird/validated_queries.json`.

**Example prompts to narrate the demo:**
- "Replay a few benchmark questions from the BIRD `california_schools` database through Eden."
- "Show which BIRD gold SQL queries validated successfully for this imported database."
- "How many public tables were imported for the selected BIRD database?"

---

## Architecture

```
                    +-----------+
                    | Eden API  |
                    +-----+-----+
                          |
                    +-----+-----+
                    | ADAM App   |  Rust — registers endpoints, generates query traffic
                    +-----+-----+
                          |
          +-------+-------+-------+-------+-------+
          |       |       |       |       |       |
        PG #1   PG #2    CH    Mongo   Redis  Weaviate
         |       |       |       |       |       |
       [silo1] [silo2] [silo3] [silo4] [silo5] [silo6]
```

The Rust app:
1. Registers each database silo as an Eden endpoint
2. Creates demo users with RBAC roles
3. Spawns workers that send domain-specific queries at configurable QPS
4. Exposes `/metrics` (Prometheus), `/health`, `/status`

## Query Counts

| Vertical | Single-DB Queries | Cross-DB Groups | Total Data (massive) |
|---|---|---|---|
| Retail | 57 | 5 | ~70M rows |
| Tech | 46 | 3 | ~338M rows |
| Finance | 48 | 3 | ~34M rows |
| Healthcare | 45 | 3 | ~19M rows |
| Insurance | 44 | 3 | ~7M rows |

## Make Commands

```bash
make help                        # Show all commands
make list                        # List available verticals
make up VERTICAL=finance         # Start databases + data init
make app VERTICAL=finance        # Start query engine
make down VERTICAL=finance       # Stop containers
make reset VERTICAL=finance      # Stop + delete volumes
make logs VERTICAL=finance       # Tail data loading logs
make app-logs VERTICAL=finance   # Tail query app logs
make app-demo VERTICAL=finance   # Low QPS for live demos (20 QPS)
make app-high VERTICAL=finance   # High QPS stress test (500 QPS)
make status VERTICAL=finance     # Check service status
```

## Pre-downloading Datasets

Datasets are downloaded from HuggingFace and stored as Parquet files in `init/data/<vertical>/`. Pre-downloading avoids needing internet access in the Docker init container.

```bash
cd init
pip install datasets pandas pyarrow tqdm huggingface_hub
python3 download_datasets.py                  # All verticals (~10 GB)
python3 download_datasets.py retail tech      # Specific verticals
```

| Vertical | Files | Size |
|---|---|---|
| retail | 9 | 574 MB |
| tech | 4 | 2.9 GB |
| finance | 4 | 6.0 GB |
| healthcare | 6 | 580 MB |
| insurance | 3 | 313 MB |

## Configuration

Copy `.env.example` to `.env` and configure:

| Variable | Description | Default |
|---|---|---|
| `VERTICAL` | Industry vertical | `retail` |
| `SCALE` | Data scale: `demo`, `small`, `medium`, `large`, `massive` | `demo` |
| `EDEN_API_URL` | Eden API URL | `http://localhost:8000` |
| `EDEN_ORG_ID` | Eden organization ID | `adam-demo` |
| `QUERIES_PER_SECOND` | Total QPS across all databases | `100` |
| `MAX_WORKERS` | Maximum concurrent query workers | `50` |
| `HTTP_TIMEOUT` | HTTP request timeout (seconds) | `30` |
| `WEAVIATE_LIMIT` | Max embeddings to load into Weaviate | `10000` |
| `STONEBREAKER_LOCALFS_ROOT` | Optional Stonebreaker raw-doc localfs root, as seen by Eden | unset |

### External Services (optional, for ADAM live demos)

| Variable | Description |
|---|---|
| `TAVILY_API_KEY` | Tavily web search ([tavily.com](https://tavily.com)) |
| `OPENAI_API_KEY` | LLM via OpenAI ([platform.openai.com](https://platform.openai.com/)) |
| `OPENAI_MODEL` | OpenAI model identifier (default: `gpt-5.4-nano`) |
| `OPENROUTER_API_KEY` | LLM via OpenRouter ([openrouter.ai](https://openrouter.ai)) |
| `OPENROUTER_MODEL` | Model identifier (default: `anthropic/claude-sonnet-4`) |
| `GOOGLE_WORKSPACE_ACCESS_TOKEN` | Google Workspace / Google APIs via HTTP |
| `AZURE_APP_ID` | Azure service principal app/client ID for Azure Resource Manager |
| `AZURE_DISPLAY_NAME` | Optional Azure service principal label for the `adam_azure` description |
| `AZURE_PASSWORD` | Azure service principal client secret |
| `AZURE_TENANT` | Azure tenant ID |
| `AZURE_SUBSCRIPTION_ID` | Azure subscription ID; scopes the `adam_azure` base URL |
| `AZURE_API_BASE_URL` | Azure Resource Manager base URL (default: `https://management.azure.com`) |
| `GITLAB_ACCESS_TOKEN` | GitLab personal access token for the `adam_gitlab` HTTP endpoint |
| `GITLAB_API_BASE_URL` | GitLab API base URL (default: `https://gitlab.com/api/v4`) |
| `DD_API_KEY` | Datadog monitoring |
| `ERASER_API_KEY` | Eraser diagram generation |

When the Azure vars are present, ADAM registers `adam_azure` as a subscription-scoped
HTTP endpoint against Azure Resource Manager and mints a bearer token from the
service-principal credentials at startup.

When the GitLab vars are present, ADAM registers `adam_gitlab` as an HTTP
endpoint against the GitLab REST API using the `PRIVATE-TOKEN` header.

## Database RBAC (Read / Write / Admin)

Every database silo has **3 access tiers** with separate credentials. The ADAM app registers **1 Eden endpoint per silo** with separate connection tiers, so you can demonstrate access control at the database level.

### Credentials

| Role | Username | Password | Access Level |
|------|----------|----------|-------------|
| **Reader** | `reader` | `reader_pass` | SELECT only (Postgres/ClickHouse), read role (MongoDB), `@read` commands (Redis) |
| **Writer** | `writer` | `writer_pass` | SELECT + INSERT + UPDATE + DELETE (Postgres/ClickHouse), readWrite role (MongoDB), `@read` + `@write` (Redis) |
| **Admin** | `eden` | `eden` | Full superuser access (all databases) |

### Eden Endpoint Connection Tiers

Each database silo is registered as **1 Eden endpoint with 4 connection tiers**:

| Connection Tier | User | Purpose |
|---|---|---|
| `read_conn` | `reader:reader_pass` | SELECT-only queries (used by workers) |
| `write_conn` | `writer:writer_pass` | INSERT, UPDATE, DELETE operations |
| `admin_conn` | `eden:eden` | DDL, schema changes, grants |
| `system_conn` | `eden:eden` | Eden metadata collection, catalog queries, performance analysis |

For example, the Tech vertical registers **6 endpoints** (one per silo), each with all 4 tiers:
- `tech_network_security` (Postgres — SecOps)
- `tech_saas_billing` (Postgres — SaaS)
- `tech_user_events` (ClickHouse)
- `tech_cve` (MongoDB)
- `tech_sessions` (Redis)
- `tech_logs` (Weaviate)

### Database-Specific RBAC Details

**Postgres** — Uses SQL roles created via `init/schemas/rbac_postgres.sql`:
```sql
-- Reader: SELECT only
GRANT SELECT ON ALL TABLES IN SCHEMA public TO reader;
-- Writer: SELECT + DML
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO writer;
```

**ClickHouse** — Uses ClickHouse native users via `init/schemas/rbac_clickhouse.sql`:
```sql
CREATE USER reader IDENTIFIED BY 'reader_pass';
GRANT SELECT ON analytics.* TO reader_role;
```

**MongoDB** — Uses MongoDB roles via per-vertical `rbac_mongo.js` scripts:
```javascript
db.createUser({ user: 'reader', pwd: 'reader_pass', roles: [{ role: 'read', db: 'DB_NAME' }] });
db.createUser({ user: 'writer', pwd: 'writer_pass', roles: [{ role: 'readWrite', db: 'DB_NAME' }] });
```

**Redis** — Uses Redis ACLs via `init/schemas/redis_users.acl`:
```
user reader on >reader_pass ~* &* +@read +@connection +ping +info +dbsize
user writer on >writer_pass ~* &* +@read +@write +@connection +ping +info +dbsize
user default on >eden ~* &* +@all
```

### Demo: Testing RBAC

Eden automatically routes queries through the appropriate connection tier. The `system_conn` is used by Eden's metadata engine to introspect schemas, collect table statistics, and analyze query patterns — this requires superuser access to `pg_catalog`, `information_schema`, and system views.

---

## Application Users & RBAC

The app registers 18 demo users with realistic enterprise roles, org-level access tiers, and per-endpoint permissions. All users are created idempotently on startup.

### Technical Roles

| Username | Password | Role | Endpoint Permissions |
|----------|----------|------|---------------------|
| `alice.chen` | `Admin123!` | Platform Admin | Full access (org-level) |
| `bob.martinez` | `DataEng1!` | Data Engineer — Analytics | Write: PG, ClickHouse |
| `carol.nguyen` | `DataEng2!` | Data Engineer — Catalog | Write: MongoDB; Read: Weaviate |
| `dave.wilson` | `Backend1!` | Backend Dev — Orders | Write: PG, Redis |
| `eve.johnson` | `Backend2!` | Backend Dev — Users | Write: PG, Redis |
| `frank.lee` | `Analyst1!` | Data Analyst — BI | Read: PG, MongoDB, ClickHouse, Weaviate |
| `grace.kim` | `Analyst2!` | Data Analyst — Fraud | Read: PG, ClickHouse |
| `henry.patel` | `MLEng123!` | ML Engineer | Write: Weaviate; Read: PG |
| `julia.santos` | `QATest12!` | QA Engineer | Read: PG, MongoDB, Redis, ClickHouse |

### Business Roles

| Username | Password | Role | Endpoint Permissions |
|----------|----------|------|---------------------|
| `karen.wright` | `VPEng123!` | VP of Engineering | Read: PG, ClickHouse |
| `liam.oconnor` | `Product1!` | Head of Product | Read: MongoDB, Weaviate |
| `maria.garcia` | `CFO12345!` | CFO | Read: ClickHouse |
| `nathan.brooks` | `SalesD12!` | Sales Director | Read: PG, MongoDB |
| `olivia.thomas` | `Support1!` | Customer Support Lead | Read: PG, Weaviate |
| `peter.chang` | `Market12!` | Marketing Manager | Read: MongoDB |
| `rachel.moore` | `Comply12!` | Compliance Officer | Read: PG, ClickHouse |
| `ivan.contractor` | `Extern12!` | External Auditor | Org-level Read only (no endpoint grants) |
