# Spacecraft Engines — Data Engineering Final Project
## Phase 1: Analysis & Design (v2 — aligned to course rubric)

> **Why this version exists:** the rubric requires ingestion from **multiple sources**
> with **at least two flow types — batch AND near-real-time/streaming** — plus
> real-time processing and storage. The earlier single-source, batch-only, 4-day
> cut does not satisfy that. This version restores a streaming leg while staying as
> small and cheap as possible.

## 1. Objective

Build a low-cost pipeline (local or AWS) that:
1. Ingests **historical engine/launch data** (batch) from a real public API.
2. Ingests **live engine telemetry** (near-real-time/streaming) from a simulated
   sensor feed, processes it as it arrives, and stores both the raw stream and
   derived results.
3. Trains a predictive model on the batch data and runs lightweight anomaly
   detection on the stream.
4. Surfaces both in one dashboard.

Target cost: **$0** — environment decision below.

**Environment: local via `floci` (a LocalStack-compatible AWS emulator), not
real AWS.** The rubric explicitly allows "entorno local o en la nube," and
`floci` emulates S3, DynamoDB, Kinesis, **and Lambda** (with real code
execution, in Docker) behind the real `boto3`/AWS SDK — so the code genuinely
calls AWS APIs, it just points at `http://localhost:4566` instead of a real
account. This removes AWS account setup, IAM, and Budgets-alert overhead
entirely, which matters a lot on a tight day count.

(History: LocalStack was the original plan, but its Docker image now
hard-requires a free-account auth token just to boot, even for community-tier
services. `moto` (pure-Python, no Docker) was the next choice, but only
partially supports real Lambda execution. `floci` — a MIT-licensed,
zero-auth, drop-in LocalStack replacement built specifically in response to
LocalStack's auth-token change — is what's actually running: `floci start`,
no signup, no Docker-socket workarounds, and it includes a real visual
dashboard, `floci-ui`, at `localhost:4501`.)

Trade-offs, and how each is handled:
- **Athena/Glue aren't meaningfully queryable end-to-end for this project's
  purposes** → use **DuckDB** to run SQL directly over the curated Parquet
  files instead. Same "SQL analytics layer" story, no service dependency.
- **Lambda is used for real**, not simulated: `consumer.py` (streaming) and
  `ingest_lambda.py` (batch) are both deployed as actual Lambda functions
  against Floci via `infra/deploy_lambda.py` / `infra/deploy_ingest_lambda.py`
  (zip packaging, IAM role, and — for the streaming one — a Kinesis event
  source mapping so it's genuinely auto-invoked on new records, not polled).
  `producer.py` stays a plain script since Lambda's 15-minute execution cap
  doesn't fit a continuously-running telemetry generator.

## 2. Rubric alignment

| Criterion | Weight | How this design satisfies it |
|---|---|---|
| Diseño y Arquitectura | 20% | §4 architecture, batch + streaming legs, data model §6 |
| Implementación Técnica | 20% | Working ingest/transform/stream scripts + tests, GitHub repo |
| Uso de Tecnologías y Herramientas | 20% | S3, Lambda, Kinesis or SQS, DynamoDB, Athena, scikit-learn, Streamlit |
| Documentación y Presentación | 20% | README, architecture diagram, 20-min deck (§9) |
| Resultados y Validación | 10% | Model metrics (§7) + anomaly-detection validation on synthetic faults |
| Innovación y Creatividad | 10% | Realistic synthetic telemetry seeded from real engine specs; anomaly detection tied to real failure modes (§3, §7) |

## 3. Data sources (two flow types, as required)

> **2026-09-09 update:** the SpaceX API (`api.spacexdata.com`) is suffering an
> extended outage on its own backend (Cloudflare→origin SSL handshake failure,
> HTTP 525, confirmed from multiple independent networks — not a local/network
> issue on our end). Its `/rockets` endpoint was our planned source for
> engine-level specs (thrust, Isp, propellant per engine). Pivoted below.

| # | Flow type | Source | What it gives us | Access |
|---|---|---|---|---|
| 1a | **Batch** | **Launch Library 2** (`ll.thespacedevs.com/2.2.0`) | Historical launches across **all agencies** (~8,000 total), rocket family, success/failure, mission/orbit, plus rocket-level specs (total thrust, LEO/GTO capacity, cost, dimensions) via `/config/launcher/{id}`. Confirmed live and working. | REST, no key |
| 1b | **Batch (static reference)** | **Hardcoded real engine specs** (Merlin, F-1, RS-25, RD-180, Vulcain 2, Rutherford, etc.) | Per-engine thrust (SL/vac), Isp (SL/vac), propellant combo — physical constants that don't change, sourced from public manufacturer/technical documentation. No API publishes this at per-engine granularity reliably, so it's a small static table (~8-10 rows) instead of a live call. | `src/batch/engine_specs.py` |
| 2 | **Streaming / near-real-time** | **Simulated engine telemetry producer** | Synthetic sensor readings (chamber pressure, temperature, vibration, fuel flow, thrust) emitted ~1/sec per "engine", parameter ranges seeded from source 1b, with occasional injected anomalies (simulating a sensor fault or off-nominal burn) | Python producer script → stream |

Sources 1a and 1b join on **rocket family name** (e.g. "Falcon 9" → Merlin 1D)
instead of SpaceX's internal engine ID, since we're no longer pulling from
SpaceX's own API. This is actually a stronger multi-source story for the rubric:
1a is multi-agency (not just SpaceX) and far larger (~8,000 vs. ~200 launches),
1b is the one piece that's legitimately static (engine physics don't change
day to day) rather than something to apologize for.

Why simulate the stream instead of a second real-time feed: there is no public
API that streams live rocket-engine telemetry (real telemetry from static-fire
tests isn't published as raw numbers). Simulating it — grounded in real specs
from source 1b — is a standard, legitimate substitute for "no live feed exists,"
and it's what makes the anomaly-detection piece possible (real historical data
has no labeled sensor-fault events to detect).

*(Optional extension if time remains: also poll a real live source, e.g. Open
Notify's ISS-position API, as a second genuinely-real near-real-time feed
alongside the simulated telemetry.)*

## 4. Architecture

All boxes below are **Floci server endpoints** (`http://localhost:4566`), addressed
through normal `boto3` calls — same code shape as real AWS, running locally.

```
BATCH LEG
 ingest_batch.py (plain script) → pulls launches from Launch Library 2 (per known
   rocket family) + reads the static engine_specs.py reference table
        │
        ▼
 S3 (Floci)  raw/launchlibrary2/{family}.json , raw/engine_specs.json
        │
        ▼
 transform_batch.py (pandas): flatten, join launches↔engine_specs on rocket
   family name, feature engineer
        │
        ▼
 S3 (Floci)  curated/engines.parquet , curated/launches.parquet
        │
        ▼
 DuckDB: SQL EDA directly over the curated Parquet (Athena stand-in)
        │
        ▼
 train_model.py (scikit-learn, local): predict launch success
        │
        ▼
 S3 (Floci)  models/model.pkl + metrics.json

STREAMING LEG
 producer.py  ──(1 record/sec/engine)──►  Kinesis Data Stream (Floci)
        │
        ▼
 consumer.py (plain Python process, polls the stream — "Lambda-equivalent")
   - rolling stats (mean/std over last N readings)
   - rule-based anomaly flag (z-score or threshold breach vs. engine's real spec range)
        │
        ├──► DynamoDB (Floci)  latest_reading (per engine_id) — powers "live" dashboard view
        └──► S3 (Floci)  raw/telemetry/{date}/*.json (append-only, for later batch reprocessing)

DASHBOARD
 Streamlit app reads:
   - batch: curated Parquet via DuckDB → historical charts + model results
   - stream: DynamoDB latest_reading, polled every few seconds → live gauge/line chart + anomaly alerts
```

## 5. Services & cost rationale (Floci, all local)

| Stage | Service (via Floci) | Why |
|---|---|---|
| Batch ingestion/storage | S3 | Same API as real AWS; no account needed |
| Batch schema/query | **DuckDB** over curated Parquet (not Athena — not supported by Floci) | Real SQL analytics layer, zero extra service |
| Streaming ingestion | **Kinesis Data Stream** (Floci supports it) | Reads as genuine "streaming" for the architecture story |
| Stream processing | Plain Python process (`consumer.py`), not a deployed Lambda | Skips Lambda packaging/IAM inside the emulator; documented as Lambda-equivalent |
| Stream "latest state" | DynamoDB | Small "latest reading per engine" table the dashboard polls |
| ML training | Local / notebook (scikit-learn) | Seconds on ~200 rows |
| Dashboard | Streamlit (local) | Batch charts + live-polling panel in one app |

Everything runs via `floci start` (one CLI, manages its own Docker container) —
no AWS account, no billing alert, no signup needed. **Cost is $0, not "near $0."**

## 6. Data model

**Batch — `curated/engines` (dimension, from `engine_specs.py`):**
`rocket_family (PK/join key), engine_name, manufacturer, propellant_type, thrust_sl_kN, thrust_vac_kN, isp_sl_s, isp_vac_s`

**Batch — `curated/launches` (fact, from Launch Library 2):**
`launch_id, date, rocket_family (FK), agency, orbit, success (bool)`

Grain: one row per launch attempt, joined to the engines dimension on
`rocket_family` (e.g. "Falcon 9" → Merlin 1D specs) instead of an internal
SpaceX rocket/engine ID.

**Streaming — `telemetry` (event, raw in S3):**
`engine_id, timestamp, chamber_pressure, temperature, vibration, fuel_flow, thrust`

**Streaming — `latest_reading` (DynamoDB, keyed by `engine_id`):**
`engine_id (PK), timestamp, latest values..., rolling_mean, rolling_std, anomaly_flag (bool), anomaly_reason`

## 7. ML + real-time processing tasks

- **Batch model:** binary classification, target = `success`, features = engine
  thrust/Isp/propellant/engine_count/payload_mass/year. Logistic Regression
  (baseline/interpretable) + Random Forest (feature importance). Evaluate with
  precision/recall/F1/PR-AUC (class imbalance expected).
- **Streaming anomaly detection:** per-engine rolling z-score on each telemetry
  field; flag `anomaly=True` when a reading exceeds ~3σ from the rolling mean, or
  falls outside the engine's real spec range from source #1 (e.g. chamber pressure
  spikes 20% above the real engine's rated max). Validate by checking the
  detector actually fires on the anomalies you deliberately injected in the
  producer — this is your "Resultados y Validación" evidence for the streaming leg.

## 8. Repo structure

```
FinalProject/
├── ANALYSIS_AND_DESIGN.md
├── README.md                    setup + "how to run" + architecture diagram
├── src/
│   ├── batch/
│   │   ├── ingest_batch.py
│   │   └── transform_batch.py
│   ├── streaming/
│   │   ├── producer.py           simulated telemetry generator
│   │   └── consumer.py           Lambda handler: rolling stats + anomaly rule
│   ├── ml/
│   │   └── train_model.py
│   └── dashboard/
│       └── app.py                Streamlit
├── sql/                          Athena DDL + queries
├── infra/                        boto3/CLI setup scripts (bucket, stream, table, Lambda)
├── tests/                        unit tests for transform + anomaly rule
└── slides/                       presentation deck
```

## 9. Presentation outline (20 min + 5 Q&A)

1. Problem & objective (1 min)
2. Architecture diagram, batch vs. streaming legs (4 min)
3. Live demo: run producer, show dashboard updating + an anomaly firing (5 min)
4. Batch results: model metrics, feature importance chart (4 min)
5. Technical challenges + how you solved them (3 min) — pick 2-3 real ones (e.g.
   handling class imbalance, tuning the anomaly threshold to avoid false positives)
6. Cost & what you'd do differently at scale (2 min)
7. Q&A (5 min)

## 10. Build plan (Floci — no AWS account setup needed)

| Day | Deliverable | Status |
|---|---|---|
| **1** | Floci up + batch raw data | ✅ Done. `floci start` running (visual dashboard at `localhost:4501`); S3 bucket + DynamoDB table + Kinesis stream created via `infra/setup.py`, smoke-tested. All 9 rocket families ingested (1,106 real launches) — first 5 via the local `ingest_batch.py` script, remaining 4 via the deployed `batch-ingest` Lambda once a rate-limit window cleared. |
| **2** | Batch curated + streaming flowing end to end | ✅ Streaming done, ahead of schedule and as a **real Lambda** (not a polling script): `producer.py` (physically-grounded simulated telemetry, injected anomalies) → Kinesis → `consumer.py` deployed via `infra/deploy_lambda.py`, auto-triggered by a Kinesis event source mapping, Welford's-algorithm rolling stats + z-score anomaly flag → DynamoDB + S3. Verified catching real injected anomalies. ❌ `transform_batch.py` (curated Parquet) — not started yet. |
| **3** | Batch model + anomaly validation | Not started. Train/evaluate Logistic Regression (+ Random Forest if time) on curated data; save metrics. Log anomaly-detector precision/recall against injected faults. |
| **4** | Dashboard + docs + presentation | Not started. Streamlit app: batch charts + live-polling panel from DynamoDB + anomaly banner. README with architecture diagram + "how to run." Build slides, rehearse the live demo. |

Dropping the real-AWS setup (no account, IAM, or Budgets alert to configure) is
what makes the tight day count workable even with **two real Lambda
deployments** in the mix — that overhead was the main cost of the earlier
real-AWS estimate, not the code itself. If time gets tight later: cut Random
Forest (keep only Logistic Regression) and keep tests manual rather than
automated; don't cut the streaming leg, the DynamoDB live view, or the anomaly
validation — that's what the rubric is actually checking for.

## 11. Open decisions

1. **Kinesis vs. SQS** for the streaming transport — resolved: Kinesis, real
   event source mapping to a real deployed Lambda. Confirmed working.
2. Whether to add the optional ISS-position live API (§3) for a second genuinely
   real-time-real-data source — nice-to-have, not required.
3. `infra/setup.py` / `infra/deploy_lambda.py` / `infra/deploy_ingest_lambda.py`
   (all boto3): create the S3 bucket, Kinesis stream, DynamoDB table, and both
   Lambda functions — this doubles as the "configuration" documentation the
   rubric's repo deliverable asks for. **(Done.)**
4. **Mention the Floci choice explicitly in the presentation** ("implemented
   against a local, LocalStack-compatible AWS environment via Floci, with real
   Lambda functions actually executing — the exact same code deploys unchanged
   to a real AWS account by flipping one env var") — frame it as a deliberate
   engineering decision (reproducibility, no cloud cost/account needed for
   grading, no Docker-auth-token friction), not as a shortcut.
