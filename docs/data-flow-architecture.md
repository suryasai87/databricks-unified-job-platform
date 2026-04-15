# Data Flow Architecture: System Tables → Lakebase

This document explains how data flows from Databricks system tables into Lakebase for the Unified Job Platform, including latency expectations and known limitations.

## Overview

```
┌─────────────────────────┐
│   Databricks System     │
│   Tables (source of     │
│   truth)                │
│                         │
│  system.lakeflow.jobs   │
│  system.lakeflow.       │
│    job_run_timeline     │
│  system.access.         │
│    workspaces_latest    │
└───────────┬─────────────┘
            │
            │  SDP Pipeline (triggered hourly)
            │  "job-monitoring-sync"
            ▼
┌─────────────────────────┐
│   Unity Catalog         │
│   Streaming Tables &    │
│   Materialized Views    │
│                         │
│  synced_jobs (ST)       │
│  synced_job_run_        │
│    timeline (ST)        │
│  synced_workspaces (MV) │
│  jobs_latest (MV)       │
│  job_runs_latest (MV)   │
└───────────┬─────────────┘
            │
            │  Lakebase Synced Tables (triggered, after SDP)
            │  3 sync pipelines in parallel
            ▼
┌─────────────────────────┐
│   Lakebase (PostgreSQL) │
│   "jobs-monitor-sync"   │
│                         │
│  lb_jobs_latest         │
│  lb_job_runs_latest     │
│  lb_synced_workspaces   │
└───────────┬─────────────┘
            │
            │  psycopg2 connection pool
            │  (circuit breaker + warehouse fallback)
            ▼
┌─────────────────────────┐
│   Unified Job Platform  │
│   FastAPI Backend       │
└─────────────────────────┘
```

## Pipeline Stages

### Stage 1: System Tables

Databricks maintains system tables that record job definitions, run history, and workspace metadata. These are the authoritative source of truth.

| System Table | Content | Update Pattern |
|---|---|---|
| `system.lakeflow.jobs` | Job definitions, names, creators, tags | Append-only changelog; new row on each job change |
| `system.lakeflow.job_run_timeline` | Run history with start/end times, result states, durations | Append-only; rows added as runs progress and complete |
| `system.access.workspaces_latest` | Workspace IDs and URLs | Snapshot table; rows updated in place |

**Latency from real event → system table row:** Databricks does not publish an SLA. In practice, system tables are updated within **minutes** of the underlying event, but delays of 15–30 minutes have been observed during high-load periods. There is no guaranteed upper bound.

### Stage 2: Spark Declarative Pipeline (SDP)

The `job-monitoring-sync` SDP pipeline reads from system tables and produces cleaned, deduplicated tables in Unity Catalog.

| SDP Table | Type | Source | Logic |
|---|---|---|---|
| `synced_jobs` | Streaming Table | `system.lakeflow.jobs` | Raw incremental ingestion |
| `synced_job_run_timeline` | Streaming Table | `system.lakeflow.job_run_timeline` | Raw incremental ingestion |
| `synced_workspaces` | Materialized View | `system.access.workspaces_latest` | Full snapshot (source receives in-place updates incompatible with streaming reads) |
| `jobs_latest` | Materialized View | `synced_jobs` | SCD Type 1 — latest row per job via `ROW_NUMBER()` over `change_time DESC` |
| `job_runs_latest` | Materialized View | `synced_job_run_timeline` | Aggregates multiple period entries per run into a single row with computed duration |

**Trigger:** Hourly via the `job-monitoring-refresh` scheduled job.

### Stage 3: Lakebase Synced Tables

Lakebase synced tables copy SDP output into PostgreSQL using the **TRIGGERED** scheduling policy. They are created and refreshed by a notebook task (`refresh_lakebase_sync.py`) that runs after the SDP pipeline completes, orchestrated by the `job-monitoring-refresh` job.

| Lakebase Table | Source (UC) |
|---|---|
| `lb_jobs_latest` | `{catalog}.{schema}.jobs_latest` |
| `lb_job_runs_latest` | `{catalog}.{schema}.job_runs_latest` |
| `lb_synced_workspaces` | `{catalog}.{schema}.synced_workspaces` |

**Sync mechanism:** Each synced table pipeline reads Change Data Feed (CDF) from its Delta source and applies the changes to PostgreSQL. For `synced_workspaces` (a materialized view without CDF), the sync performs a full snapshot comparison.

### Stage 4: Application Query Layer

The FastAPI backend queries Lakebase via a psycopg2 connection pool. If Lakebase is unavailable, a circuit breaker automatically falls back to the SQL Warehouse.

| Path | Typical Query Latency | When Used |
|---|---|---|
| **Lakebase** | 5–50 ms | Default; Lakebase available and table is synced |
| **SQL Warehouse** | 500–3,000 ms | Fallback; Lakebase down or table not synced |

## End-to-End Latency

There is **no SLA** on end-to-end data freshness. The total delay from a real-world event (e.g., a job failure) appearing in the app depends on multiple stages, each with its own latency:

| Stage | Typical Latency | Guaranteed SLA |
|---|---|---|
| Event → system table | 1–15 min (can be 30+ min) | **None** |
| System table → SDP output | Depends on pipeline schedule | Hourly trigger |
| SDP output → Lakebase | 1–5 min per sync pipeline | **None** (triggered after SDP) |
| **Total** | **~1–2 hours worst case** | **None** |

### Key implications

- **The app shows data that is typically 1–2 hours old.** After each hourly job run, data is current to within minutes. Just before the next run, data may be up to ~1 hour stale plus system table ingestion delay.
- **System table latency is the least controllable.** Databricks does not guarantee when events appear in system tables. If fresher data is critical, consider querying the Jobs API directly for real-time status.
- **The hourly schedule is a cost/freshness tradeoff.** The SDP pipeline and Lakebase syncs are serverless, so each run incurs compute cost. Running more frequently (e.g., every 15 minutes) would reduce staleness but increase cost proportionally.
- **Lakebase query latency is not the bottleneck.** Lakebase reads are consistently under 50 ms. The delay is entirely in data ingestion, not data serving.

## Refresh Job

A single scheduled job (`job-monitoring-refresh`) orchestrates the full refresh:

```
refresh_sdp_pipeline                  ← runs SDP pipeline
    └── refresh_lakebase_sync         ← notebook: creates/refreshes Lakebase synced tables
```

- **Schedule:** Every 1 hour (periodic trigger)
- **Typical total run time:** 2–5 minutes
- **Pipeline:** Defined as `job-monitoring-sync` in `resources/pipeline.yml`

## Design Decisions

**Why streaming tables + materialized views instead of querying system tables directly?**
Streaming tables provide incremental ingestion (only new rows are processed), while materialized views handle deduplication and aggregation. This avoids expensive full-table scans on system tables for every app query.

**Why Lakebase instead of just SQL Warehouse?**
Lakebase provides PostgreSQL-compatible sub-50ms reads, which makes the dashboard feel responsive. SQL Warehouse cold-start and query latency (500ms–3s) creates a noticeably slower user experience, especially for pages that issue multiple queries.

**Why TRIGGERED instead of CONTINUOUS sync?**
CONTINUOUS sync provides ~15-second freshness but runs compute continuously. Since system tables themselves have multi-minute ingestion delays, continuous Lakebase sync doesn't meaningfully improve end-to-end freshness — the bottleneck is upstream. TRIGGERED sync aligned with the SDP pipeline schedule is more cost-effective.

**Why is `synced_workspaces` a materialized view instead of a streaming table?**
`system.access.workspaces_latest` is a snapshot table that receives in-place updates (not just appends). Streaming reads fail on non-append changes. A materialized view does a full read each refresh, which is fine for this small, slowly-changing table.
