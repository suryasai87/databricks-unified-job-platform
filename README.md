# Unified Job Platform

Enterprise job monitoring and cost analytics for Databricks with optional Lakebase acceleration.

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) v0.200+ authenticated to your workspace
  - If deploying to a workspace different from the default profile, always specify `--profile` in CLI commands
- System tables enabled (`system.lakeflow`, `system.billing`, `system.access`)
- Node.js 18+ and Python 3.10+
- A SQL Warehouse (serverless recommended)
- *(Optional)* A Lakebase instance for sub-100ms query performance on job monitoring pages

## Step 1: Clone and Configure

Edit `databricks.yml` and set the variables for your environment:

```yaml
variables:
  catalog:
    default: "main"                    # Your Unity Catalog name
  schema:
    default: "cost_management"         # Schema where tables will be created
  warehouse_id:
    default: "<YOUR_WAREHOUSE_ID>"     # Required — SQL Warehouse ID
  lakebase_instance_name:
    default: ""                        # Optional — Lakebase instance name
  genie_space_id:
    default: ""                        # Optional — Genie Space ID for AI assistant
```

If you have multiple environments, override variables per target:

```yaml
targets:
  prod:
    variables:
      catalog: "prod_catalog"
      warehouse_id: "abc123"
```

## Step 2: Deploy the Data Pipeline

The app reads from materialized views, not raw system tables directly. A Spark Declarative Pipeline (SDP) ingests from `system.lakeflow.*`, `system.billing.*`, and `system.access.*` into your catalog/schema.

```bash
# Validate the bundle
databricks bundle validate --target dev

# Deploy the pipeline and refresh job
databricks bundle deploy --target dev
```

This creates two resources:

1. **`job-monitoring-sync` pipeline** — Streaming tables + materialized views:
  - `synced_job_run_timeline` (streaming table)
  - `synced_jobs` (streaming table)
  - `synced_workspaces` (materialized view)
  - `job_runs_latest` (materialized view — one row per run)
  - `jobs_latest` (materialized view — latest state per job)
  - `billing_usage_enriched` (materialized view — jobs billing with list prices, filtered to `job_id IS NOT NULL`, deduped by `usage_date/workspace_id/job_id/job_run_id/sku_name`)
2. **`job-monitoring-refresh` job** — Runs hourly to refresh the pipeline and (optionally) Lakebase synced tables.

Run the pipeline for the first time:

```bash
# Trigger initial pipeline run
databricks bundle run job-monitoring-sync --target dev
```

Verify the tables were created:

```sql
-- Run in Databricks SQL
SHOW TABLES IN <catalog>.<schema>;
-- You should see: job_runs_latest, jobs_latest, synced_workspaces, billing_usage_enriched, etc.
```

## Step 3: Set Up Lakebase (Optional)

Skip this step if you don't have a Lakebase instance. The app will use SQL Warehouse queries instead.

Lakebase accelerates the job monitoring pages (Jobs List, Health Monitor) with sub-100ms queries. Cost Analytics always uses the SQL Warehouse since billing aggregations are better suited to columnar compute.

Create three synced tables in your Lakebase instance following the [Databricks synced tables documentation](https://learn.microsoft.com/en-us/azure/databricks/oltp/projects/sync-tables). When prompted, use the same name as your Unity Catalog catalog for the Lakebase database name, and the same name as your Unity Catalog schema for the Lakebase schema name. Use these source tables and target names:

| Source (Unity Catalog) | Synced table name |
| --- | --- |
| `<catalog>.<schema>.job_runs_latest` | `lb_job_runs_latest` |
| `<catalog>.<schema>.jobs_latest` | `lb_jobs_latest` |
| `<catalog>.<schema>.synced_workspaces` | `lb_synced_workspaces` |

Once the tables are created, the `job-monitoring-refresh` job will refresh them on every hourly run. Make sure `lakebase_instance_name` is set in `databricks.yml` so the refresh job can locate the instance.

Add the Lakebase database as an app resource through app authorization. In your app's settings, go to **Authorization** and add the Lakebase database instance as a resource. This allows the app's service principal to authenticate with Lakebase.

Grant the app's service principal access to the Lakebase schema (run against your Lakebase instance):

```sql
GRANT CONNECT ON DATABASE <catalog> TO "<app-service-principal>";
GRANT USAGE ON SCHEMA <schema> TO "<app-service-principal>";
GRANT SELECT ON ALL TABLES IN SCHEMA <schema> TO "<app-service-principal>";
```

Get the app's service principal identity:

```bash
databricks apps get unified-job-platform --output json | jq -r '.service_principal_client_id'
```

## Step 4: Build and Deploy the App

Start the app in the workspace.

```bash
# Build frontend + backend into build/app
python build.py --target dev
```

This compiles the React frontend, packages it with the FastAPI backend, and generates `app.yaml` with your environment variables.

```bash
# Upload and deploy
databricks workspace import-dir build/app \
  /Workspace/Users/$(databricks current-user me --output json | jq -r '.userName')/.bundle/unified-job-platform/dev/app \
  --overwrite

databricks apps deploy unified-job-platform \
  --source-code-path /Workspace/Users/$(databricks current-user me --output json | jq -r '.userName')/.bundle/unified-job-platform/dev/app
```

## Step 5: Grant Permissions

The app runs as a service principal. It needs access to your SQL Warehouse and Unity Catalog tables.

```bash
# Get the app's service principal ID
APP_SP=$(databricks apps get unified-job-platform --output json | jq -r '.service_principal_client_id')
echo "Service Principal: $APP_SP"
```

Grant Unity Catalog access (run in Databricks SQL). Alternatively, do this in UI:

```sql
GRANT USE CATALOG ON CATALOG <catalog> TO `<APP_SP>`;
GRANT USE SCHEMA ON SCHEMA <catalog>.<schema> TO `<APP_SP>`;
GRANT SELECT ON SCHEMA <catalog>.<schema> TO `<APP_SP>`;
```

## Step 6: Open the App

```bash
# Get the app URL
databricks apps get unified-job-platform --output json | jq -r '.url'
```

Open the URL in your browser. The app should load with your job data. Check `/api/health` to confirm the data source is connected.

## Known Limitations

- **SQL Warehouse row cap**: Query results are capped at 10,000 rows per request due to the inline byte limit. This does not apply when using Lakebase.
- **Jobs list**: Displays up to 1,000 job runs per query. Adjust the time range or use filters to narrow results.
- **Gantt — concurrent runs chart**: Derived from the same job runs fetch as the timeline (no separate query). Accuracy is limited by the 10,000-run fetch cap — in very high-frequency workspaces, concurrent counts may be understated.
- **Gantt — job timeline**: Displays up to 200 jobs at a time, sorted by earliest run start within the selected window.
- **Cost Analytics** captures only the last 90 days of billing data, as defined by the `billing_usage_enriched` materialized view filter.
- **Cancel Job** requires the app's service principal to have **CAN MANAGE RUN** permission on the target jobs. Without it, cancel requests return a 403 error.

## Configuration Reference


| Variable                 | Required  | Description                                     |
| ------------------------ | --------- | ----------------------------------------------- |
| `catalog`                | Yes       | Unity Catalog name                              |
| `schema`                 | Yes       | Schema for materialized views and synced tables |
| `warehouse_id`           | Yes       | SQL Warehouse ID                                |
| `lakebase_instance_name` | No        | Enables sub-100ms queries for job monitoring    |
| `genie_space_id`         | No        | Enables AI assistant tab                        |
| `service_principal_name` | Prod only | Service principal for production `run_as`       |
