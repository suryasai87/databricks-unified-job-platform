# Unified Job Platform

**Enterprise Databricks Job Monitoring & Cost Attribution Platform**

A comprehensive, plug-and-play solution combining job monitoring, cost attribution, and serverless tagging for Databricks workloads. Features sub-100ms query performance with Lakebase integration.

![Dashboard Overview](docs/screenshots/dashboard.png)

## Features

### Job Monitoring
- **Real-time Dashboard** - Overview of job runs, success rates, and trends
- **Jobs List** - Searchable, filterable list of all job runs
- **Matrix View** - Visual grid showing last N runs per job with color-coded status
- **Gantt View** - Timeline visualization of job execution with overlap detection
- **Health Monitor** - Failed jobs, prolonged runs, anomaly detection, SLA compliance

### Cost Attribution (FinOps)
- **Cost Analytics** - Total cost, DBUs, daily trends, cost by identity
- **Tag Correlation** - Dynamic cost attribution for serverless workloads
- **ADF Integration** - Azure Data Factory pipeline parameter passing
- **Department/Project Tracking** - Cost breakdown by organizational dimensions

### AI-Powered Insights
- **Genie Integration** - Natural language queries about jobs and costs
- **Suggested Questions** - Pre-built queries for common analysis scenarios

### Performance
- **Lakebase Integration** - Sub-100ms query latency (10-50x faster than SQL Warehouse)
- **Intelligent Caching** - 5-minute TTL cache with manual refresh
- **Circuit Breaker** - Automatic failover from Lakebase to SQL Warehouse

## Quick Start

### One-Command Deployment

```bash
# Clone the repository
git clone https://github.com/suryasai87/databricks-unified-job-platform.git
cd databricks-unified-job-platform

# Deploy to development
python deploy.py dev

# Or deploy to production
python deploy.py prod
```

### Manual Deployment

```bash
# 1. Build the application
python build.py

# 2. Deploy using Databricks CLI
databricks apps create unified-job-platform
databricks apps deploy unified-job-platform --source-code-path /Workspace/path/to/app
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Unified Job Platform                            │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    React Frontend (TypeScript)                   │   │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐            │   │
│  │  │Dashboard │ │Cost      │ │Health    │ │Matrix    │            │   │
│  │  │          │ │Analytics │ │Monitor   │ │Gantt     │            │   │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘            │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                               │                                         │
│                               ▼                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    FastAPI Backend (Python)                      │   │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐            │   │
│  │  │Jobs API  │ │Costs API │ │Health API│ │Tags API  │            │   │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘            │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                               │                                         │
│                               ▼                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                   Unified Data Layer                             │   │
│  │  ┌─────────────────────┐    ┌─────────────────────┐             │   │
│  │  │    Lakebase         │◄──►│   SQL Warehouse     │             │   │
│  │  │  (Sub-100ms)        │    │    (Fallback)       │             │   │
│  │  └─────────────────────┘    └─────────────────────┘             │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                               │                                         │
│                               ▼                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    Data Sources                                  │   │
│  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────────────────┐ │   │
│  │  │System Tables │ │Billing Data  │ │Tag Correlation Tables    │ │   │
│  │  │(lakeflow)    │ │(billing)     │ │(custom)                  │ │   │
│  │  └──────────────┘ └──────────────┘ └──────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

## Project Structure

```
databricks-unified-job-platform/
├── databricks.yml              # Asset Bundle configuration
├── resources/
│   └── app.yml                 # Databricks App resource
├── build.py                    # Build script
├── deploy.py                   # Automated deployment
├── src/
│   ├── frontend/               # React TypeScript app
│   │   ├── src/
│   │   │   ├── pages/          # Dashboard, Jobs, Costs, Health, etc.
│   │   │   ├── components/     # Reusable UI components
│   │   │   ├── services/       # API client
│   │   │   └── types/          # TypeScript interfaces
│   │   └── package.json
│   └── backend/                # FastAPI Python app
│       ├── app.py              # Main application
│       ├── routers/            # API endpoints
│       ├── data/               # Unified data layer
│       └── requirements.txt
├── sql/
│   └── 01_create_infrastructure.sql  # Tables and views
├── packages/
│   └── databricks_tag_logger/  # Python package for notebooks
├── notebooks/                  # Databricks notebooks
└── adf_templates/              # Azure Data Factory templates
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABRICKS_HOST` | Workspace URL | fe-vm-hls-amer.cloud.databricks.com |
| `CATALOG` | Unity Catalog name | hls_amer_catalog |
| `SCHEMA` | Schema name | cost_management |
| `WAREHOUSE_ID` | SQL Warehouse ID | 4b28691c780d9875 |
| `LAKEBASE_INSTANCE_ID` | Lakebase instance | 6b59171b-cee8-4acc-9209-6c848ffbfbfe |
| `LAKEBASE_ENABLED` | Enable Lakebase | true |
| `CACHE_TTL` | Cache TTL (seconds) | 300 |

### Lakebase Integration

Lakebase provides PostgreSQL-compatible access with sub-100ms latency:

```python
# Performance comparison
SQL Warehouse: 500-5000ms
Lakebase:      20-100ms (10-50x faster)
```

To use Lakebase:
1. Create a Lakebase instance in your workspace
2. Set `LAKEBASE_INSTANCE_ID` in the app configuration
3. Create synced tables for system tables

## Tag Correlation (Cost Attribution)

### Problem Solved
Native dynamic tagging from Azure Data Factory to Databricks serverless compute doesn't exist. This framework provides:
1. Dynamic metadata passing from ADF as job parameters
2. Tag logging to Delta tables at job start
3. Correlation with `system.billing.usage` for cost attribution

### Usage in Notebooks

```python
from databricks_tag_logger import init_tag_logging

# At the start of your notebook
logger, correlation_id = init_tag_logging(
    project_code="PROJ-001",
    department="Engineering"
)

# ... your ETL code ...

# At the end
logger.update_run_end(correlation_id, "SUCCESS")
```

### ADF Integration

Pass parameters from ADF pipelines:
```json
{
  "jobParameters": {
    "adf_pipeline_name": "@{pipeline().Pipeline}",
    "adf_run_id": "@{pipeline().RunId}",
    "project_code": "@{pipeline().parameters.project_code}",
    "department": "@{pipeline().parameters.department}"
  }
}
```

## Permissions Setup

After deployment, grant permissions to the app's service principal:

```bash
# Get the app's service principal
APP_CLIENT_ID=$(databricks apps get unified-job-platform --output json | jq -r '.service_principal_client_id')

# Grant SQL Warehouse access
databricks permissions update sql/warehouses/WAREHOUSE_ID --json '{
  "access_control_list": [{
    "service_principal_name": "'$APP_CLIENT_ID'",
    "permission_level": "CAN_USE"
  }]
}'
```

```sql
-- Grant Unity Catalog access (run in Databricks SQL)
GRANT USE CATALOG ON CATALOG hls_amer_catalog TO `<APP_CLIENT_ID>`;
GRANT USE SCHEMA ON SCHEMA hls_amer_catalog.cost_management TO `<APP_CLIENT_ID>`;
GRANT SELECT ON SCHEMA hls_amer_catalog.cost_management TO `<APP_CLIENT_ID>`;
```

## UI Features

### Dashboard
- Total runs, success/failed/running counts
- Daily job runs trend (area chart)
- Runs by type (pie chart)
- Cost summary and tag correlation rate

### Cost Analytics
- Total cost, DBUs, average cost per run
- Daily cost trend
- Cost by department (pie chart)
- Top 10 expensive jobs

### Health Monitor
- Failed jobs with success rates
- Prolonged runs (running longer than expected)
- Anomaly detection using z-score
- SLA compliance tracking

### Matrix View
- Visual grid of job runs
- Color-coded status (green/red/yellow)
- Hover for run details

### Gantt View
- Timeline visualization
- Concurrent jobs chart
- Overlap detection

## Error Handling

The UI displays clear error messages when:
- Data access is denied (with permission commands to fix)
- Tables don't exist (with setup instructions)
- API calls fail (with retry options)

## Development

```bash
# Install frontend dependencies
cd src/frontend
npm install

# Start development server
npm run dev

# Start backend
cd ../backend
pip install -r requirements.txt
uvicorn app:app --reload
```

## License

Apache 2.0 License

## Contributing

Contributions welcome! Please submit pull requests or open issues.
