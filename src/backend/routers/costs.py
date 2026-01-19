"""
Costs Router - Cost attribution and analytics endpoints
"""
import os
from typing import List, Optional

from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel

router = APIRouter()


class CostSummary(BaseModel):
    total_cost_usd: float
    total_dbus: float
    unique_jobs: int
    total_runs: int
    avg_cost_per_run: float
    avg_daily_cost: float


def get_data_layer():
    """Get data layer from app state."""
    from app import data_layer
    if not data_layer:
        raise HTTPException(status_code=503, detail="Data layer not initialized")
    return data_layer


@router.get("/summary", response_model=CostSummary)
async def get_cost_summary(
    days: int = Query(30, ge=1, le=90, description="Number of days to look back"),
):
    """Get cost summary statistics."""
    dl = get_data_layer()
    catalog = os.getenv("CATALOG", "hls_amer_catalog")
    schema = os.getenv("SCHEMA", "cost_management")

    query = f"""
        SELECT
            COALESCE(SUM(total_cost_usd), 0) AS total_cost,
            COALESCE(SUM(total_dbus), 0) AS total_dbus,
            COUNT(DISTINCT job_run_id) AS unique_jobs,
            COALESCE(SUM(job_runs), 0) AS total_runs
        FROM {catalog}.{schema}.serverless_cost_summary
        WHERE usage_date >= CURRENT_DATE - INTERVAL {days} DAY
    """

    try:
        result = dl.execute_query(query)

        if not result.data or not result.data[0]:
            return CostSummary(
                total_cost_usd=0,
                total_dbus=0,
                unique_jobs=0,
                total_runs=0,
                avg_cost_per_run=0,
                avg_daily_cost=0,
            )

        row = result.data[0]
        total_cost = float(row[0] or 0)
        total_dbus = float(row[1] or 0)
        unique_jobs = int(row[2] or 0)
        total_runs = int(row[3] or 0)

        return CostSummary(
            total_cost_usd=round(total_cost, 2),
            total_dbus=round(total_dbus, 2),
            unique_jobs=unique_jobs,
            total_runs=total_runs,
            avg_cost_per_run=round(total_cost / total_runs, 4) if total_runs > 0 else 0,
            avg_daily_cost=round(total_cost / days, 2),
        )

    except Exception as e:
        # Fall back to system.billing.usage if custom views don't exist
        fallback_query = f"""
            SELECT
                COALESCE(SUM(usage_quantity * 0.15), 0) AS total_cost,
                COALESCE(SUM(usage_quantity), 0) AS total_dbus,
                COUNT(DISTINCT usage_metadata.job_id) AS unique_jobs,
                COUNT(DISTINCT usage_metadata.job_run_id) AS total_runs
            FROM system.billing.usage
            WHERE usage_date >= CURRENT_DATE - INTERVAL {days} DAY
              AND product_features.is_serverless = TRUE
        """

        try:
            result = dl.execute_query(fallback_query)
            row = result.data[0] if result.data else [0, 0, 0, 0]

            return CostSummary(
                total_cost_usd=round(float(row[0] or 0), 2),
                total_dbus=round(float(row[1] or 0), 2),
                unique_jobs=int(row[2] or 0),
                total_runs=int(row[3] or 0),
                avg_cost_per_run=round(float(row[0] or 0) / int(row[3] or 1), 4),
                avg_daily_cost=round(float(row[0] or 0) / days, 2),
            )
        except Exception as fallback_error:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to get cost data: {str(fallback_error)}",
            )


@router.get("/daily")
async def get_daily_costs(
    days: int = Query(30, ge=1, le=90, description="Number of days to look back"),
):
    """Get daily cost breakdown."""
    dl = get_data_layer()

    query = f"""
        SELECT
            usage_date,
            COALESCE(SUM(usage_quantity * 0.15), 0) AS daily_cost,
            COALESCE(SUM(usage_quantity), 0) AS daily_dbus,
            COUNT(DISTINCT usage_metadata.job_run_id) AS job_runs
        FROM system.billing.usage
        WHERE usage_date >= CURRENT_DATE - INTERVAL {days} DAY
          AND product_features.is_serverless = TRUE
        GROUP BY usage_date
        ORDER BY usage_date
    """

    result = dl.execute_query(query)

    return [
        {
            "date": str(row[0]),
            "cost": round(float(row[1] or 0), 2),
            "dbus": round(float(row[2] or 0), 2),
            "job_runs": int(row[3] or 0),
        }
        for row in result.data
    ]


@router.get("/top-jobs")
async def get_top_expensive_jobs(
    days: int = Query(30, ge=1, le=90, description="Number of days to look back"),
    limit: int = Query(10, ge=1, le=50, description="Number of top jobs to return"),
):
    """Get top expensive jobs with job names and workspace info."""
    dl = get_data_layer()
    host = os.getenv("DATABRICKS_HOST", "fe-vm-hls-amer.cloud.databricks.com")

    # Query with job name from system.lakeflow.jobs
    query = f"""
        WITH job_costs AS (
            SELECT
                usage_metadata.job_id AS job_id,
                usage_metadata.workspace_id AS workspace_id,
                FIRST_VALUE(usage_metadata.notebook_path) AS notebook_path,
                COALESCE(SUM(usage_quantity * 0.15), 0) AS total_cost,
                COALESCE(SUM(usage_quantity), 0) AS total_dbus,
                COUNT(DISTINCT usage_metadata.job_run_id) AS run_count
            FROM system.billing.usage
            WHERE usage_date >= CURRENT_DATE - INTERVAL {days} DAY
              AND product_features.is_serverless = TRUE
              AND usage_metadata.job_id IS NOT NULL
            GROUP BY usage_metadata.job_id, usage_metadata.workspace_id
        )
        SELECT
            jc.job_id,
            jc.workspace_id,
            j.name AS job_name,
            jc.notebook_path,
            jc.total_cost,
            jc.total_dbus,
            jc.run_count
        FROM job_costs jc
        LEFT JOIN system.lakeflow.jobs j
            ON jc.job_id = j.job_id AND jc.workspace_id = j.workspace_id
        ORDER BY jc.total_cost DESC
        LIMIT {limit}
    """

    try:
        result = dl.execute_query(query)

        return [
            {
                "job_id": row[0],
                "workspace_id": row[1],
                "job_name": row[2] or f"Job {row[0]}",
                "notebook_path": row[3],
                "total_cost": round(float(row[4] or 0), 2),
                "total_dbus": round(float(row[5] or 0), 2),
                "run_count": int(row[6] or 0),
                "avg_cost_per_run": round(float(row[4] or 0) / max(int(row[6] or 1), 1), 4),
                "job_url": f"https://{host}/jobs/{row[0]}" if row[0] else None,
            }
            for row in result.data
        ]
    except Exception as e:
        # Fallback if system.lakeflow.jobs is not available
        fallback_query = f"""
            SELECT
                usage_metadata.job_id AS job_id,
                usage_metadata.workspace_id AS workspace_id,
                FIRST_VALUE(usage_metadata.notebook_path) AS notebook_path,
                COALESCE(SUM(usage_quantity * 0.15), 0) AS total_cost,
                COALESCE(SUM(usage_quantity), 0) AS total_dbus,
                COUNT(DISTINCT usage_metadata.job_run_id) AS run_count
            FROM system.billing.usage
            WHERE usage_date >= CURRENT_DATE - INTERVAL {days} DAY
              AND product_features.is_serverless = TRUE
              AND usage_metadata.job_id IS NOT NULL
            GROUP BY usage_metadata.job_id, usage_metadata.workspace_id
            ORDER BY total_cost DESC
            LIMIT {limit}
        """

        result = dl.execute_query(fallback_query)

        return [
            {
                "job_id": row[0],
                "workspace_id": row[1],
                "job_name": f"Job {row[0]}",
                "notebook_path": row[2],
                "total_cost": round(float(row[3] or 0), 2),
                "total_dbus": round(float(row[4] or 0), 2),
                "run_count": int(row[5] or 0),
                "avg_cost_per_run": round(float(row[3] or 0) / max(int(row[5] or 1), 1), 4),
                "job_url": f"https://{host}/jobs/{row[0]}" if row[0] else None,
            }
            for row in result.data
        ]


@router.get("/by-identity")
async def get_cost_by_identity(
    days: int = Query(30, ge=1, le=90, description="Number of days to look back"),
    limit: int = Query(20, ge=1, le=100, description="Number of identities to return"),
):
    """Get costs grouped by user/identity."""
    dl = get_data_layer()

    query = f"""
        SELECT
            COALESCE(identity_metadata.run_as, 'Unknown') AS identity,
            COALESCE(SUM(usage_quantity * 0.15), 0) AS total_cost,
            COALESCE(SUM(usage_quantity), 0) AS total_dbus,
            COUNT(DISTINCT usage_metadata.job_run_id) AS job_runs
        FROM system.billing.usage
        WHERE usage_date >= CURRENT_DATE - INTERVAL {days} DAY
          AND product_features.is_serverless = TRUE
        GROUP BY identity_metadata.run_as
        ORDER BY total_cost DESC
        LIMIT {limit}
    """

    result = dl.execute_query(query)

    return [
        {
            "identity": row[0],
            "total_cost": round(float(row[1] or 0), 2),
            "total_dbus": round(float(row[2] or 0), 2),
            "job_runs": int(row[3] or 0),
        }
        for row in result.data
    ]


@router.get("/by-project")
async def get_cost_by_project(
    days: int = Query(30, ge=1, le=90, description="Number of days to look back"),
):
    """Get costs grouped by project code (from tag correlation)."""
    dl = get_data_layer()
    catalog = os.getenv("CATALOG", "hls_amer_catalog")
    schema = os.getenv("SCHEMA", "cost_management")

    query = f"""
        SELECT
            COALESCE(project_code, 'Untagged') AS project_code,
            COALESCE(department, 'Unknown') AS department,
            COALESCE(SUM(total_cost_usd), 0) AS total_cost,
            COALESCE(SUM(total_dbus), 0) AS total_dbus,
            COALESCE(SUM(job_runs), 0) AS job_runs
        FROM {catalog}.{schema}.serverless_cost_summary
        WHERE usage_date >= CURRENT_DATE - INTERVAL {days} DAY
        GROUP BY project_code, department
        ORDER BY total_cost DESC
    """

    try:
        result = dl.execute_query(query)

        return [
            {
                "project_code": row[0],
                "department": row[1],
                "total_cost": round(float(row[2] or 0), 2),
                "total_dbus": round(float(row[3] or 0), 2),
                "job_runs": int(row[4] or 0),
            }
            for row in result.data
        ]
    except Exception:
        # Return empty if tag correlation table doesn't exist yet
        return []


@router.get("/by-department")
async def get_cost_by_department(
    days: int = Query(30, ge=1, le=90, description="Number of days to look back"),
):
    """Get costs grouped by department (from tag correlation)."""
    dl = get_data_layer()
    catalog = os.getenv("CATALOG", "hls_amer_catalog")
    schema = os.getenv("SCHEMA", "cost_management")

    query = f"""
        SELECT
            COALESCE(department, 'Unknown') AS department,
            COALESCE(SUM(total_cost_usd), 0) AS total_cost,
            COALESCE(SUM(total_dbus), 0) AS total_dbus,
            COALESCE(SUM(job_runs), 0) AS job_runs,
            COUNT(DISTINCT project_code) AS project_count
        FROM {catalog}.{schema}.serverless_cost_summary
        WHERE usage_date >= CURRENT_DATE - INTERVAL {days} DAY
        GROUP BY department
        ORDER BY total_cost DESC
    """

    try:
        result = dl.execute_query(query)

        return [
            {
                "department": row[0],
                "total_cost": round(float(row[1] or 0), 2),
                "total_dbus": round(float(row[2] or 0), 2),
                "job_runs": int(row[3] or 0),
                "project_count": int(row[4] or 0),
            }
            for row in result.data
        ]
    except Exception:
        return []


@router.get("/trends")
async def get_cost_trends(
    weeks: int = Query(8, ge=1, le=52, description="Number of weeks to analyze"),
):
    """Get weekly cost trends with week-over-week analysis."""
    dl = get_data_layer()
    catalog = os.getenv("CATALOG", "hls_amer_catalog")
    schema = os.getenv("SCHEMA", "cost_management")

    query = f"""
        SELECT
            week_start,
            COALESCE(SUM(weekly_cost_usd), 0) AS weekly_cost,
            COALESCE(SUM(weekly_dbus), 0) AS weekly_dbus,
            COALESCE(SUM(weekly_job_runs), 0) AS weekly_jobs
        FROM {catalog}.{schema}.serverless_cost_trends
        WHERE week_start >= CURRENT_DATE - INTERVAL {weeks} WEEK
        GROUP BY week_start
        ORDER BY week_start
    """

    try:
        result = dl.execute_query(query)

        data = [
            {
                "week_start": str(row[0]),
                "cost": round(float(row[1] or 0), 2),
                "dbus": round(float(row[2] or 0), 2),
                "job_runs": int(row[3] or 0),
            }
            for row in result.data
        ]

        # Calculate week-over-week changes
        for i in range(1, len(data)):
            prev_cost = data[i - 1]["cost"]
            curr_cost = data[i]["cost"]
            if prev_cost > 0:
                data[i]["wow_change_pct"] = round((curr_cost - prev_cost) / prev_cost * 100, 2)
            else:
                data[i]["wow_change_pct"] = 0

        return data

    except Exception:
        # Fall back to basic weekly aggregation from billing
        fallback_query = f"""
            SELECT
                DATE_TRUNC('week', usage_date) AS week_start,
                COALESCE(SUM(usage_quantity * 0.15), 0) AS weekly_cost,
                COALESCE(SUM(usage_quantity), 0) AS weekly_dbus,
                COUNT(DISTINCT usage_metadata.job_run_id) AS weekly_jobs
            FROM system.billing.usage
            WHERE usage_date >= CURRENT_DATE - INTERVAL {weeks} WEEK
              AND product_features.is_serverless = TRUE
            GROUP BY DATE_TRUNC('week', usage_date)
            ORDER BY week_start
        """

        result = dl.execute_query(fallback_query)

        return [
            {
                "week_start": str(row[0]),
                "cost": round(float(row[1] or 0), 2),
                "dbus": round(float(row[2] or 0), 2),
                "job_runs": int(row[3] or 0),
            }
            for row in result.data
        ]


@router.get("/correlation-rate")
async def get_correlation_rate(
    days: int = Query(7, ge=1, le=90, description="Number of days to analyze"),
):
    """Get tag correlation rate for cost attribution."""
    dl = get_data_layer()
    catalog = os.getenv("CATALOG", "hls_amer_catalog")
    schema = os.getenv("SCHEMA", "cost_management")

    query = f"""
        SELECT
            correlation_status,
            COUNT(*) AS record_count,
            COALESCE(SUM(estimated_cost_usd), 0) AS total_cost
        FROM {catalog}.{schema}.serverless_cost_by_tags
        WHERE usage_date >= CURRENT_DATE - INTERVAL {days} DAY
        GROUP BY correlation_status
    """

    try:
        result = dl.execute_query(query)

        total_records = sum(int(row[1] or 0) for row in result.data)
        matched = sum(int(row[1] or 0) for row in result.data if row[0] == 'MATCHED')

        return {
            "total_records": total_records,
            "matched": matched,
            "unmatched": total_records - matched,
            "correlation_rate_pct": round(matched / total_records * 100, 2) if total_records > 0 else 0,
            "breakdown": [
                {
                    "status": row[0],
                    "count": int(row[1] or 0),
                    "cost": round(float(row[2] or 0), 2),
                }
                for row in result.data
            ],
        }

    except Exception:
        return {
            "total_records": 0,
            "matched": 0,
            "unmatched": 0,
            "correlation_rate_pct": 0,
            "breakdown": [],
            "message": "Tag correlation table not yet configured",
        }
