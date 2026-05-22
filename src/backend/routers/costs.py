"""
Costs Router - Cost analytics endpoints querying billing_usage_enriched MV
"""
import asyncio
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


class DailyCost(BaseModel):
    date: str
    cost: float
    dbus: float
    job_runs: int


class TopJob(BaseModel):
    job_id: str
    job_name: Optional[str]
    workspace_id: Optional[str]
    total_cost: float
    total_dbus: float
    run_count: int
    primary_sku: Optional[str]


class TopJobRun(BaseModel):
    job_id: str
    job_name: Optional[str]
    job_run_id: str
    workspace_id: Optional[str]
    result_state: Optional[str]
    cost: float
    dbus: float
    sku_name: Optional[str]


class CostBySku(BaseModel):
    sku_name: str
    total_dbus: float
    total_cost: float
    job_count: int


def get_data_layer():
    """Get data layer from app state."""
    from app import data_layer
    if not data_layer:
        raise HTTPException(status_code=503, detail="Data layer not initialized")
    return data_layer


@router.get("/summary", response_model=CostSummary)
async def get_cost_summary(
    days: int = Query(30, ge=1, le=90, description="Number of days to look back"),
    workspace_id: Optional[str] = Query(None, description="Filter by workspace ID"),
):
    """Get cost KPIs: total cost, total DBUs, unique jobs, run count, avg cost/run, avg daily cost."""
    dl = get_data_layer()

    ws_filter = ""
    if workspace_id:
        safe_ws = workspace_id.replace("'", "''")
        ws_filter = f"AND CAST(workspace_id AS STRING) = '{safe_ws}'"

    query = f"""
        SELECT
            COALESCE(SUM(cost_usd), 0) AS total_cost_usd,
            COALESCE(SUM(dbus), 0) AS total_dbus,
            COUNT(DISTINCT job_id) AS unique_jobs,
            COUNT(DISTINCT job_run_id) AS total_runs,
            CASE WHEN COUNT(DISTINCT job_run_id) > 0
                 THEN SUM(cost_usd) / COUNT(DISTINCT job_run_id)
                 ELSE 0 END AS avg_cost_per_run,
            CASE WHEN COUNT(DISTINCT usage_date) > 0
                 THEN SUM(cost_usd) / COUNT(DISTINCT usage_date)
                 ELSE 0 END AS avg_daily_cost
        FROM {dl.catalog}.{dl.schema}.billing_usage_enriched
        WHERE usage_date >= CURRENT_DATE - INTERVAL {days} DAY
          {ws_filter}
    """

    result = await asyncio.to_thread(dl.execute_query, query)

    if not result.data:
        return CostSummary(
            total_cost_usd=0, total_dbus=0, unique_jobs=0,
            total_runs=0, avg_cost_per_run=0, avg_daily_cost=0,
        )

    row = result.data[0]
    return CostSummary(
        total_cost_usd=round(float(row[0] or 0), 2),
        total_dbus=round(float(row[1] or 0), 2),
        unique_jobs=int(row[2] or 0),
        total_runs=int(row[3] or 0),
        avg_cost_per_run=round(float(row[4] or 0), 2),
        avg_daily_cost=round(float(row[5] or 0), 2),
    )


@router.get("/daily", response_model=List[DailyCost])
async def get_daily_costs(
    days: int = Query(30, ge=1, le=90, description="Number of days to look back"),
    workspace_id: Optional[str] = Query(None, description="Filter by workspace ID"),
):
    """Get daily cost time series."""
    dl = get_data_layer()

    ws_filter = ""
    if workspace_id:
        safe_ws = workspace_id.replace("'", "''")
        ws_filter = f"AND CAST(workspace_id AS STRING) = '{safe_ws}'"

    query = f"""
        SELECT
            CAST(usage_date AS STRING) AS usage_date,
            COALESCE(SUM(cost_usd), 0) AS cost,
            COALESCE(SUM(dbus), 0) AS dbus,
            COUNT(DISTINCT job_run_id) AS job_runs
        FROM {dl.catalog}.{dl.schema}.billing_usage_enriched
        WHERE usage_date >= CURRENT_DATE - INTERVAL {days} DAY
          {ws_filter}
        GROUP BY usage_date
        ORDER BY usage_date
    """

    result = await asyncio.to_thread(dl.execute_query, query)

    return [
        DailyCost(
            date=str(row[0]),
            cost=round(float(row[1] or 0), 2),
            dbus=round(float(row[2] or 0), 2),
            job_runs=int(row[3] or 0),
        )
        for row in result.data
    ]


@router.get("/top-jobs", response_model=List[TopJob])
async def get_top_expensive_jobs(
    days: int = Query(30, ge=1, le=90, description="Number of days to look back"),
    limit: int = Query(20, ge=1, le=100, description="Number of top jobs to return"),
    workspace_id: Optional[str] = Query(None, description="Filter by workspace ID"),
):
    """Get top N most expensive jobs."""
    dl = get_data_layer()

    ws_filter = ""
    if workspace_id:
        safe_ws = workspace_id.replace("'", "''")
        ws_filter = f"AND CAST(workspace_id AS STRING) = '{safe_ws}'"

    query = f"""
        SELECT
            CAST(job_id AS STRING) AS job_id,
            FIRST_VALUE(job_name) AS job_name,
            CAST(FIRST_VALUE(workspace_id) AS STRING) AS workspace_id,
            SUM(cost_usd) AS total_cost,
            SUM(dbus) AS total_dbus,
            COUNT(DISTINCT job_run_id) AS run_count,
            FIRST_VALUE(sku_name) AS primary_sku
        FROM {dl.catalog}.{dl.schema}.billing_usage_enriched
        WHERE usage_date >= CURRENT_DATE - INTERVAL {days} DAY
          {ws_filter}
        GROUP BY job_id
        ORDER BY total_cost DESC
        LIMIT {limit}
    """

    result = await asyncio.to_thread(dl.execute_query, query)

    return [
        TopJob(
            job_id=str(row[0] or ""),
            job_name=row[1],
            workspace_id=str(row[2]) if row[2] else None,
            total_cost=round(float(row[3] or 0), 2),
            total_dbus=round(float(row[4] or 0), 2),
            run_count=int(row[5] or 0),
            primary_sku=row[6],
        )
        for row in result.data
    ]


@router.get("/top-runs", response_model=List[TopJobRun])
async def get_top_expensive_runs(
    days: int = Query(30, ge=1, le=90, description="Number of days to look back"),
    limit: int = Query(100, ge=1, le=100, description="Number of top runs to return"),
    workspace_id: Optional[str] = Query(None, description="Filter by workspace ID"),
):
    """Get top N most expensive individual job runs."""
    dl = get_data_layer()

    ws_filter = ""
    if workspace_id:
        safe_ws = workspace_id.replace("'", "''")
        ws_filter = f"AND CAST(workspace_id AS STRING) = '{safe_ws}'"

    query = f"""
        SELECT
            CAST(job_id AS STRING) AS job_id,
            FIRST_VALUE(job_name) AS job_name,
            CAST(job_run_id AS STRING) AS job_run_id,
            CAST(FIRST_VALUE(workspace_id) AS STRING) AS workspace_id,
            SUM(cost_usd) AS cost,
            SUM(dbus) AS dbus,
            FIRST_VALUE(sku_name) AS sku_name
        FROM {dl.catalog}.{dl.schema}.billing_usage_enriched
        WHERE usage_date >= CURRENT_DATE - INTERVAL {days} DAY
          {ws_filter}
        GROUP BY job_id, job_run_id
        ORDER BY cost DESC
        LIMIT {limit}
    """

    result = await asyncio.to_thread(dl.execute_query, query)

    return [
        TopJobRun(
            job_id=str(row[0] or ""),
            job_name=row[1],
            job_run_id=str(row[2] or ""),
            workspace_id=str(row[3]) if row[3] else None,
            result_state=None,
            cost=round(float(row[4] or 0), 2),
            dbus=round(float(row[5] or 0), 2),
            sku_name=row[6],
        )
        for row in result.data
    ]


@router.get("/by-sku", response_model=List[CostBySku])
async def get_cost_by_sku(
    days: int = Query(30, ge=1, le=90, description="Number of days to look back"),
    workspace_id: Optional[str] = Query(None, description="Filter by workspace ID"),
):
    """Get cost breakdown by SKU type."""
    dl = get_data_layer()

    ws_filter = ""
    if workspace_id:
        safe_ws = workspace_id.replace("'", "''")
        ws_filter = f"AND CAST(workspace_id AS STRING) = '{safe_ws}'"

    query = f"""
        SELECT
            REGEXP_REPLACE(sku_name, '_(US|AP|EUROPE|SA|CA)_[A-Z_]+$', '') AS sku,
            COALESCE(SUM(dbus), 0) AS total_dbus,
            COALESCE(SUM(cost_usd), 0) AS total_cost,
            COUNT(DISTINCT job_id) AS job_count
        FROM {dl.catalog}.{dl.schema}.billing_usage_enriched
        WHERE usage_date >= CURRENT_DATE - INTERVAL {days} DAY
          {ws_filter}
        GROUP BY sku
        ORDER BY total_cost DESC
    """

    result = await asyncio.to_thread(dl.execute_query, query)

    return [
        CostBySku(
            sku_name=row[0] or "Unknown",
            total_dbus=round(float(row[1] or 0), 2),
            total_cost=round(float(row[2] or 0), 2),
            job_count=int(row[3] or 0),
        )
        for row in result.data
    ]
