"""
Jobs Router - Job monitoring endpoints
"""
import asyncio
import os
from typing import List, Optional
from datetime import datetime

from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel

router = APIRouter()




class JobRun(BaseModel):
    job_id: int
    job_name: Optional[str]
    run_id: int
    workspace_id: Optional[str]
    result_state: Optional[str]
    run_type: Optional[str]
    start_time: Optional[str]
    end_time: Optional[str]
    execution_duration: Optional[float]
    creator_user_name: Optional[str]


class RunSummary(BaseModel):
    total_runs: int
    succeeded: int
    failed: int
    running: int
    success_rate: float


class MatrixRow(BaseModel):
    job_id: int
    job_name: str
    runs: List[dict]


def get_data_layer():
    """Get data layer from app state."""
    from app import data_layer
    if not data_layer:
        raise HTTPException(status_code=503, detail="Data layer not initialized")
    return data_layer


@router.get("/runs", response_model=List[JobRun])
async def get_job_runs(
    days: int = Query(7, ge=1, le=90, description="Number of days to look back (used when hours is not set)"),
    hours: Optional[int] = Query(
        None,
        ge=1,
        le=168,
        description="Rolling hours lookback; when set, returns runs overlapping this window (matches /jobs/concurrent)",
    ),
    limit: int = Query(100, ge=1, le=10000, description="Maximum number of results"),
    status: Optional[str] = Query(None, description="Filter by status"),
    search: Optional[str] = Query(None, description="Search by job name, job ID, or run ID"),
    workspace_id: Optional[str] = Query(None, description="Filter by workspace ID"),
):
    """Get recent job runs."""
    dl = get_data_layer()

    VALID_STATUSES = {"SUCCEEDED", "FAILED", "ERROR", "TIMED_OUT", "CANCELLED", "SKIPPED"}
    status_filter = ""
    if status:
        if status == "RUNNING":
            status_filter = "AND (r.result_state IS NULL OR r.result_state = 'RUNNING')"
        elif status.upper() in VALID_STATUSES:
            status_filter = f"AND r.result_state = '{status}'"

    workspace_filter = ""
    if workspace_id:
        safe_ws = workspace_id.replace("'", "''")
        workspace_filter = f"AND CAST(r.workspace_id AS STRING) = '{safe_ws}'"

    search_filter = ""
    if search:
        safe_search = search.replace("'", "''")
        search_filter = f"AND (CAST(r.job_id AS STRING) LIKE '%{safe_search}%' OR CAST(r.run_id AS STRING) LIKE '%{safe_search}%' OR LOWER(j.name) LIKE LOWER('%{safe_search}%'))"

    if hours is not None:
        time_filter = f"r.period_start_time >= CURRENT_TIMESTAMP - INTERVAL {hours} HOUR"
    else:
        time_filter = f"r.period_start_time >= CURRENT_DATE - INTERVAL {days} DAY"

    query = f"""
        SELECT
            CAST(r.job_id AS BIGINT) AS job_id,
            j.name AS job_name,
            CAST(r.run_id AS BIGINT) AS run_id,
            CAST(r.workspace_id AS STRING) AS workspace_id,
            COALESCE(r.result_state, 'RUNNING') AS result_state,
            r.run_type,
            CAST(r.period_start_time AS STRING) AS start_time,
            CAST(r.period_end_time AS STRING) AS end_time,
            r.execution_duration_seconds AS execution_duration,
            j.creator_user_name
        FROM {dl.catalog}.{dl.schema}.job_runs_latest r
        LEFT JOIN {dl.catalog}.{dl.schema}.jobs_latest j
            ON r.job_id = j.job_id AND r.workspace_id = j.workspace_id
        WHERE {time_filter}
        {status_filter}
        {workspace_filter}
        {search_filter}
        ORDER BY r.period_start_time DESC
        LIMIT {limit}
    """

    lb_search_filter = ""
    if search:
        safe_search = search.replace("'", "''")
        lb_search_filter = f"AND (r.job_id::text LIKE '%{safe_search}%' OR r.run_id::text LIKE '%{safe_search}%' OR LOWER(j.name) LIKE LOWER('%{safe_search}%'))"

    lb_workspace_filter = ""
    if workspace_id:
        safe_ws = workspace_id.replace("'", "''")
        lb_workspace_filter = f"AND r.workspace_id = '{safe_ws}'"

    if hours is not None:
        lb_time_filter = f"r.period_start_time >= CURRENT_TIMESTAMP - INTERVAL '{hours} hours'"
    else:
        lb_time_filter = f"r.period_start_time >= CURRENT_DATE - INTERVAL '{days} days'"

    lb_query = f"""
        SELECT
            r.job_id::bigint AS job_id,
            j.name AS job_name,
            r.run_id::bigint AS run_id,
            r.workspace_id::text AS workspace_id,
            COALESCE(r.result_state, 'RUNNING') AS result_state,
            r.run_type,
            r.period_start_time::text AS start_time,
            r.period_end_time::text AS end_time,
            r.execution_duration_seconds AS execution_duration,
            j.creator_user_name
        FROM {dl.schema}.lb_job_runs_latest r
        LEFT JOIN {dl.schema}.lb_jobs_latest j
            ON r.job_id = j.job_id AND r.workspace_id = j.workspace_id
        WHERE {lb_time_filter}
        {status_filter}
        {lb_workspace_filter}
        {lb_search_filter}
        ORDER BY r.period_start_time DESC
        LIMIT {limit}
    """

    result = await asyncio.to_thread(dl.execute_query, query, None, True, True, lb_query)

    return [
        JobRun(
            job_id=row[0],
            job_name=row[1],
            run_id=row[2],
            workspace_id=row[3],
            result_state=row[4],
            run_type=row[5],
            start_time=row[6],
            end_time=row[7],
            execution_duration=row[8],
            creator_user_name=row[9],
        )
        for row in result.data
    ]


@router.get("/summary", response_model=RunSummary)
async def get_run_summary(
    days: int = Query(7, ge=1, le=90, description="Number of days to look back"),
):
    """Get summary statistics for job runs."""
    dl = get_data_layer()

    query = f"""
        SELECT
            COUNT(*) AS total_runs,
            SUM(CASE WHEN result_state = 'SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded,
            SUM(CASE WHEN result_state IN ('FAILED', 'ERROR', 'TIMED_OUT') THEN 1 ELSE 0 END) AS failed,
            SUM(CASE WHEN result_state IS NULL OR result_state = 'RUNNING' THEN 1 ELSE 0 END) AS running
        FROM {dl.catalog}.{dl.schema}.job_runs_latest
        WHERE period_start_time >= CURRENT_DATE - INTERVAL {days} DAY
    """

    lb_query = f"""
        SELECT
            COUNT(*) AS total_runs,
            SUM(CASE WHEN result_state = 'SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded,
            SUM(CASE WHEN result_state IN ('FAILED', 'ERROR', 'TIMED_OUT') THEN 1 ELSE 0 END) AS failed,
            SUM(CASE WHEN result_state IS NULL OR result_state = 'RUNNING' THEN 1 ELSE 0 END) AS running
        FROM {dl.schema}.lb_job_runs_latest
        WHERE period_start_time >= CURRENT_DATE - INTERVAL '{days} days'
    """

    result = await asyncio.to_thread(dl.execute_query, query, None, True, True, lb_query)

    if not result.data:
        return RunSummary(total_runs=0, succeeded=0, failed=0, running=0, success_rate=0)

    row = result.data[0]
    total = int(row[0] or 0)
    succeeded = int(row[1] or 0)
    failed = int(row[2] or 0)
    running = int(row[3] or 0)

    return RunSummary(
        total_runs=total,
        succeeded=succeeded,
        failed=failed,
        running=running,
        success_rate=round(succeeded / total * 100, 2) if total > 0 else 0,
    )


@router.get("/daily")
async def get_daily_runs(
    days: int = Query(30, ge=1, le=90, description="Number of days to look back"),
):
    """Get daily job run counts."""
    dl = get_data_layer()

    query = f"""
        SELECT
            DATE(period_start_time) AS run_date,
            COUNT(*) AS total_runs,
            SUM(CASE WHEN result_state = 'SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded,
            SUM(CASE WHEN result_state IN ('FAILED', 'ERROR', 'TIMED_OUT') THEN 1 ELSE 0 END) AS failed
        FROM {dl.catalog}.{dl.schema}.job_runs_latest
        WHERE period_start_time >= CURRENT_DATE - INTERVAL {days} DAY
        GROUP BY DATE(period_start_time)
        ORDER BY run_date
    """

    lb_query = f"""
        SELECT
            period_start_time::date AS run_date,
            COUNT(*) AS total_runs,
            SUM(CASE WHEN result_state = 'SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded,
            SUM(CASE WHEN result_state IN ('FAILED', 'ERROR', 'TIMED_OUT') THEN 1 ELSE 0 END) AS failed
        FROM {dl.schema}.lb_job_runs_latest
        WHERE period_start_time >= CURRENT_DATE - INTERVAL '{days} days'
        GROUP BY period_start_time::date
        ORDER BY run_date
    """

    result = await asyncio.to_thread(dl.execute_query, query, None, True, True, lb_query)

    return [
        {
            "date": str(row[0]),
            "total": int(row[1] or 0),
            "succeeded": int(row[2] or 0),
            "failed": int(row[3] or 0),
        }
        for row in result.data
    ]


@router.get("/by-type")
async def get_runs_by_type(
    days: int = Query(7, ge=1, le=90, description="Number of days to look back"),
):
    """Get job runs grouped by run type."""
    dl = get_data_layer()

    query = f"""
        SELECT
            COALESCE(run_type, 'UNKNOWN') AS run_type,
            COUNT(*) AS count
        FROM {dl.catalog}.{dl.schema}.job_runs_latest
        WHERE period_start_time >= CURRENT_DATE - INTERVAL {days} DAY
        GROUP BY run_type
        ORDER BY count DESC
    """

    lb_query = f"""
        SELECT
            COALESCE(run_type, 'UNKNOWN') AS run_type,
            COUNT(*) AS count
        FROM {dl.schema}.lb_job_runs_latest
        WHERE period_start_time >= CURRENT_DATE - INTERVAL '{days} days'
        GROUP BY run_type
        ORDER BY count DESC
    """

    result = await asyncio.to_thread(dl.execute_query, query, None, True, True, lb_query)

    return [{"run_type": row[0], "count": int(row[1])} for row in result.data]


@router.get("/matrix")
async def get_jobs_matrix(
    limit: int = Query(50, ge=1, le=200, description="Maximum number of jobs"),
    runs_per_job: int = Query(10, ge=1, le=50, description="Runs per job to show"),
):
    """Get matrix view data: last N runs for each job."""
    dl = get_data_layer()

    query = f"""
        WITH ranked_runs AS (
            SELECT
                CAST(r.job_id AS BIGINT) AS job_id,
                j.name AS job_name,
                CAST(r.run_id AS BIGINT) AS run_id,
                r.result_state,
                CAST(r.period_start_time AS STRING) AS start_time,
                r.execution_duration_seconds AS duration_seconds,
                ROW_NUMBER() OVER (PARTITION BY r.job_id ORDER BY r.period_start_time DESC) AS rn
            FROM {dl.catalog}.{dl.schema}.job_runs_latest r
            LEFT JOIN {dl.catalog}.{dl.schema}.jobs_latest j
                ON r.job_id = j.job_id AND r.workspace_id = j.workspace_id
            WHERE r.period_start_time >= CURRENT_DATE - INTERVAL 30 DAY
        )
        SELECT
            job_id,
            job_name,
            run_id,
            result_state,
            start_time,
            duration_seconds,
            rn
        FROM ranked_runs
        WHERE rn <= {runs_per_job}
        ORDER BY job_id, rn
        LIMIT {limit * runs_per_job}
    """

    lb_query = f"""
        WITH
        ranked_runs AS (
            SELECT
                r.job_id::bigint AS job_id,
                j.name AS job_name,
                r.run_id::bigint AS run_id,
                r.result_state,
                r.period_start_time::text AS start_time,
                r.execution_duration_seconds AS duration_seconds,
                ROW_NUMBER() OVER (PARTITION BY r.job_id ORDER BY r.period_start_time DESC) AS rn
            FROM {dl.schema}.lb_job_runs_latest r
            LEFT JOIN {dl.schema}.lb_jobs_latest j
                ON r.job_id = j.job_id AND r.workspace_id = j.workspace_id
            WHERE r.period_start_time >= CURRENT_DATE - INTERVAL '30 days'
        )
        SELECT
            job_id,
            job_name,
            run_id,
            result_state,
            start_time,
            duration_seconds,
            rn
        FROM ranked_runs
        WHERE rn <= {runs_per_job}
        ORDER BY job_id, rn
        LIMIT {limit * runs_per_job}
    """

    result = await asyncio.to_thread(dl.execute_query, query, None, True, True, lb_query)

    # Group by job
    jobs = {}
    for row in result.data:
        job_id = row[0]
        if job_id not in jobs:
            jobs[job_id] = {
                "job_id": job_id,
                "job_name": row[1] or f"Job {job_id}",
                "runs": [],
            }
        jobs[job_id]["runs"].append({
            "run_id": row[2],
            "result_state": row[3],
            "start_time": row[4],
            "duration_seconds": float(row[5]) if row[5] is not None else None,
        })

    return list(jobs.values())[:limit]


@router.get("/overlaps")
async def get_overlapping_runs(
    hours: int = Query(24, ge=1, le=168, description="Hours to look back"),
):
    """Get overlapping job runs for Gantt view."""
    dl = get_data_layer()

    query = f"""
        WITH runs AS (
            SELECT
                CAST(r.job_id AS BIGINT) AS job_id,
                COALESCE(j.name, CAST(r.job_id AS STRING)) AS job_name,
                CAST(r.run_id AS BIGINT) AS run_id,
                r.period_start_time,
                r.period_end_time,
                r.result_state
            FROM {dl.catalog}.{dl.schema}.job_runs_latest r
            LEFT JOIN {dl.catalog}.{dl.schema}.jobs_latest j
                ON r.job_id = j.job_id AND r.workspace_id = j.workspace_id
            WHERE r.period_start_time >= CURRENT_TIMESTAMP - INTERVAL {hours} HOUR
        )
        SELECT
            a.job_id AS job_id_1,
            a.job_name AS job_name_1,
            a.run_id AS run_id_1,
            b.job_id AS job_id_2,
            b.job_name AS job_name_2,
            b.run_id AS run_id_2,
            CAST(GREATEST(a.period_start_time, b.period_start_time) AS STRING) AS overlap_start,
            CAST(LEAST(a.period_end_time, b.period_end_time) AS STRING) AS overlap_end
        FROM runs a
        JOIN runs b ON a.run_id < b.run_id
            AND a.period_start_time < b.period_end_time
            AND a.period_end_time > b.period_start_time
        ORDER BY overlap_start DESC
        LIMIT 100
    """

    lb_query = f"""
        WITH
        runs AS (
            SELECT
                r.job_id::bigint AS job_id,
                COALESCE(j.name, r.job_id::text) AS job_name,
                r.run_id::bigint AS run_id,
                r.period_start_time,
                r.period_end_time,
                r.result_state
            FROM {dl.schema}.lb_job_runs_latest r
            LEFT JOIN {dl.schema}.lb_jobs_latest j
                ON r.job_id = j.job_id AND r.workspace_id = j.workspace_id
            WHERE r.period_start_time >= CURRENT_TIMESTAMP - INTERVAL '{hours} hours'
        )
        SELECT
            a.job_id AS job_id_1,
            a.job_name AS job_name_1,
            a.run_id AS run_id_1,
            b.job_id AS job_id_2,
            b.job_name AS job_name_2,
            b.run_id AS run_id_2,
            GREATEST(a.period_start_time, b.period_start_time)::text AS overlap_start,
            LEAST(a.period_end_time, b.period_end_time)::text AS overlap_end
        FROM runs a
        JOIN runs b ON a.run_id < b.run_id
            AND a.period_start_time < b.period_end_time
            AND a.period_end_time > b.period_start_time
        ORDER BY overlap_start DESC
        LIMIT 100
    """

    result = await asyncio.to_thread(dl.execute_query, query, None, True, True, lb_query)

    return [
        {
            "job_1": {"id": row[0], "name": row[1], "run_id": row[2]},
            "job_2": {"id": row[3], "name": row[4], "run_id": row[5]},
            "overlap_start": row[6],
            "overlap_end": row[7],
        }
        for row in result.data
    ]


@router.get("/concurrent")
async def get_concurrent_jobs_over_time(
    hours: int = Query(24, ge=1, le=168, description="Hours to look back"),
):
    """Get concurrent job count over time."""
    dl = get_data_layer()

    query = f"""
        WITH time_points AS (
            SELECT CURRENT_TIMESTAMP - INTERVAL {hours} HOUR AS time_point
            UNION
            SELECT DISTINCT period_start_time AS time_point
            FROM {dl.catalog}.{dl.schema}.job_runs_latest
            WHERE period_start_time >= CURRENT_TIMESTAMP - INTERVAL {hours} HOUR
              AND period_start_time <= CURRENT_TIMESTAMP
            UNION
            SELECT DISTINCT period_end_time AS time_point
            FROM {dl.catalog}.{dl.schema}.job_runs_latest
            WHERE period_end_time >= CURRENT_TIMESTAMP - INTERVAL {hours} HOUR
              AND period_end_time <= CURRENT_TIMESTAMP
        ),
        concurrent_counts AS (
            SELECT
                tp.time_point,
                COUNT(*) AS concurrent_jobs
            FROM time_points tp
            JOIN {dl.catalog}.{dl.schema}.job_runs_latest r
                ON tp.time_point >= r.period_start_time
                AND tp.time_point <= COALESCE(r.period_end_time, CURRENT_TIMESTAMP)
            WHERE r.period_start_time <= CURRENT_TIMESTAMP
              AND COALESCE(r.period_end_time, CURRENT_TIMESTAMP) >= CURRENT_TIMESTAMP - INTERVAL {hours} HOUR
            GROUP BY tp.time_point
        )
        SELECT
            CAST(time_point AS STRING) AS time_point,
            concurrent_jobs
        FROM concurrent_counts
        ORDER BY time_point
        LIMIT 10000
    """

    lb_query = f"""
        WITH time_points AS (
            SELECT CURRENT_TIMESTAMP - INTERVAL '{hours} hours' AS time_point
            UNION
            SELECT DISTINCT period_start_time AS time_point
            FROM {dl.schema}.lb_job_runs_latest
            WHERE period_start_time >= CURRENT_TIMESTAMP - INTERVAL '{hours} hours'
              AND period_start_time <= CURRENT_TIMESTAMP
            UNION
            SELECT DISTINCT period_end_time AS time_point
            FROM {dl.schema}.lb_job_runs_latest
            WHERE period_end_time >= CURRENT_TIMESTAMP - INTERVAL '{hours} hours'
              AND period_end_time <= CURRENT_TIMESTAMP
        ),
        concurrent_counts AS (
            SELECT
                tp.time_point,
                COUNT(*) AS concurrent_jobs
            FROM time_points tp
            JOIN {dl.schema}.lb_job_runs_latest r
                ON tp.time_point >= r.period_start_time
                AND tp.time_point <= COALESCE(r.period_end_time, CURRENT_TIMESTAMP)
            WHERE r.period_start_time <= CURRENT_TIMESTAMP
              AND COALESCE(r.period_end_time, CURRENT_TIMESTAMP) >= CURRENT_TIMESTAMP - INTERVAL '{hours} hours'
            GROUP BY tp.time_point
        )
        SELECT
            time_point::text AS time_point,
            concurrent_jobs
        FROM concurrent_counts
        ORDER BY time_point
        LIMIT 10000
    """

    result = await asyncio.to_thread(dl.execute_query, query, None, True, True, lb_query)

    return [
        {"time": row[0], "concurrent_jobs": int(row[1])}
        for row in result.data
    ]


class GanttBucket(BaseModel):
    job_id: int
    job_name: Optional[str]
    bucket: str
    succeeded: int
    failed: int
    running: int


@router.get("/gantt", response_model=List[GanttBucket])
async def get_gantt_data(
    hours: int = Query(24, ge=1, le=168, description="Hours to look back"),
):
    """Bucketed per-job run counts for Gantt view. Concurrent is derived by summing across jobs per bucket."""
    dl = get_data_layer()

    if hours <= 6:
        step_minutes = 5
    elif hours <= 12:
        step_minutes = 10
    elif hours <= 24:
        step_minutes = 15
    elif hours <= 48:
        step_minutes = 30
    else:
        step_minutes = 60

    query = f"""
        WITH time_series AS (
            SELECT explode(sequence(
                date_trunc('minute', CURRENT_TIMESTAMP - INTERVAL {hours} HOUR),
                CURRENT_TIMESTAMP,
                INTERVAL {step_minutes} MINUTE
            )) AS bucket
        )
        SELECT
            CAST(r.job_id AS BIGINT) AS job_id,
            j.name AS job_name,
            CAST(ts.bucket AS STRING) AS bucket,
            COUNT(CASE WHEN r.result_state = 'SUCCEEDED' THEN 1 END) AS succeeded,
            COUNT(CASE WHEN r.result_state IN ('FAILED', 'ERROR', 'TIMED_OUT') THEN 1 END) AS failed,
            COUNT(CASE WHEN r.result_state IS NULL OR r.result_state = 'RUNNING' THEN 1 END) AS running
        FROM time_series ts
        JOIN {dl.catalog}.{dl.schema}.job_runs_latest r
            ON ts.bucket >= r.period_start_time
            AND ts.bucket <= COALESCE(r.period_end_time, CURRENT_TIMESTAMP)
            AND r.period_start_time <= CURRENT_TIMESTAMP
            AND COALESCE(r.period_end_time, CURRENT_TIMESTAMP) >= CURRENT_TIMESTAMP - INTERVAL {hours} HOUR
        LEFT JOIN {dl.catalog}.{dl.schema}.jobs_latest j
            ON r.job_id = j.job_id AND r.workspace_id = j.workspace_id
        GROUP BY r.job_id, j.name, ts.bucket
        ORDER BY job_id, bucket
    """

    lb_query = f"""
        WITH time_series AS (
            SELECT generate_series(
                date_trunc('minute', CURRENT_TIMESTAMP - INTERVAL '{hours} hours'),
                CURRENT_TIMESTAMP,
                INTERVAL '{step_minutes} minutes'
            ) AS bucket
        )
        SELECT
            r.job_id::bigint AS job_id,
            j.name AS job_name,
            ts.bucket::text AS bucket,
            COUNT(CASE WHEN r.result_state = 'SUCCEEDED' THEN 1 END) AS succeeded,
            COUNT(CASE WHEN r.result_state IN ('FAILED', 'ERROR', 'TIMED_OUT') THEN 1 END) AS failed,
            COUNT(CASE WHEN r.result_state IS NULL OR r.result_state = 'RUNNING' THEN 1 END) AS running
        FROM time_series ts
        JOIN {dl.schema}.lb_job_runs_latest r
            ON ts.bucket >= r.period_start_time
            AND ts.bucket <= COALESCE(r.period_end_time, CURRENT_TIMESTAMP)
            AND r.period_start_time <= CURRENT_TIMESTAMP
            AND COALESCE(r.period_end_time, CURRENT_TIMESTAMP) >= CURRENT_TIMESTAMP - INTERVAL '{hours} hours'
        LEFT JOIN {dl.schema}.lb_jobs_latest j
            ON r.job_id = j.job_id AND r.workspace_id = j.workspace_id
        GROUP BY r.job_id, j.name, ts.bucket
        ORDER BY job_id, bucket
    """

    result = await asyncio.to_thread(dl.execute_query, query, None, True, True, lb_query)

    return [
        GanttBucket(
            job_id=row[0],
            job_name=row[1],
            bucket=str(row[2]),
            succeeded=int(row[3] or 0),
            failed=int(row[4] or 0),
            running=int(row[5] or 0),
        )
        for row in result.data
    ]


class CancelRequest(BaseModel):
    run_id: int
    workspace_id: str


@router.post("/cancel")
async def cancel_job_run(req: CancelRequest):
    """Cancel a running job on any workspace using a cross-workspace SDK client."""
    from app import data_layer, _workspace_urls, _workspace_urls_loaded
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.config import Config

    if not data_layer or not data_layer._workspace_client:
        raise HTTPException(status_code=503, detail="Workspace client not initialized")

    if not _workspace_urls_loaded:
        from app import get_workspace_urls
        try:
            await get_workspace_urls()
        except Exception:
            pass

    workspace_url = _workspace_urls.get(req.workspace_id, {}).get("url")
    if not workspace_url:
        raise HTTPException(status_code=400, detail=f"Unknown workspace {req.workspace_id}")

    try:
        # Create a client targeting the specific workspace.
        # In Databricks Apps, the service principal's OAuth credentials
        # work across workspaces in the same account.
        home_config = data_layer._workspace_client.config
        target_client = WorkspaceClient(
            config=Config(
                host=workspace_url,
                client_id=home_config.client_id,
                client_secret=home_config.client_secret,
                token=home_config.token,
            )
        )
        target_client.jobs.cancel_run(run_id=req.run_id)
        return {"status": "cancelled", "run_id": req.run_id}
    except Exception as e:
        err_msg = str(e)
        if "INVALID_PARAMETER_VALUE" in err_msg:
            raise HTTPException(status_code=404, detail=f"Run {req.run_id} not found or already completed")
        if "PERMISSION_DENIED" in err_msg or "403" in err_msg:
            raise HTTPException(status_code=403, detail=f"No permission to cancel run {req.run_id} in workspace {req.workspace_id}")
        raise HTTPException(status_code=500, detail=err_msg[:300])
