"""
Jobs Router - Job monitoring endpoints
"""
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
    days: int = Query(7, ge=1, le=90, description="Number of days to look back"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results"),
    status: Optional[str] = Query(None, description="Filter by status"),
):
    """Get recent job runs."""
    dl = get_data_layer()

    status_filter = ""
    if status:
        status_filter = f"AND r.result_state = '{status}'"

    query = f"""
        SELECT
            CAST(r.job_id AS BIGINT) AS job_id,
            j.name AS job_name,
            CAST(r.run_id AS BIGINT) AS run_id,
            r.result_state,
            r.run_type,
            CAST(r.period_start_time AS STRING) AS start_time,
            CAST(r.period_end_time AS STRING) AS end_time,
            r.execution_duration_seconds AS execution_duration,
            j.creator_user_name
        FROM system.lakeflow.job_run_timeline r
        LEFT JOIN system.lakeflow.jobs j
            ON r.job_id = j.job_id AND r.workspace_id = j.workspace_id
        WHERE r.period_start_time >= CURRENT_DATE - INTERVAL {days} DAY
        {status_filter}
        ORDER BY r.period_start_time DESC
        LIMIT {limit}
    """

    result = dl.execute_query(query)

    return [
        JobRun(
            job_id=row[0],
            job_name=row[1],
            run_id=row[2],
            result_state=row[3],
            run_type=row[4],
            start_time=row[5],
            end_time=row[6],
            execution_duration=row[7],
            creator_user_name=row[8],
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
        FROM system.lakeflow.job_run_timeline
        WHERE period_start_time >= CURRENT_DATE - INTERVAL {days} DAY
    """

    result = dl.execute_query(query)

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
        FROM system.lakeflow.job_run_timeline
        WHERE period_start_time >= CURRENT_DATE - INTERVAL {days} DAY
        GROUP BY DATE(period_start_time)
        ORDER BY run_date
    """

    result = dl.execute_query(query)

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
        FROM system.lakeflow.job_run_timeline
        WHERE period_start_time >= CURRENT_DATE - INTERVAL {days} DAY
        GROUP BY run_type
        ORDER BY count DESC
    """

    result = dl.execute_query(query)

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
            FROM system.lakeflow.job_run_timeline r
            LEFT JOIN system.lakeflow.jobs j
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

    result = dl.execute_query(query)

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
                COALESCE(j.name, r.run_name) AS job_name,
                CAST(r.run_id AS BIGINT) AS run_id,
                r.period_start_time,
                r.period_end_time,
                r.result_state
            FROM system.lakeflow.job_run_timeline r
            LEFT JOIN system.lakeflow.jobs j
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

    result = dl.execute_query(query)

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
            SELECT DISTINCT period_start_time AS time_point
            FROM system.lakeflow.job_run_timeline
            WHERE period_start_time >= CURRENT_TIMESTAMP - INTERVAL {hours} HOUR
            UNION
            SELECT DISTINCT period_end_time AS time_point
            FROM system.lakeflow.job_run_timeline
            WHERE period_end_time >= CURRENT_TIMESTAMP - INTERVAL {hours} HOUR
        ),
        concurrent_counts AS (
            SELECT
                tp.time_point,
                COUNT(*) AS concurrent_jobs
            FROM time_points tp
            JOIN system.lakeflow.job_run_timeline r
                ON tp.time_point >= r.period_start_time
                AND tp.time_point <= COALESCE(r.period_end_time, CURRENT_TIMESTAMP)
            WHERE r.period_start_time >= CURRENT_TIMESTAMP - INTERVAL {hours} HOUR
            GROUP BY tp.time_point
        )
        SELECT
            CAST(time_point AS STRING) AS time_point,
            concurrent_jobs
        FROM concurrent_counts
        ORDER BY time_point
    """

    result = dl.execute_query(query)

    return [
        {"time": row[0], "concurrent_jobs": int(row[1])}
        for row in result.data
    ]
