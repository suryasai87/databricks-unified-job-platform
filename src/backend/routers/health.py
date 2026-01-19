"""
Health Router - Job health monitoring endpoints
"""
from typing import List, Optional

from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel

router = APIRouter()


class FailedJob(BaseModel):
    job_id: int
    job_name: Optional[str]
    total_runs: int
    failed_runs: int
    success_rate: float
    last_failure: Optional[str]


class ProlongedJob(BaseModel):
    job_id: int
    job_name: Optional[str]
    run_id: int
    start_time: str
    duration_seconds: float
    avg_duration_seconds: float
    status: str  # warning or critical


class Anomaly(BaseModel):
    job_id: int
    job_name: Optional[str]
    metric: str
    current_value: float
    avg_value: float
    std_dev: float
    z_score: float
    severity: str


def get_data_layer():
    """Get data layer from app state."""
    from app import data_layer
    if not data_layer:
        raise HTTPException(status_code=503, detail="Data layer not initialized")
    return data_layer


@router.get("/failed-jobs", response_model=List[FailedJob])
async def get_failed_jobs(
    days: int = Query(7, ge=1, le=90, description="Number of days to look back"),
    min_runs: int = Query(3, ge=1, description="Minimum runs to include"),
    limit: int = Query(20, ge=1, le=100, description="Maximum results"),
):
    """Get jobs with recent failures."""
    dl = get_data_layer()

    query = f"""
        SELECT
            job_id,
            job_name,
            COUNT(*) AS total_runs,
            SUM(CASE WHEN result_state = 'FAILED' THEN 1 ELSE 0 END) AS failed_runs,
            MAX(CASE WHEN result_state = 'FAILED' THEN CAST(period_start_time AS STRING) END) AS last_failure
        FROM system.lakeflow.job_run_timeline
        WHERE period_start_time >= CURRENT_DATE - INTERVAL {days} DAY
        GROUP BY job_id, job_name
        HAVING COUNT(*) >= {min_runs}
           AND SUM(CASE WHEN result_state = 'FAILED' THEN 1 ELSE 0 END) > 0
        ORDER BY failed_runs DESC
        LIMIT {limit}
    """

    result = dl.execute_query(query)

    return [
        FailedJob(
            job_id=row[0],
            job_name=row[1],
            total_runs=int(row[2] or 0),
            failed_runs=int(row[3] or 0),
            success_rate=round((int(row[2] or 0) - int(row[3] or 0)) / int(row[2] or 1) * 100, 2),
            last_failure=row[4],
        )
        for row in result.data
    ]


@router.get("/prolonged-jobs", response_model=List[ProlongedJob])
async def get_prolonged_jobs(
    warning_multiplier: float = Query(1.5, ge=1.0, description="Warning threshold multiplier"),
    critical_multiplier: float = Query(2.0, ge=1.0, description="Critical threshold multiplier"),
):
    """Get currently running jobs that exceed expected duration."""
    dl = get_data_layer()

    query = f"""
        WITH running_jobs AS (
            SELECT
                job_id,
                job_name,
                run_id,
                period_start_time,
                (UNIX_TIMESTAMP(CURRENT_TIMESTAMP) - UNIX_TIMESTAMP(period_start_time)) AS current_duration_seconds
            FROM system.lakeflow.job_run_timeline
            WHERE result_state IS NULL OR result_state = 'RUNNING'
        ),
        historical_avg AS (
            SELECT
                job_id,
                AVG(execution_duration / 1000.0) AS avg_duration_seconds
            FROM system.lakeflow.job_run_timeline
            WHERE result_state = 'SUCCESS'
              AND period_start_time >= CURRENT_DATE - INTERVAL 30 DAY
            GROUP BY job_id
        )
        SELECT
            r.job_id,
            r.job_name,
            r.run_id,
            CAST(r.period_start_time AS STRING) AS start_time,
            r.current_duration_seconds,
            COALESCE(h.avg_duration_seconds, 0) AS avg_duration_seconds
        FROM running_jobs r
        LEFT JOIN historical_avg h ON r.job_id = h.job_id
        WHERE r.current_duration_seconds > COALESCE(h.avg_duration_seconds * {warning_multiplier}, 300)
        ORDER BY r.current_duration_seconds DESC
    """

    result = dl.execute_query(query)

    return [
        ProlongedJob(
            job_id=row[0],
            job_name=row[1],
            run_id=row[2],
            start_time=row[3],
            duration_seconds=float(row[4] or 0),
            avg_duration_seconds=float(row[5] or 0),
            status="critical" if float(row[4] or 0) > float(row[5] or 0) * critical_multiplier else "warning",
        )
        for row in result.data
    ]


@router.get("/anomalies", response_model=List[Anomaly])
async def get_anomalies(
    days: int = Query(7, ge=1, le=30, description="Days of recent data to check"),
    baseline_days: int = Query(30, ge=7, le=90, description="Days for baseline calculation"),
    z_threshold: float = Query(2.0, ge=1.0, description="Z-score threshold for anomalies"),
):
    """Detect anomalies in job execution patterns using z-score."""
    dl = get_data_layer()

    query = f"""
        WITH recent_stats AS (
            SELECT
                job_id,
                job_name,
                AVG(execution_duration / 1000.0) AS recent_avg_duration,
                COUNT(*) AS recent_runs,
                SUM(CASE WHEN result_state = 'FAILED' THEN 1 ELSE 0 END) AS recent_failures
            FROM system.lakeflow.job_run_timeline
            WHERE period_start_time >= CURRENT_DATE - INTERVAL {days} DAY
              AND result_state IN ('SUCCESS', 'FAILED')
            GROUP BY job_id, job_name
        ),
        baseline_stats AS (
            SELECT
                job_id,
                AVG(execution_duration / 1000.0) AS baseline_avg_duration,
                STDDEV(execution_duration / 1000.0) AS baseline_stddev_duration,
                COUNT(*) AS baseline_runs,
                AVG(CASE WHEN result_state = 'FAILED' THEN 1.0 ELSE 0.0 END) AS baseline_failure_rate,
                STDDEV(CASE WHEN result_state = 'FAILED' THEN 1.0 ELSE 0.0 END) AS baseline_failure_stddev
            FROM system.lakeflow.job_run_timeline
            WHERE period_start_time >= CURRENT_DATE - INTERVAL {baseline_days} DAY
              AND period_start_time < CURRENT_DATE - INTERVAL {days} DAY
              AND result_state IN ('SUCCESS', 'FAILED')
            GROUP BY job_id
            HAVING COUNT(*) >= 5
        )
        SELECT
            r.job_id,
            r.job_name,
            'duration' AS metric,
            r.recent_avg_duration AS current_value,
            b.baseline_avg_duration AS avg_value,
            b.baseline_stddev_duration AS std_dev,
            CASE
                WHEN b.baseline_stddev_duration > 0
                THEN (r.recent_avg_duration - b.baseline_avg_duration) / b.baseline_stddev_duration
                ELSE 0
            END AS z_score
        FROM recent_stats r
        JOIN baseline_stats b ON r.job_id = b.job_id
        WHERE b.baseline_stddev_duration > 0
          AND ABS((r.recent_avg_duration - b.baseline_avg_duration) / b.baseline_stddev_duration) >= {z_threshold}
        ORDER BY ABS((r.recent_avg_duration - b.baseline_avg_duration) / b.baseline_stddev_duration) DESC
        LIMIT 20
    """

    result = dl.execute_query(query)

    return [
        Anomaly(
            job_id=row[0],
            job_name=row[1],
            metric=row[2],
            current_value=round(float(row[3] or 0), 2),
            avg_value=round(float(row[4] or 0), 2),
            std_dev=round(float(row[5] or 0), 2),
            z_score=round(float(row[6] or 0), 2),
            severity="critical" if abs(float(row[6] or 0)) >= 3.0 else "warning",
        )
        for row in result.data
    ]


@router.get("/retry-stats")
async def get_retry_stats(
    days: int = Query(7, ge=1, le=90, description="Number of days to look back"),
    limit: int = Query(20, ge=1, le=100, description="Maximum results"),
):
    """Get jobs with high retry rates."""
    dl = get_data_layer()

    query = f"""
        WITH job_runs AS (
            SELECT
                job_id,
                job_name,
                run_id,
                attempt_number
            FROM system.lakeflow.job_run_timeline
            WHERE period_start_time >= CURRENT_DATE - INTERVAL {days} DAY
        )
        SELECT
            job_id,
            job_name,
            COUNT(DISTINCT run_id) AS unique_runs,
            COUNT(*) AS total_attempts,
            SUM(CASE WHEN attempt_number > 1 THEN 1 ELSE 0 END) AS retry_attempts,
            MAX(attempt_number) AS max_attempts
        FROM job_runs
        GROUP BY job_id, job_name
        HAVING SUM(CASE WHEN attempt_number > 1 THEN 1 ELSE 0 END) > 0
        ORDER BY retry_attempts DESC
        LIMIT {limit}
    """

    result = dl.execute_query(query)

    return [
        {
            "job_id": row[0],
            "job_name": row[1],
            "unique_runs": int(row[2] or 0),
            "total_attempts": int(row[3] or 0),
            "retry_attempts": int(row[4] or 0),
            "max_attempts": int(row[5] or 0),
            "retry_rate_pct": round(int(row[4] or 0) / max(int(row[2] or 1), 1) * 100, 2),
        }
        for row in result.data
    ]


@router.get("/sla-status")
async def get_sla_status(
    days: int = Query(7, ge=1, le=90, description="Number of days to look back"),
    sla_multiplier: float = Query(2.0, ge=1.0, description="SLA violation multiplier vs avg"),
):
    """Get SLA compliance status for jobs."""
    dl = get_data_layer()

    query = f"""
        WITH job_durations AS (
            SELECT
                job_id,
                job_name,
                execution_duration / 1000.0 AS duration_seconds
            FROM system.lakeflow.job_run_timeline
            WHERE period_start_time >= CURRENT_DATE - INTERVAL {days} DAY
              AND result_state = 'SUCCESS'
        ),
        job_stats AS (
            SELECT
                job_id,
                job_name,
                COUNT(*) AS total_runs,
                AVG(duration_seconds) AS avg_duration
            FROM job_durations
            GROUP BY job_id, job_name
        ),
        sla_violations AS (
            SELECT
                d.job_id,
                COUNT(*) AS violation_count
            FROM job_durations d
            JOIN job_stats s ON d.job_id = s.job_id
            WHERE d.duration_seconds > s.avg_duration * {sla_multiplier}
            GROUP BY d.job_id
        )
        SELECT
            s.job_id,
            s.job_name,
            s.total_runs,
            ROUND(s.avg_duration, 2) AS avg_duration_seconds,
            COALESCE(v.violation_count, 0) AS sla_violations,
            ROUND((s.total_runs - COALESCE(v.violation_count, 0)) * 100.0 / s.total_runs, 2) AS compliance_rate
        FROM job_stats s
        LEFT JOIN sla_violations v ON s.job_id = v.job_id
        ORDER BY sla_violations DESC NULLS LAST, s.total_runs DESC
        LIMIT 50
    """

    result = dl.execute_query(query)

    return [
        {
            "job_id": row[0],
            "job_name": row[1],
            "total_runs": int(row[2] or 0),
            "avg_duration_seconds": float(row[3] or 0),
            "sla_violations": int(row[4] or 0),
            "compliance_rate_pct": float(row[5] or 100),
            "status": "compliant" if float(row[5] or 100) >= 95 else "at_risk" if float(row[5] or 100) >= 80 else "non_compliant",
        }
        for row in result.data
    ]


@router.get("/duration-percentiles")
async def get_duration_percentiles(
    days: int = Query(30, ge=1, le=90, description="Number of days to analyze"),
):
    """Get duration percentiles (p50, p90, p95, p99) for jobs."""
    dl = get_data_layer()

    query = f"""
        SELECT
            job_id,
            job_name,
            COUNT(*) AS run_count,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY execution_duration / 1000.0), 2) AS p50,
            ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY execution_duration / 1000.0), 2) AS p90,
            ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY execution_duration / 1000.0), 2) AS p95,
            ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY execution_duration / 1000.0), 2) AS p99
        FROM system.lakeflow.job_run_timeline
        WHERE period_start_time >= CURRENT_DATE - INTERVAL {days} DAY
          AND result_state = 'SUCCESS'
        GROUP BY job_id, job_name
        HAVING COUNT(*) >= 5
        ORDER BY p50 DESC
        LIMIT 50
    """

    result = dl.execute_query(query)

    return [
        {
            "job_id": row[0],
            "job_name": row[1],
            "run_count": int(row[2] or 0),
            "p50_seconds": float(row[3] or 0),
            "p90_seconds": float(row[4] or 0),
            "p95_seconds": float(row[5] or 0),
            "p99_seconds": float(row[6] or 0),
        }
        for row in result.data
    ]
