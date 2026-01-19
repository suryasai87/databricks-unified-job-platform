"""
Tags Router - Tag correlation and management endpoints
"""
import os
from typing import List, Optional

from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel

router = APIRouter()


class TagCorrelation(BaseModel):
    correlation_id: int
    job_run_id: Optional[int]
    notebook_path: Optional[str]
    adf_pipeline_name: Optional[str]
    adf_run_id: Optional[str]
    project_code: Optional[str]
    department: Optional[str]
    environment: Optional[str]
    run_status: Optional[str]
    run_start_time: Optional[str]


class TagPolicy(BaseModel):
    tag_key: str
    tag_display_name: Optional[str]
    tag_description: Optional[str]
    tag_category: Optional[str]
    is_required: bool
    allowed_values: Optional[List[str]]
    default_value: Optional[str]
    validation_regex: Optional[str]


def get_data_layer():
    """Get data layer from app state."""
    from app import data_layer
    if not data_layer:
        raise HTTPException(status_code=503, detail="Data layer not initialized")
    return data_layer


@router.get("/correlations", response_model=List[TagCorrelation])
async def get_tag_correlations(
    days: int = Query(7, ge=1, le=90, description="Number of days to look back"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum results"),
    project_code: Optional[str] = Query(None, description="Filter by project code"),
    department: Optional[str] = Query(None, description="Filter by department"),
):
    """Get recent tag correlation records."""
    dl = get_data_layer()
    catalog = os.getenv("CATALOG", "hls_amer_catalog")
    schema = os.getenv("SCHEMA", "cost_management")

    filters = []
    if project_code:
        filters.append(f"project_code = '{project_code}'")
    if department:
        filters.append(f"department = '{department}'")

    where_clause = " AND ".join(filters) if filters else "1=1"

    query = f"""
        SELECT
            correlation_id,
            job_run_id,
            notebook_path,
            adf_pipeline_name,
            adf_run_id,
            project_code,
            department,
            environment,
            run_status,
            CAST(run_start_time AS STRING) AS run_start_time
        FROM {catalog}.{schema}.serverless_tag_correlation
        WHERE run_start_time >= CURRENT_DATE - INTERVAL {days} DAY
          AND {where_clause}
        ORDER BY run_start_time DESC
        LIMIT {limit}
    """

    try:
        result = dl.execute_query(query)

        return [
            TagCorrelation(
                correlation_id=row[0],
                job_run_id=row[1],
                notebook_path=row[2],
                adf_pipeline_name=row[3],
                adf_run_id=row[4],
                project_code=row[5],
                department=row[6],
                environment=row[7],
                run_status=row[8],
                run_start_time=row[9],
            )
            for row in result.data
        ]

    except Exception as e:
        # Table might not exist yet
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
            return []
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/policies", response_model=List[TagPolicy])
async def get_tag_policies(
    active_only: bool = Query(True, description="Only return active policies"),
):
    """Get tag policy definitions."""
    dl = get_data_layer()
    catalog = os.getenv("CATALOG", "hls_amer_catalog")
    schema = os.getenv("SCHEMA", "cost_management")

    active_filter = "AND is_active = TRUE" if active_only else ""

    query = f"""
        SELECT
            tag_key,
            tag_display_name,
            tag_description,
            tag_category,
            is_required,
            allowed_values,
            default_value,
            validation_regex
        FROM {catalog}.{schema}.tag_policy_definitions
        WHERE 1=1 {active_filter}
        ORDER BY is_required DESC, tag_key
    """

    try:
        result = dl.execute_query(query)

        return [
            TagPolicy(
                tag_key=row[0],
                tag_display_name=row[1],
                tag_description=row[2],
                tag_category=row[3],
                is_required=bool(row[4]),
                allowed_values=row[5] if isinstance(row[5], list) else None,
                default_value=row[6],
                validation_regex=row[7],
            )
            for row in result.data
        ]

    except Exception as e:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
            # Return default policies
            return [
                TagPolicy(tag_key="project_code", tag_display_name="Project Code", is_required=True),
                TagPolicy(tag_key="department", tag_display_name="Department", is_required=True),
                TagPolicy(tag_key="environment", tag_display_name="Environment", is_required=True, default_value="dev"),
                TagPolicy(tag_key="cost_center", tag_display_name="Cost Center", is_required=True),
            ]
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/summary")
async def get_tag_summary(
    days: int = Query(30, ge=1, le=90, description="Number of days to analyze"),
):
    """Get summary of tag usage."""
    dl = get_data_layer()
    catalog = os.getenv("CATALOG", "hls_amer_catalog")
    schema = os.getenv("SCHEMA", "cost_management")

    query = f"""
        SELECT
            COUNT(*) AS total_records,
            COUNT(DISTINCT project_code) AS unique_projects,
            COUNT(DISTINCT department) AS unique_departments,
            COUNT(DISTINCT adf_pipeline_name) AS unique_pipelines,
            SUM(CASE WHEN project_code IS NOT NULL THEN 1 ELSE 0 END) AS records_with_project,
            SUM(CASE WHEN department IS NOT NULL THEN 1 ELSE 0 END) AS records_with_department,
            SUM(CASE WHEN run_status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_runs,
            SUM(CASE WHEN run_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_runs
        FROM {catalog}.{schema}.serverless_tag_correlation
        WHERE run_start_time >= CURRENT_DATE - INTERVAL {days} DAY
    """

    try:
        result = dl.execute_query(query)

        if not result.data or not result.data[0]:
            return {
                "total_records": 0,
                "unique_projects": 0,
                "unique_departments": 0,
                "unique_pipelines": 0,
                "tagging_completeness": {
                    "project_code_pct": 0,
                    "department_pct": 0,
                },
                "run_status": {
                    "successful": 0,
                    "failed": 0,
                },
            }

        row = result.data[0]
        total = int(row[0] or 0)

        return {
            "total_records": total,
            "unique_projects": int(row[1] or 0),
            "unique_departments": int(row[2] or 0),
            "unique_pipelines": int(row[3] or 0),
            "tagging_completeness": {
                "project_code_pct": round(int(row[4] or 0) / max(total, 1) * 100, 2),
                "department_pct": round(int(row[5] or 0) / max(total, 1) * 100, 2),
            },
            "run_status": {
                "successful": int(row[6] or 0),
                "failed": int(row[7] or 0),
            },
        }

    except Exception as e:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
            return {
                "total_records": 0,
                "message": "Tag correlation table not yet configured. Run the setup script.",
                "setup_required": True,
            }
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/by-pipeline")
async def get_tags_by_pipeline(
    days: int = Query(30, ge=1, le=90, description="Number of days to analyze"),
    limit: int = Query(20, ge=1, le=100, description="Maximum pipelines to return"),
):
    """Get tag correlation stats by ADF pipeline."""
    dl = get_data_layer()
    catalog = os.getenv("CATALOG", "hls_amer_catalog")
    schema = os.getenv("SCHEMA", "cost_management")

    query = f"""
        SELECT
            COALESCE(adf_pipeline_name, 'Direct Execution') AS pipeline_name,
            COUNT(*) AS job_runs,
            COUNT(DISTINCT project_code) AS unique_projects,
            COUNT(DISTINCT department) AS unique_departments,
            SUM(CASE WHEN run_status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful,
            SUM(CASE WHEN run_status = 'FAILED' THEN 1 ELSE 0 END) AS failed
        FROM {catalog}.{schema}.serverless_tag_correlation
        WHERE run_start_time >= CURRENT_DATE - INTERVAL {days} DAY
        GROUP BY adf_pipeline_name
        ORDER BY job_runs DESC
        LIMIT {limit}
    """

    try:
        result = dl.execute_query(query)

        return [
            {
                "pipeline_name": row[0],
                "job_runs": int(row[1] or 0),
                "unique_projects": int(row[2] or 0),
                "unique_departments": int(row[3] or 0),
                "successful": int(row[4] or 0),
                "failed": int(row[5] or 0),
                "success_rate_pct": round(int(row[4] or 0) / max(int(row[1] or 1), 1) * 100, 2),
            }
            for row in result.data
        ]

    except Exception as e:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
            return []
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/unmatched")
async def get_unmatched_runs(
    days: int = Query(7, ge=1, le=30, description="Number of days to look back"),
    limit: int = Query(50, ge=1, le=200, description="Maximum results"),
):
    """Get serverless runs without tag correlation."""
    dl = get_data_layer()
    catalog = os.getenv("CATALOG", "hls_amer_catalog")
    schema = os.getenv("SCHEMA", "cost_management")

    query = f"""
        SELECT
            CAST(usage_date AS STRING) AS usage_date,
            workspace_id,
            job_id,
            job_run_id,
            notebook_path,
            sku_name,
            dbus,
            estimated_cost_usd
        FROM {catalog}.{schema}.unmatched_serverless_runs
        WHERE usage_date >= CURRENT_DATE - INTERVAL {days} DAY
        LIMIT {limit}
    """

    try:
        result = dl.execute_query(query)

        return [
            {
                "usage_date": row[0],
                "workspace_id": row[1],
                "job_id": row[2],
                "job_run_id": row[3],
                "notebook_path": row[4],
                "sku_name": row[5],
                "dbus": round(float(row[6] or 0), 2),
                "estimated_cost_usd": round(float(row[7] or 0), 4),
            }
            for row in result.data
        ]

    except Exception as e:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
            return []
        raise HTTPException(status_code=500, detail=str(e))
