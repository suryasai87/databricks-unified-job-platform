"""
Databricks Tag Logger Package
=============================
A reusable Python package for logging tag correlations between
Azure Data Factory pipelines and Databricks serverless workloads.

Usage:
    from databricks_tag_logger import ServerlessTagLogger, init_tag_logging

    # Option 1: Quick-start with widgets
    logger, correlation_id = init_tag_logging(project_code="PROJ-001")
    # ... your ETL code ...
    logger.update_run_end(correlation_id, "SUCCESS")

    # Option 2: Direct usage
    logger = ServerlessTagLogger(catalog="hls_amer_catalog", schema="cost_management")
    correlation_id = logger.log_tags(
        adf_pipeline_name="my_pipeline",
        project_code="PROJ-001",
        department="Engineering"
    )
"""

from .logger import ServerlessTagLogger, TagRecord, init_tag_logging, track_serverless_tags
from .config import TagLoggerConfig
from .validation import validate_tags, get_required_tags, get_default_tags

__version__ = "1.0.0"
__all__ = [
    "ServerlessTagLogger",
    "TagRecord",
    "TagLoggerConfig",
    "init_tag_logging",
    "track_serverless_tags",
    "validate_tags",
    "get_required_tags",
    "get_default_tags",
]
