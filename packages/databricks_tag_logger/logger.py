"""
Core Tag Logger Implementation
"""
import json
from dataclasses import dataclass, field, asdict
from datetime import datetime
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Tuple

from .config import TagLoggerConfig, DEFAULT_WIDGETS


@dataclass
class TagRecord:
    """Data structure for a tag correlation record."""

    # Databricks Context
    job_id: Optional[int] = None
    job_run_id: Optional[int] = None
    task_run_id: Optional[int] = None
    notebook_id: Optional[int] = None
    notebook_path: Optional[str] = None
    workspace_id: Optional[int] = None
    cluster_id: Optional[str] = None

    # ADF Context
    adf_pipeline_name: Optional[str] = None
    adf_pipeline_id: Optional[str] = None
    adf_run_id: Optional[str] = None
    adf_activity_name: Optional[str] = None
    adf_trigger_name: Optional[str] = None
    adf_trigger_time: Optional[datetime] = None
    adf_data_factory_name: Optional[str] = None

    # Cost Attribution Tags
    project_code: Optional[str] = None
    cost_center: Optional[str] = None
    department: Optional[str] = None
    business_unit: Optional[str] = None
    environment: Optional[str] = None
    application_name: Optional[str] = None
    owner_email: Optional[str] = None

    # Custom Tags
    custom_tags: Dict[str, str] = field(default_factory=dict)

    # Execution Metadata
    run_start_time: Optional[datetime] = None
    run_end_time: Optional[datetime] = None
    run_status: str = "RUNNING"
    created_by: Optional[str] = None


class ServerlessTagLogger:
    """
    Logger for correlating ADF pipeline tags with Databricks serverless jobs.

    This class provides methods to:
    - Log tags at the start of a job/notebook
    - Update run status at completion
    - Create and read from notebook widgets
    - Validate tags against policies
    """

    def __init__(
        self,
        catalog: str = "hls_amer_catalog",
        schema: str = "cost_management",
        table: str = "serverless_tag_correlation",
        fail_silently: bool = True,
    ):
        self.config = TagLoggerConfig(
            catalog=catalog,
            schema=schema,
            table=table,
            fail_silently=fail_silently,
        )
        self._spark = None
        self._dbutils = None

    @property
    def spark(self):
        """Lazy-load SparkSession."""
        if self._spark is None:
            try:
                from pyspark.sql import SparkSession
                self._spark = SparkSession.builder.getOrCreate()
            except Exception as e:
                if not self.config.fail_silently:
                    raise
                print(f"Warning: Could not get SparkSession: {e}")
        return self._spark

    @property
    def dbutils(self):
        """Lazy-load dbutils."""
        if self._dbutils is None:
            try:
                from pyspark.dbutils import DBUtils
                self._dbutils = DBUtils(self.spark)
            except Exception:
                try:
                    # Fallback for notebook environment
                    import IPython
                    self._dbutils = IPython.get_ipython().user_ns.get("dbutils")
                except Exception as e:
                    if not self.config.fail_silently:
                        raise
                    print(f"Warning: Could not get dbutils: {e}")
        return self._dbutils

    def _get_job_context(self) -> Dict[str, Any]:
        """Extract Databricks job context from the current environment."""
        context = {}
        try:
            if self.dbutils:
                # Get notebook context
                notebook_info = json.loads(
                    self.dbutils.notebook.entry_point.getDbutils()
                    .notebook().getContext().toJson()
                )

                context["notebook_path"] = notebook_info.get("extraContext", {}).get("notebook_path")
                context["notebook_id"] = notebook_info.get("extraContext", {}).get("notebook_id")
                context["cluster_id"] = notebook_info.get("clusterId")
                context["workspace_id"] = notebook_info.get("tags", {}).get("orgId")

                # Job run context (if running as a job)
                job_info = notebook_info.get("tags", {})
                if job_info.get("jobId"):
                    context["job_id"] = int(job_info.get("jobId"))
                if job_info.get("runId"):
                    context["job_run_id"] = int(job_info.get("runId"))
                if job_info.get("taskRunId"):
                    context["task_run_id"] = int(job_info.get("taskRunId"))

                # User info
                context["created_by"] = notebook_info.get("tags", {}).get("user")
        except Exception as e:
            if not self.config.fail_silently:
                raise
            print(f"Warning: Could not get job context: {e}")

        return context

    def create_widgets(self) -> None:
        """Create standard widgets for ADF parameters."""
        if not self.dbutils:
            return

        for name, default, label in DEFAULT_WIDGETS:
            try:
                self.dbutils.widgets.text(name, default, label)
            except Exception:
                pass  # Widget may already exist

    def from_widgets(self) -> Dict[str, str]:
        """Extract tag values from notebook widgets."""
        values = {}
        if not self.dbutils:
            return values

        for name, _, _ in DEFAULT_WIDGETS:
            try:
                value = self.dbutils.widgets.get(name)
                if value:
                    values[name] = value
            except Exception:
                pass

        return values

    def log_tags(
        self,
        adf_pipeline_name: Optional[str] = None,
        adf_run_id: Optional[str] = None,
        project_code: Optional[str] = None,
        department: Optional[str] = None,
        **kwargs,
    ) -> Optional[str]:
        """
        Log tags to the correlation table.

        Returns the job_run_id as a correlation key, or None if logging failed.
        """
        try:
            # Get job context
            context = self._get_job_context()

            # Build record
            record = TagRecord(
                # Databricks context
                job_id=context.get("job_id"),
                job_run_id=context.get("job_run_id"),
                task_run_id=context.get("task_run_id"),
                notebook_id=context.get("notebook_id"),
                notebook_path=context.get("notebook_path"),
                workspace_id=context.get("workspace_id"),
                cluster_id=context.get("cluster_id"),
                created_by=context.get("created_by"),
                # ADF context
                adf_pipeline_name=adf_pipeline_name or kwargs.get("adf_pipeline_name"),
                adf_pipeline_id=kwargs.get("adf_pipeline_id"),
                adf_run_id=adf_run_id or kwargs.get("adf_run_id"),
                adf_activity_name=kwargs.get("adf_activity_name"),
                adf_trigger_name=kwargs.get("adf_trigger_name"),
                adf_data_factory_name=kwargs.get("adf_data_factory_name"),
                # Cost attribution
                project_code=project_code or kwargs.get("project_code"),
                cost_center=kwargs.get("cost_center"),
                department=department or kwargs.get("department"),
                business_unit=kwargs.get("business_unit"),
                environment=kwargs.get("environment", "dev"),
                application_name=kwargs.get("application_name"),
                owner_email=kwargs.get("owner_email"),
                # Execution metadata
                run_start_time=datetime.now(),
                run_status="RUNNING",
            )

            # Extract custom tags
            known_keys = set(asdict(record).keys())
            custom_tags = {k: v for k, v in kwargs.items() if k not in known_keys and v}
            if custom_tags:
                record.custom_tags = custom_tags

            # Insert into Delta table
            self._insert_record(record)

            return str(record.job_run_id) if record.job_run_id else None

        except Exception as e:
            if not self.config.fail_silently:
                raise
            print(f"Warning: Failed to log tags: {e}")
            return None

    def _insert_record(self, record: TagRecord) -> None:
        """Insert a tag record into the Delta table."""
        if not self.spark:
            raise RuntimeError("SparkSession not available")

        # Convert to dict and handle special types
        data = asdict(record)

        # Convert datetime to string
        for key in ["run_start_time", "run_end_time", "adf_trigger_time"]:
            if data.get(key):
                data[key] = data[key].isoformat()

        # Convert custom_tags to JSON string for map type
        if data.get("custom_tags"):
            data["custom_tags"] = json.dumps(data["custom_tags"])
        else:
            data["custom_tags"] = None

        # Create DataFrame and insert
        df = self.spark.createDataFrame([data])
        df.write.format("delta").mode("append").saveAsTable(self.config.table_path)

    def update_run_end(self, job_run_id: str, status: str = "SUCCESS") -> bool:
        """
        Update the run end time and status for a completed job.

        Args:
            job_run_id: The job run ID returned from log_tags()
            status: Run status (SUCCESS, FAILED, CANCELLED)

        Returns:
            True if update succeeded, False otherwise.
        """
        try:
            if not self.spark:
                return False

            update_sql = f"""
                UPDATE {self.config.table_path}
                SET
                    run_end_time = current_timestamp(),
                    run_status = '{status}',
                    updated_at = current_timestamp()
                WHERE job_run_id = {job_run_id}
                  AND run_end_time IS NULL
            """
            self.spark.sql(update_sql)
            return True

        except Exception as e:
            if not self.config.fail_silently:
                raise
            print(f"Warning: Failed to update run end: {e}")
            return False

    def log_from_widgets(self, **extra_tags) -> Optional[str]:
        """
        Convenience method to create widgets, read values, and log tags.

        Args:
            **extra_tags: Additional tags to include beyond widget values.

        Returns:
            The job_run_id as a correlation key.
        """
        self.create_widgets()
        widget_values = self.from_widgets()
        widget_values.update(extra_tags)
        return self.log_tags(**widget_values)


def track_serverless_tags(
    catalog: str = "hls_amer_catalog",
    schema: str = "cost_management",
    **default_tags,
) -> Callable:
    """
    Decorator for automatic tag logging around functions.

    Usage:
        @track_serverless_tags(project_code="PROJ-001", department="Engineering")
        def my_etl_function():
            # Your ETL code here
            pass
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger = ServerlessTagLogger(catalog=catalog, schema=schema)

            # Merge default tags with any runtime tags
            all_tags = {**default_tags, **kwargs.pop("_tags", {})}

            # Log start
            correlation_id = logger.log_tags(**all_tags)

            try:
                result = func(*args, **kwargs)
                logger.update_run_end(correlation_id, "SUCCESS")
                return result
            except Exception as e:
                logger.update_run_end(correlation_id, "FAILED")
                raise

        return wrapper
    return decorator


def init_tag_logging(
    catalog: str = "hls_amer_catalog",
    schema: str = "cost_management",
    **default_tags,
) -> Tuple["ServerlessTagLogger", Optional[str]]:
    """
    Quick-start function for notebook tag logging.

    Usage:
        logger, correlation_id = init_tag_logging(project_code="PROJ-001")
        # ... your ETL code ...
        logger.update_run_end(correlation_id, "SUCCESS")

    Returns:
        Tuple of (logger instance, correlation_id)
    """
    logger = ServerlessTagLogger(catalog=catalog, schema=schema)
    correlation_id = logger.log_from_widgets(**default_tags)
    return logger, correlation_id
