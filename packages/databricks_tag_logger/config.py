"""
Configuration for Databricks Tag Logger
"""
import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class TagLoggerConfig:
    """Configuration for the ServerlessTagLogger."""

    catalog: str = "hls_amer_catalog"
    schema: str = "cost_management"
    table: str = "serverless_tag_correlation"
    policy_table: str = "tag_policy_definitions"
    fail_silently: bool = True

    @classmethod
    def from_env(cls) -> "TagLoggerConfig":
        """Create configuration from environment variables."""
        return cls(
            catalog=os.getenv("TAG_CATALOG", "hls_amer_catalog"),
            schema=os.getenv("TAG_SCHEMA", "cost_management"),
            table=os.getenv("TAG_TABLE", "serverless_tag_correlation"),
            policy_table=os.getenv("TAG_POLICY_TABLE", "tag_policy_definitions"),
            fail_silently=os.getenv("TAG_FAIL_SILENTLY", "true").lower() == "true",
        )

    @property
    def table_path(self) -> str:
        """Full path to the correlation table."""
        return f"{self.catalog}.{self.schema}.{self.table}"

    @property
    def policy_table_path(self) -> str:
        """Full path to the policy definitions table."""
        return f"{self.catalog}.{self.schema}.{self.policy_table}"


# Default widgets for ADF integration
DEFAULT_WIDGETS = [
    # ADF Context
    ("adf_pipeline_name", "", "ADF Pipeline Name"),
    ("adf_pipeline_id", "", "ADF Pipeline ID"),
    ("adf_run_id", "", "ADF Run ID"),
    ("adf_activity_name", "", "ADF Activity Name"),
    ("adf_trigger_name", "", "ADF Trigger Name"),
    ("adf_trigger_time", "", "ADF Trigger Time"),
    ("adf_data_factory_name", "", "ADF Data Factory Name"),
    # Cost Attribution
    ("project_code", "", "Project Code"),
    ("cost_center", "", "Cost Center"),
    ("department", "", "Department"),
    ("business_unit", "", "Business Unit"),
    ("environment", "dev", "Environment"),
    ("application_name", "", "Application Name"),
    ("owner_email", "", "Owner Email"),
]
