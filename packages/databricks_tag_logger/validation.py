"""
Tag Validation Utilities
"""
import re
from typing import Any, Dict, List, Optional, Tuple


def validate_tags(
    tags: Dict[str, Any],
    catalog: str = "hls_amer_catalog",
    schema: str = "cost_management",
) -> Tuple[bool, List[str]]:
    """
    Validate tags against policy definitions.

    Args:
        tags: Dictionary of tag key-value pairs to validate.
        catalog: Unity Catalog name.
        schema: Schema name.

    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []

    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()

        # Get policy definitions
        policies_df = spark.sql(f"""
            SELECT tag_key, is_required, allowed_values, validation_regex
            FROM {catalog}.{schema}.tag_policy_definitions
            WHERE is_active = TRUE
        """)

        policies = {row.tag_key: row for row in policies_df.collect()}

        for tag_key, policy in policies.items():
            value = tags.get(tag_key)

            # Check required
            if policy.is_required and not value:
                errors.append(f"Required tag '{tag_key}' is missing")
                continue

            if value:
                # Check allowed values
                if policy.allowed_values and value not in policy.allowed_values:
                    errors.append(
                        f"Tag '{tag_key}' value '{value}' not in allowed values: {policy.allowed_values}"
                    )

                # Check regex
                if policy.validation_regex:
                    if not re.match(policy.validation_regex, value):
                        errors.append(
                            f"Tag '{tag_key}' value '{value}' doesn't match pattern: {policy.validation_regex}"
                        )

        return len(errors) == 0, errors

    except Exception as e:
        return False, [f"Validation error: {str(e)}"]


def get_required_tags(
    catalog: str = "hls_amer_catalog",
    schema: str = "cost_management",
) -> List[str]:
    """Get list of required tag keys from policy definitions."""
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()

        result = spark.sql(f"""
            SELECT tag_key
            FROM {catalog}.{schema}.tag_policy_definitions
            WHERE is_required = TRUE AND is_active = TRUE
        """)

        return [row.tag_key for row in result.collect()]

    except Exception:
        return ["project_code", "cost_center", "department", "environment"]


def get_default_tags(
    catalog: str = "hls_amer_catalog",
    schema: str = "cost_management",
) -> Dict[str, str]:
    """Get default tag values from policy definitions."""
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()

        result = spark.sql(f"""
            SELECT tag_key, default_value
            FROM {catalog}.{schema}.tag_policy_definitions
            WHERE default_value IS NOT NULL AND is_active = TRUE
        """)

        return {row.tag_key: row.default_value for row in result.collect()}

    except Exception:
        return {"environment": "dev"}
