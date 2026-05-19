# Databricks notebook source
# MAGIC %md
# MAGIC # Refresh Lakebase Synced Tables
# MAGIC
# MAGIC Triggers a refresh of the three Lakebase synced tables used by the Unified Job Platform.
# MAGIC Tables must be created first — see Step 3 of the README.

# COMMAND ----------

%pip install --upgrade databricks-sdk
dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Unity Catalog name")
dbutils.widgets.text("schema", "cost_management", "Schema name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

SYNCED_TABLES = [
    f"{catalog}.{schema}.lb_job_runs_latest",
    f"{catalog}.{schema}.lb_jobs_latest",
    f"{catalog}.{schema}.lb_synced_workspaces",
    f"{catalog}.{schema}.lb_billing_usage_enriched",
]

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

for table_name in SYNCED_TABLES:
    print(f"Refreshing {table_name}...")
    synced_table = w.database.get_synced_database_table(table_name)
    pipeline_id = synced_table.data_synchronization_status.pipeline_id
    w.pipelines.start_update(pipeline_id=pipeline_id)
    print(f"  Triggered pipeline {pipeline_id}")

print("\nDone.")
