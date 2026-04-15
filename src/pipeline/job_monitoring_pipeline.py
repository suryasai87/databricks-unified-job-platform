# Databricks notebook source
# MAGIC %md
# MAGIC # Job Monitoring Pipeline
# MAGIC Ingests from system tables → streaming tables → SCD1 materialized views.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --- Streaming Tables (raw ingestion) ---

@dlt.table(
    name="synced_job_run_timeline",
    comment="Raw job run timeline from system.lakeflow.job_run_timeline",
)
def synced_job_run_timeline():
    return spark.readStream.table("system.lakeflow.job_run_timeline")


@dlt.table(
    name="synced_jobs",
    comment="Raw jobs changelog from system.lakeflow.jobs",
)
def synced_jobs():
    return spark.readStream.table("system.lakeflow.jobs")


# Use a materialized view instead of a streaming table for workspaces_latest.
# This table receives in-place updates (not just appends), which causes
# streaming reads to fail with:
#   "Detected a data update in the source table ... set 'skipChangeCommits' to 'true'"
@dlt.table(
    name="synced_workspaces",
    comment="Workspace metadata from system.access.workspaces_latest",
)
def synced_workspaces():
    return spark.read.table("system.access.workspaces_latest")

# --- Materialized Views (SCD Type 1) ---

@dlt.table(
    name="job_runs_latest",
    comment="SCD Type 1: one row per job run with original start time and computed duration",
)
def job_runs_latest():
    """Aggregate all period entries per run into a single row."""
    df = dlt.read("synced_job_run_timeline")
    agg = df.groupBy("account_id", "workspace_id", "job_id", "run_id").agg(
        F.min("period_start_time").alias("period_start_time"),
        F.max("period_end_time").alias("period_end_time"),
        F.max("result_state").alias("result_state"),
        F.max("run_name").alias("run_name"),
        F.max("run_type").alias("run_type"),
        F.max("trigger_type").alias("trigger_type"),
        F.max("termination_code").alias("termination_code"),
        F.sum("run_duration_seconds").alias("run_duration_seconds"),
        F.sum("execution_duration_seconds").alias("_raw_exec_dur"),
        F.sum("setup_duration_seconds").alias("setup_duration_seconds"),
        F.sum("queue_duration_seconds").alias("queue_duration_seconds"),
        F.sum("cleanup_duration_seconds").alias("cleanup_duration_seconds"),
    )
    return agg.withColumn(
        "execution_duration_seconds",
        F.coalesce(
            F.when(F.col("_raw_exec_dur") > 0, F.col("_raw_exec_dur")),
            (F.unix_timestamp("period_end_time") - F.unix_timestamp("period_start_time")).cast("bigint"),
        ),
    ).drop("_raw_exec_dur")


@dlt.table(
    name="jobs_latest",
    comment="SCD Type 1: latest state per job",
)
def jobs_latest():
    df = dlt.read("synced_jobs")
    w = Window.partitionBy("workspace_id", "job_id").orderBy(F.col("change_time").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
