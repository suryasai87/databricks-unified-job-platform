-- ============================================================
-- Unified Job Platform - Infrastructure Setup
-- Creates all tables and views for job monitoring + cost attribution
-- ============================================================

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema}
COMMENT 'Unified Job Platform: Job Monitoring + Cost Attribution';

-- ============================================================
-- TABLE 1: serverless_tag_correlation
-- Core table for ADF-Databricks tag correlation
-- ============================================================
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.serverless_tag_correlation (
  -- Identity
  correlation_id BIGINT GENERATED ALWAYS AS IDENTITY,

  -- Databricks Job Context
  job_id BIGINT,
  job_run_id BIGINT,
  task_run_id BIGINT,
  notebook_id BIGINT,
  notebook_path STRING,
  workspace_id BIGINT,
  cluster_id STRING,

  -- Azure Data Factory Context
  adf_pipeline_name STRING,
  adf_pipeline_id STRING,
  adf_run_id STRING,
  adf_activity_name STRING,
  adf_trigger_name STRING,
  adf_trigger_time TIMESTAMP,
  adf_data_factory_name STRING,

  -- Cost Attribution Tags
  project_code STRING COMMENT 'Project identifier (e.g., PROJ-001)',
  cost_center STRING COMMENT 'Cost center code (e.g., CC12345)',
  department STRING COMMENT 'Department name',
  business_unit STRING COMMENT 'Business unit',
  environment STRING COMMENT 'Environment: dev, test, staging, prod',
  application_name STRING COMMENT 'Application or service name',
  owner_email STRING COMMENT 'Owner email address',

  -- Custom Tags (extensible)
  custom_tags MAP<STRING, STRING> COMMENT 'Additional custom tags',

  -- Execution Metadata
  run_start_time TIMESTAMP,
  run_end_time TIMESTAMP,
  run_status STRING COMMENT 'SUCCESS, FAILED, RUNNING, CANCELLED',

  -- Audit Fields
  created_by STRING,
  created_at TIMESTAMP DEFAULT current_timestamp(),
  updated_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (DATE(run_start_time))
CLUSTER BY (project_code, department, workspace_id)
COMMENT 'Tag correlation between ADF pipelines and Databricks serverless jobs'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.deletedFileRetentionDuration' = 'interval 30 days',
  'delta.logRetentionDuration' = 'interval 60 days'
);

-- ============================================================
-- TABLE 2: tag_policy_definitions
-- Master configuration for allowed tags and validation rules
-- ============================================================
CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.tag_policy_definitions (
  tag_key STRING NOT NULL,
  tag_display_name STRING,
  tag_description STRING,
  tag_category STRING COMMENT 'cost, compliance, or operations',
  is_required BOOLEAN DEFAULT FALSE,
  allowed_values ARRAY<STRING> COMMENT 'NULL means any value allowed',
  default_value STRING,
  validation_regex STRING,
  is_active BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMP DEFAULT current_timestamp(),
  updated_at TIMESTAMP,
  created_by STRING,

  CONSTRAINT pk_tag_policy PRIMARY KEY (tag_key)
)
USING DELTA
COMMENT 'Tag policy definitions for validation and governance';

-- Insert default tag policies
MERGE INTO ${catalog}.${schema}.tag_policy_definitions AS target
USING (
  SELECT 'project_code' AS tag_key, 'Project Code' AS tag_display_name,
         'Project identifier for cost attribution' AS tag_description,
         'cost' AS tag_category, TRUE AS is_required, NULL AS allowed_values,
         NULL AS default_value, '^[A-Z]{2,5}-[0-9]{3,6}$' AS validation_regex
  UNION ALL
  SELECT 'cost_center', 'Cost Center', 'Financial cost center code',
         'cost', TRUE, NULL, NULL, '^CC[0-9]{4,8}$'
  UNION ALL
  SELECT 'department', 'Department', 'Organizational department',
         'cost', TRUE,
         ARRAY('Engineering', 'Data Science', 'Analytics', 'Finance', 'Operations', 'Marketing', 'Healthcare', 'Research'),
         NULL, NULL
  UNION ALL
  SELECT 'environment', 'Environment', 'Deployment environment',
         'operations', TRUE, ARRAY('dev', 'test', 'staging', 'prod'), 'dev', NULL
  UNION ALL
  SELECT 'business_unit', 'Business Unit', 'Business unit name',
         'cost', FALSE, NULL, NULL, NULL
  UNION ALL
  SELECT 'application_name', 'Application Name', 'Application or service name',
         'operations', FALSE, NULL, NULL, NULL
  UNION ALL
  SELECT 'owner_email', 'Owner Email', 'Responsible team or person email',
         'operations', FALSE, NULL, NULL, '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'
) AS source
ON target.tag_key = source.tag_key
WHEN NOT MATCHED THEN
  INSERT (tag_key, tag_display_name, tag_description, tag_category,
          is_required, allowed_values, default_value, validation_regex, is_active)
  VALUES (source.tag_key, source.tag_display_name, source.tag_description,
          source.tag_category, source.is_required, source.allowed_values,
          source.default_value, source.validation_regex, TRUE);

-- ============================================================
-- VIEW 1: serverless_cost_by_tags
-- Joins billing usage with tag correlation for cost attribution
-- ============================================================
CREATE OR REPLACE VIEW ${catalog}.${schema}.serverless_cost_by_tags AS
WITH billing_usage AS (
  SELECT
    usage_date,
    workspace_id,
    usage_metadata.job_id AS job_id,
    usage_metadata.job_run_id AS job_run_id,
    usage_metadata.notebook_path AS notebook_path,
    sku_name,
    usage_quantity AS dbus,
    usage_quantity * COALESCE(
      (SELECT default_pricing FROM system.billing.list_prices lp
       WHERE lp.sku_name = u.sku_name
       AND lp.price_start_time <= u.usage_date
       AND (lp.price_end_time IS NULL OR lp.price_end_time > u.usage_date)
       LIMIT 1), 0.15
    ) AS estimated_cost_usd,
    usage_start_time,
    usage_end_time
  FROM system.billing.usage u
  WHERE product_features.is_serverless = TRUE
    AND billing_origin_product IN ('JOBS', 'INTERACTIVE', 'WORKFLOWS')
    AND usage_date >= CURRENT_DATE - INTERVAL 90 DAYS
)
SELECT
  b.usage_date,
  b.workspace_id,
  b.job_id,
  b.job_run_id,
  b.notebook_path,
  b.sku_name,
  b.dbus,
  b.estimated_cost_usd,
  b.usage_start_time,
  b.usage_end_time,
  -- Tag correlation fields
  t.correlation_id,
  t.adf_pipeline_name,
  t.adf_run_id,
  t.project_code,
  t.cost_center,
  t.department,
  t.business_unit,
  t.environment,
  t.application_name,
  t.owner_email,
  t.run_status,
  t.created_by,
  -- Correlation status
  CASE
    WHEN t.correlation_id IS NOT NULL THEN 'MATCHED'
    ELSE 'UNMATCHED'
  END AS correlation_status
FROM billing_usage b
LEFT JOIN ${catalog}.${schema}.serverless_tag_correlation t
  ON (b.job_run_id = t.job_run_id)
  OR (b.notebook_path = t.notebook_path
      AND b.usage_start_time BETWEEN t.run_start_time AND COALESCE(t.run_end_time, CURRENT_TIMESTAMP));

-- ============================================================
-- VIEW 2: serverless_cost_summary
-- Aggregated cost summary by tag dimensions
-- ============================================================
CREATE OR REPLACE VIEW ${catalog}.${schema}.serverless_cost_summary AS
SELECT
  usage_date,
  project_code,
  cost_center,
  department,
  environment,
  application_name,
  adf_pipeline_name,
  COUNT(DISTINCT job_run_id) AS job_runs,
  COUNT(DISTINCT notebook_path) AS unique_notebooks,
  SUM(dbus) AS total_dbus,
  SUM(estimated_cost_usd) AS total_cost_usd,
  AVG(dbus) AS avg_dbus_per_run,
  MAX(estimated_cost_usd) AS max_run_cost,
  SUM(CASE WHEN correlation_status = 'MATCHED' THEN 1 ELSE 0 END) AS matched_runs,
  SUM(CASE WHEN correlation_status = 'UNMATCHED' THEN 1 ELSE 0 END) AS unmatched_runs,
  ROUND(
    SUM(CASE WHEN correlation_status = 'MATCHED' THEN 1 ELSE 0 END) * 100.0 /
    NULLIF(COUNT(*), 0), 2
  ) AS correlation_rate_pct
FROM ${catalog}.${schema}.serverless_cost_by_tags
GROUP BY usage_date, project_code, cost_center, department, environment,
         application_name, adf_pipeline_name;

-- ============================================================
-- VIEW 3: serverless_cost_trends
-- Weekly cost trends with week-over-week analysis
-- ============================================================
CREATE OR REPLACE VIEW ${catalog}.${schema}.serverless_cost_trends AS
WITH weekly_costs AS (
  SELECT
    DATE_TRUNC('week', usage_date) AS week_start,
    project_code,
    department,
    SUM(dbus) AS weekly_dbus,
    SUM(estimated_cost_usd) AS weekly_cost_usd,
    COUNT(DISTINCT job_run_id) AS weekly_job_runs
  FROM ${catalog}.${schema}.serverless_cost_by_tags
  GROUP BY DATE_TRUNC('week', usage_date), project_code, department
)
SELECT
  week_start,
  project_code,
  department,
  weekly_dbus,
  weekly_cost_usd,
  weekly_job_runs,
  LAG(weekly_cost_usd) OVER (
    PARTITION BY project_code, department
    ORDER BY week_start
  ) AS prev_week_cost,
  ROUND(
    (weekly_cost_usd - LAG(weekly_cost_usd) OVER (
      PARTITION BY project_code, department ORDER BY week_start
    )) * 100.0 /
    NULLIF(LAG(weekly_cost_usd) OVER (
      PARTITION BY project_code, department ORDER BY week_start
    ), 0), 2
  ) AS wow_change_pct
FROM weekly_costs;

-- ============================================================
-- VIEW 4: unmatched_serverless_runs
-- Runs without tag correlation for troubleshooting
-- ============================================================
CREATE OR REPLACE VIEW ${catalog}.${schema}.unmatched_serverless_runs AS
SELECT
  usage_date,
  workspace_id,
  job_id,
  job_run_id,
  notebook_path,
  sku_name,
  dbus,
  estimated_cost_usd,
  usage_start_time,
  usage_end_time
FROM ${catalog}.${schema}.serverless_cost_by_tags
WHERE correlation_status = 'UNMATCHED'
ORDER BY estimated_cost_usd DESC;

-- ============================================================
-- VIEW 5: job_health_metrics
-- Health metrics for job monitoring
-- ============================================================
CREATE OR REPLACE VIEW ${catalog}.${schema}.job_health_metrics AS
WITH run_stats AS (
  SELECT
    job_id,
    job_name,
    COUNT(*) AS total_runs,
    SUM(CASE WHEN result_state = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count,
    SUM(CASE WHEN result_state = 'FAILED' THEN 1 ELSE 0 END) AS failed_count,
    AVG(execution_duration / 1000.0) AS avg_duration_seconds,
    STDDEV(execution_duration / 1000.0) AS stddev_duration_seconds,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY execution_duration / 1000.0) AS p50_duration,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY execution_duration / 1000.0) AS p90_duration,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY execution_duration / 1000.0) AS p95_duration,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY execution_duration / 1000.0) AS p99_duration
  FROM system.lakeflow.job_run_timeline
  WHERE period_start_time >= CURRENT_DATE - INTERVAL 30 DAYS
  GROUP BY job_id, job_name
)
SELECT
  job_id,
  job_name,
  total_runs,
  success_count,
  failed_count,
  ROUND(success_count * 100.0 / NULLIF(total_runs, 0), 2) AS success_rate_pct,
  ROUND(avg_duration_seconds, 2) AS avg_duration_seconds,
  ROUND(stddev_duration_seconds, 2) AS stddev_duration_seconds,
  ROUND(p50_duration, 2) AS p50_duration,
  ROUND(p90_duration, 2) AS p90_duration,
  ROUND(p95_duration, 2) AS p95_duration,
  ROUND(p99_duration, 2) AS p99_duration,
  CASE
    WHEN success_count * 1.0 / NULLIF(total_runs, 0) >= 0.95 THEN 'HEALTHY'
    WHEN success_count * 1.0 / NULLIF(total_runs, 0) >= 0.80 THEN 'WARNING'
    ELSE 'CRITICAL'
  END AS health_status
FROM run_stats;

-- ============================================================
-- Grant permissions (customize for your environment)
-- ============================================================
-- GRANT SELECT ON ${catalog}.${schema}.serverless_tag_correlation TO `data-engineers`;
-- GRANT SELECT ON ${catalog}.${schema}.serverless_cost_by_tags TO `data-engineers`;
-- GRANT SELECT ON ${catalog}.${schema}.serverless_cost_summary TO `data-engineers`;
