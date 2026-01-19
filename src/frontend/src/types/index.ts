// User & Auth types
export interface User {
  email: string;
  name?: string;
  source: 'OBO' | 'U2M' | 'Cookie' | 'M2M' | 'unknown';
  authenticated: boolean;
}

export interface AuthStatus {
  authenticated: boolean;
  user: User | null;
  auth_method: string;
}

// Data Access types
export interface DataAccessStatus {
  accessible: boolean;
  data_source?: string;
  error_code?: string;
  message?: string;
  resolution?: string;
  tables_affected?: string[];
  tables?: TableAccess[];
}

export interface TableAccess {
  table: string;
  description: string;
  accessible: boolean;
  error?: string;
}

// Job types
export interface JobRun {
  job_id: number;
  job_name: string | null;
  run_id: number;
  result_state: string | null;
  run_type: string | null;
  start_time: string | null;
  end_time: string | null;
  execution_duration: number | null;
  creator_user_name: string | null;
}

export interface RunSummary {
  total_runs: number;
  succeeded: number;
  failed: number;
  running: number;
  success_rate: number;
}

export interface DailyRuns {
  date: string;
  total: number;
  succeeded: number;
  failed: number;
}

export interface MatrixJob {
  job_id: number;
  job_name: string;
  runs: MatrixRun[];
}

export interface MatrixRun {
  run_id: number;
  result_state: string | null;
  start_time: string | null;
  duration_seconds: number | null;
}

// Cost types
export interface CostSummary {
  total_cost_usd: number;
  total_dbus: number;
  unique_jobs: number;
  total_runs: number;
  avg_cost_per_run: number;
  avg_daily_cost: number;
}

export interface DailyCost {
  date: string;
  cost: number;
  dbus: number;
  job_runs: number;
}

export interface TopJob {
  job_id: number;
  job_name: string;
  workspace_id: string | null;
  notebook_path: string | null;
  total_cost: number;
  total_dbus: number;
  run_count: number;
  avg_cost_per_run: number;
  job_url: string | null;
}

export interface CostByProject {
  project_code: string;
  department: string;
  total_cost: number;
  total_dbus: number;
  job_runs: number;
}

export interface CostByDepartment {
  department: string;
  total_cost: number;
  total_dbus: number;
  job_runs: number;
  project_count: number;
}

export interface CorrelationRate {
  total_records: number;
  matched: number;
  unmatched: number;
  correlation_rate_pct: number;
  breakdown: { status: string; count: number; cost: number }[];
}

// Health types
export interface FailedJob {
  job_id: number;
  job_name: string | null;
  total_runs: number;
  failed_runs: number;
  success_rate: number;
  last_failure: string | null;
}

export interface ProlongedJob {
  job_id: number;
  job_name: string | null;
  run_id: number;
  start_time: string;
  duration_seconds: number;
  avg_duration_seconds: number;
  status: 'warning' | 'critical';
}

export interface Anomaly {
  job_id: number;
  job_name: string | null;
  metric: string;
  current_value: number;
  avg_value: number;
  std_dev: number;
  z_score: number;
  severity: 'warning' | 'critical';
}

export interface SLAStatus {
  job_id: number;
  job_name: string | null;
  total_runs: number;
  avg_duration_seconds: number;
  sla_violations: number;
  compliance_rate_pct: number;
  status: 'compliant' | 'at_risk' | 'non_compliant';
}

// Tag types
export interface TagCorrelation {
  correlation_id: number;
  job_run_id: number | null;
  notebook_path: string | null;
  adf_pipeline_name: string | null;
  adf_run_id: string | null;
  project_code: string | null;
  department: string | null;
  environment: string | null;
  run_status: string | null;
  run_start_time: string | null;
}

export interface TagPolicy {
  tag_key: string;
  tag_display_name: string | null;
  tag_description: string | null;
  tag_category: string | null;
  is_required: boolean;
  allowed_values: string[] | null;
  default_value: string | null;
  validation_regex: string | null;
}

export interface TagSummary {
  total_records: number;
  unique_projects: number;
  unique_departments: number;
  unique_pipelines: number;
  tagging_completeness: {
    project_code_pct: number;
    department_pct: number;
  };
  run_status: {
    successful: number;
    failed: number;
  };
  setup_required?: boolean;
  message?: string;
}

// Genie types
export interface GenieSpace {
  id: string;
  name: string;
  description: string | null;
}

export interface GenieMessage {
  role: string;
  content: string;
  sql?: string;
  results?: Record<string, unknown>[];
}

// Performance types
export interface PerformanceStats {
  lakebase: {
    execution_time_ms: number;
    status: string;
  } | null;
  warehouse: {
    execution_time_ms: number;
    status: string;
  } | null;
  speedup_factor: number | null;
}
