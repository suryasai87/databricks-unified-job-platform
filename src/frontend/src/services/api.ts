import axios from 'axios';
import type {
  AuthStatus,
  DataAccessStatus,
  RunSummary,
  JobRun,
  DailyRuns,
  MatrixJob,
  FailedJob,
  ProlongedJob,
  Anomaly,
  SLAStatus,
  GenieSpace,
  PerformanceStats,
  GanttBucketRow,
  CostSummary,
  DailyCost,
  TopJob,
  TopJobRun,
  CostBySku,
} from '../types';

const api = axios.create({
  baseURL: '/api',
  timeout: 60000,
});

// Health
export const getAppHealth = () => api.get<{ data_source: string }>('/health');

// Auth
export const getAuthStatus = () => api.get<AuthStatus>('/auth/status');

// Data Access
export const checkDataAccess = () => api.get<DataAccessStatus>('/data/access-check');
export const getPerformanceStats = () => api.get<PerformanceStats>('/data/performance');
export const getWorkspaceUrls = () => api.get<Record<string, { name: string; url: string }>>('/workspaces');

// Jobs
export const getRunSummary = (days: number = 7) =>
  api.get<RunSummary>('/jobs/summary', { params: { days } });

export const getJobRuns = (
  days: number = 7,
  limit: number = 100,
  status?: string,
  search?: string,
  workspace_id?: string,
  /** Rolling window (hours); when set, backend uses overlap with this window like /jobs/concurrent */
  hours?: number,
) => api.get<JobRun[]>('/jobs/runs', { params: { days, limit, status, search, workspace_id, hours } });

export const cancelJobRun = (run_id: number, workspace_id: string) =>
  api.post('/jobs/cancel', { run_id, workspace_id });

export const getDailyRuns = (days: number = 30) =>
  api.get<DailyRuns[]>('/jobs/daily', { params: { days } });

export const getRunsByType = (days: number = 7) =>
  api.get<{ run_type: string; count: number }[]>('/jobs/by-type', { params: { days } });

export const getJobsMatrix = (limit: number = 50, runsPerJob: number = 10) =>
  api.get<MatrixJob[]>('/jobs/matrix', { params: { limit, runs_per_job: runsPerJob } });

export const getOverlaps = (hours: number = 24) =>
  api.get('/jobs/overlaps', { params: { hours } });

export const getConcurrentJobs = (hours: number = 24) =>
  api.get('/jobs/concurrent', { params: { hours } });

export const getGanttData = (hours: number = 24) =>
  api.get<GanttBucketRow[]>('/jobs/gantt', { params: { hours } });

// Health
export const getFailedJobs = (days: number = 7, minRuns: number = 3, limit: number = 20) =>
  api.get<FailedJob[]>('/health-metrics/failed-jobs', { params: { days, min_runs: minRuns, limit } });

export const getProlongedJobs = (warningMultiplier: number = 1.5, criticalMultiplier: number = 2.0) =>
  api.get<ProlongedJob[]>('/health-metrics/prolonged-jobs', {
    params: { warning_multiplier: warningMultiplier, critical_multiplier: criticalMultiplier },
  });

export const getAnomalies = (days: number = 7, baselineDays: number = 30, zThreshold: number = 2.0) =>
  api.get<Anomaly[]>('/health-metrics/anomalies', {
    params: { days, baseline_days: baselineDays, z_threshold: zThreshold },
  });

export const getRetryStats = (days: number = 7, limit: number = 20) =>
  api.get('/health-metrics/retry-stats', { params: { days, limit } });

export const getSLAStatus = (days: number = 7, slaMultiplier: number = 2.0) =>
  api.get<SLAStatus[]>('/health-metrics/sla-status', { params: { days, sla_multiplier: slaMultiplier } });

export const getDurationPercentiles = (days: number = 30) =>
  api.get('/health-metrics/duration-percentiles', { params: { days } });

// Genie
export const getGenieSpaces = () => api.get<GenieSpace[]>('/genie/spaces');

export const startConversation = (spaceId: string, initialMessage?: string) =>
  api.post('/genie/conversations', { space_id: spaceId, initial_message: initialMessage });

export const sendMessage = (conversationId: string, content: string) =>
  api.post(`/genie/conversations/${conversationId}/messages`, { content });

export const getSuggestedQuestions = () => api.get('/genie/suggested-questions');

// Costs
export const getCostSummary = (days: number = 30, workspace_id?: string) =>
  api.get<CostSummary>('/costs/summary', { params: { days, workspace_id } });

export const getDailyCosts = (days: number = 30, workspace_id?: string) =>
  api.get<DailyCost[]>('/costs/daily', { params: { days, workspace_id } });

export const getTopExpensiveJobs = (days: number = 30, limit: number = 100, workspace_id?: string) =>
  api.get<TopJob[]>('/costs/top-jobs', { params: { days, limit, workspace_id } });

export const getTopExpensiveRuns = (days: number = 30, limit: number = 100, workspace_id?: string) =>
  api.get<TopJobRun[]>('/costs/top-runs', { params: { days, limit, workspace_id } });

export const getCostBySku = (days: number = 30, workspace_id?: string) =>
  api.get<CostBySku[]>('/costs/by-sku', { params: { days, workspace_id } });


export default api;
