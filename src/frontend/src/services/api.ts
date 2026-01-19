import axios from 'axios';
import type {
  AuthStatus,
  DataAccessStatus,
  RunSummary,
  JobRun,
  DailyRuns,
  MatrixJob,
  CostSummary,
  DailyCost,
  TopJob,
  CostByProject,
  CostByDepartment,
  CorrelationRate,
  FailedJob,
  ProlongedJob,
  Anomaly,
  SLAStatus,
  TagCorrelation,
  TagPolicy,
  TagSummary,
  GenieSpace,
  PerformanceStats,
} from '../types';

const api = axios.create({
  baseURL: '/api',
  timeout: 60000,
});

// Auth
export const getAuthStatus = () => api.get<AuthStatus>('/auth/status');

// Data Access
export const checkDataAccess = () => api.get<DataAccessStatus>('/data/access-check');
export const getPerformanceStats = () => api.get<PerformanceStats>('/data/performance');

// Jobs
export const getRunSummary = (days: number = 7) =>
  api.get<RunSummary>('/jobs/summary', { params: { days } });

export const getJobRuns = (days: number = 7, limit: number = 100, status?: string) =>
  api.get<JobRun[]>('/jobs/runs', { params: { days, limit, status } });

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

// Costs
export const getCostSummary = (days: number = 30) =>
  api.get<CostSummary>('/costs/summary', { params: { days } });

export const getDailyCosts = (days: number = 30) =>
  api.get<DailyCost[]>('/costs/daily', { params: { days } });

export const getTopExpensiveJobs = (days: number = 30, limit: number = 10) =>
  api.get<TopJob[]>('/costs/top-jobs', { params: { days, limit } });

export const getCostByIdentity = (days: number = 30, limit: number = 20) =>
  api.get('/costs/by-identity', { params: { days, limit } });

export const getCostByProject = (days: number = 30) =>
  api.get<CostByProject[]>('/costs/by-project', { params: { days } });

export const getCostByDepartment = (days: number = 30) =>
  api.get<CostByDepartment[]>('/costs/by-department', { params: { days } });

export const getCostTrends = (weeks: number = 8) =>
  api.get('/costs/trends', { params: { weeks } });

export const getCorrelationRate = (days: number = 7) =>
  api.get<CorrelationRate>('/costs/correlation-rate', { params: { days } });

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

// Tags
export const getTagCorrelations = (days: number = 7, limit: number = 100, projectCode?: string, department?: string) =>
  api.get<TagCorrelation[]>('/tags/correlations', { params: { days, limit, project_code: projectCode, department } });

export const getTagPolicies = (activeOnly: boolean = true) =>
  api.get<TagPolicy[]>('/tags/policies', { params: { active_only: activeOnly } });

export const getTagSummary = (days: number = 30) =>
  api.get<TagSummary>('/tags/summary', { params: { days } });

export const getTagsByPipeline = (days: number = 30, limit: number = 20) =>
  api.get('/tags/by-pipeline', { params: { days, limit } });

export const getUnmatchedRuns = (days: number = 7, limit: number = 50) =>
  api.get('/tags/unmatched', { params: { days, limit } });

// Genie
export const getGenieSpaces = () => api.get<GenieSpace[]>('/genie/spaces');

export const startConversation = (spaceId: string, initialMessage?: string) =>
  api.post('/genie/conversations', { space_id: spaceId, initial_message: initialMessage });

export const sendMessage = (conversationId: string, content: string) =>
  api.post(`/genie/conversations/${conversationId}/messages`, { content });

export const getSuggestedQuestions = () => api.get('/genie/suggested-questions');

export default api;
