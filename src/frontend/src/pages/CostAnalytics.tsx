import React, { useState, useEffect } from 'react';
import {
  Box,
  Grid,
  Card,
  CardContent,
  CardHeader,
  Typography,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Alert,
  Skeleton,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TablePagination,
  Paper,
  Autocomplete,
  TextField,
  Link,
} from '@mui/material';
import { OpenInNew } from '@mui/icons-material';
import {
  AttachMoney,
  Speed,
  WorkHistory,
  TrendingUp,
} from '@mui/icons-material';
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
  Legend,
} from 'recharts';

import { MetricCard } from '../components';
import {
  getCostSummary,
  getDailyCosts,
  getTopExpensiveJobs,
  getTopExpensiveRuns,
  getCostBySku,
  getWorkspaceUrls,
} from '../services/api';
import type { CostSummary, DailyCost, TopJob, TopJobRun, CostBySku } from '../types';

const PIE_COLORS = ['#1976d2', '#388e3c', '#f57c00', '#d32f2f', '#7b1fa2', '#0097a7', '#455a64', '#c2185b'];

const CostAnalyticsPage: React.FC = () => {
  const [days, setDays] = useState(30);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [summary, setSummary] = useState<CostSummary | null>(null);
  const [dailyCosts, setDailyCosts] = useState<DailyCost[]>([]);
  const [topJobs, setTopJobs] = useState<TopJob[]>([]);
  const [topRuns, setTopRuns] = useState<TopJobRun[]>([]);
  const [skuData, setSkuData] = useState<CostBySku[]>([]);
  const [workspaceUrls, setWorkspaceUrls] = useState<Record<string, { name: string; url: string }>>({});
  const [workspaceFilter, setWorkspaceFilter] = useState<string>('');
  const [jobsPage, setJobsPage] = useState(0);
  const [runsPage, setRunsPage] = useState(0);
  const rowsPerPage = 10;

  useEffect(() => {
    getWorkspaceUrls().then((res) => {
      setWorkspaceUrls(res.data);
    }).catch(() => {});
  }, []);

  useEffect(() => {
    setLoading(true);
    setError(null);
    const wsParam = workspaceFilter || undefined;
    Promise.all([
      getCostSummary(days, wsParam),
      getDailyCosts(days, wsParam),
      getTopExpensiveJobs(days, 100, wsParam),
      getTopExpensiveRuns(days, 100, wsParam),
      getCostBySku(days, wsParam),
    ])
      .then(([summaryRes, dailyRes, topRes, topRunsRes, skuRes]) => {
        setSummary(summaryRes.data);
        setDailyCosts(dailyRes.data);
        setTopJobs(topRes.data);
        setTopRuns(topRunsRes.data);
        setSkuData(skuRes.data);
        setJobsPage(0);
        setRunsPage(0);
      })
      .catch((err) => {
        setError(err?.response?.data?.detail || 'Failed to load cost data');
      })
      .finally(() => setLoading(false));
  }, [days, workspaceFilter]);

  const pieData = skuData
    .map((s) => ({ name: s.sku_name, value: Math.round(s.total_cost * 100) / 100 }))
    .filter((d) => d.value > 0)
    .sort((a, b) => b.value - a.value);

  const fmt = (n: number) =>
    n >= 1000 ? `$${(n / 1000).toFixed(1)}k` : `$${n.toFixed(2)}`;

  return (
    <Box>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 3 }}>
        <Typography variant="h5" fontWeight={700}>
          Cost Analytics
        </Typography>
        <Box sx={{ display: 'flex', gap: 2 }}>
          <FormControl size="small" sx={{ minWidth: 140 }}>
            <InputLabel>Time Range</InputLabel>
            <Select value={days} label="Time Range" onChange={(e) => setDays(Number(e.target.value))}>
              <MenuItem value={7}>Last 7 days</MenuItem>
              <MenuItem value={14}>Last 14 days</MenuItem>
              <MenuItem value={30}>Last 30 days</MenuItem>
              <MenuItem value={60}>Last 60 days</MenuItem>
              <MenuItem value={90}>Last 90 days</MenuItem>
            </Select>
          </FormControl>
          <Autocomplete
            size="small"
            sx={{ minWidth: 250 }}
            options={Object.entries(workspaceUrls)
              .map(([id, ws]) => ({
                id,
                label: ws.name || ws.url.replace('https://', '').replace('.cloud.databricks.com', ''),
              }))
              .sort((a, b) => a.label.localeCompare(b.label))}
            getOptionLabel={(option) => option.label}
            value={
              workspaceFilter
                ? {
                    id: workspaceFilter,
                    label: workspaceUrls[workspaceFilter]?.name || workspaceUrls[workspaceFilter]?.url?.replace('https://', '').replace('.cloud.databricks.com', '') || '',
                  }
                : null
            }
            onChange={(_, val) => setWorkspaceFilter(val?.id || '')}
            isOptionEqualToValue={(option, value) => option.id === value.id}
            renderInput={(params) => <TextField {...params} label="Workspace" />}
          />
        </Box>
      </Box>

      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      {/* KPI Cards */}
      <Grid container spacing={2} sx={{ mb: 3 }}>
        <Grid item xs={12} sm={6} md={3}>
          {loading ? (
            <Skeleton variant="rounded" height={120} />
          ) : (
            <MetricCard
              title="Total Cost"
              value={fmt(summary?.total_cost_usd ?? 0)}
              icon={<AttachMoney />}
              color="primary"
            />
          )}
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          {loading ? (
            <Skeleton variant="rounded" height={120} />
          ) : (
            <MetricCard
              title="Avg Daily Cost"
              value={fmt(summary?.avg_daily_cost ?? 0)}
              icon={<TrendingUp />}
              color="success"
            />
          )}
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          {loading ? (
            <Skeleton variant="rounded" height={120} />
          ) : (
            <MetricCard
              title="Total DBUs"
              value={(summary?.total_dbus ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}
              icon={<Speed />}
              color="warning"
            />
          )}
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          {loading ? (
            <Skeleton variant="rounded" height={120} />
          ) : (
            <MetricCard
              title="Avg Cost / Run"
              value={fmt(summary?.avg_cost_per_run ?? 0)}
              icon={<WorkHistory />}
              color="secondary"
            />
          )}
        </Grid>
      </Grid>

      {/* Charts Row */}
      <Grid container spacing={2} sx={{ mb: 3 }}>
        {/* Daily Cost Trend */}
        <Grid item xs={12} md={8}>
          <Card>
            <CardHeader title="Daily Cost Trend" />
            <CardContent sx={{ height: 320 }}>
              {loading ? (
                <Skeleton variant="rounded" height={280} />
              ) : (
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={dailyCosts}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="date" tick={{ fontSize: 12 }} />
                    <YAxis tickFormatter={(v: number) => `$${v}`} />
                    <Tooltip formatter={(v: number) => [`$${v.toFixed(2)}`, 'Cost']} />
                    <Area type="monotone" dataKey="cost" stroke="#1976d2" fill="#1976d2" fillOpacity={0.2} />
                  </AreaChart>
                </ResponsiveContainer>
              )}
            </CardContent>
          </Card>
        </Grid>

        {/* Cost by SKU Category */}
        <Grid item xs={12} md={4}>
          <Card>
            <CardHeader title="Cost by SKU" />
            <CardContent sx={{ height: 320 }}>
              {loading ? (
                <Skeleton variant="rounded" height={280} />
              ) : pieData.length > 0 ? (
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie
                      data={pieData}
                      cx="50%"
                      cy="45%"
                      outerRadius={90}
                      dataKey="value"
                      label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
                    >
                      {pieData.map((_, i) => (
                        <Cell key={i} fill={PIE_COLORS[i % PIE_COLORS.length]} />
                      ))}
                    </Pie>
                    <Tooltip formatter={(v: number) => `$${v.toFixed(2)}`} />
                    <Legend />
                  </PieChart>
                </ResponsiveContainer>
              ) : (
                <Typography color="text.secondary" align="center" sx={{ pt: 10 }}>
                  No SKU data available
                </Typography>
              )}
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Tables Row */}
      <Grid container spacing={2}>
        {/* Top Expensive Jobs */}
        <Grid item xs={12} md={6}>
          <Card>
            <CardHeader title="Top Expensive Jobs" />
            <CardContent>
              {loading ? (
                <Skeleton variant="rounded" height={300} />
              ) : (
                <>
                <TableContainer component={Paper} variant="outlined">
                  <Table size="small" stickyHeader>
                    <TableHead>
                      <TableRow>
                        <TableCell>Job</TableCell>
                        <TableCell align="right">Cost</TableCell>
                        <TableCell align="right">DBUs</TableCell>
                        <TableCell align="right">Runs</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {topJobs
                        .slice(jobsPage * rowsPerPage, jobsPage * rowsPerPage + rowsPerPage)
                        .map((job) => {
                        const wsEntry = job.workspace_id ? workspaceUrls[job.workspace_id] : undefined;
                        const jobUrl = wsEntry
                          ? `${wsEntry.url}/jobs/${job.job_id}/tasks?o=${job.workspace_id}`
                          : undefined;
                        return (
                        <TableRow key={job.job_id} hover>
                          <TableCell>
                            {jobUrl ? (
                              <Link
                                href={jobUrl}
                                target="_blank"
                                rel="noopener noreferrer"
                                sx={{ display: 'inline-flex', alignItems: 'center', gap: 0.5 }}
                              >
                                <Typography variant="body2" noWrap sx={{ maxWidth: 200 }}>
                                  {job.job_name || `Job ${job.job_id}`}
                                </Typography>
                                <OpenInNew sx={{ fontSize: 14 }} />
                              </Link>
                            ) : (
                              <Typography variant="body2" noWrap sx={{ maxWidth: 200 }}>
                                {job.job_name || `Job ${job.job_id}`}
                              </Typography>
                            )}
                            <Typography variant="caption" color="text.secondary">
                              {job.job_id}
                            </Typography>
                          </TableCell>
                          <TableCell align="right">{fmt(job.total_cost)}</TableCell>
                          <TableCell align="right">{job.total_dbus.toLocaleString()}</TableCell>
                          <TableCell align="right">{job.run_count}</TableCell>
                        </TableRow>
                        );
                      })}
                    </TableBody>
                  </Table>
                </TableContainer>
                <TablePagination
                  component="div"
                  count={topJobs.length}
                  page={jobsPage}
                  onPageChange={(_, p) => setJobsPage(p)}
                  rowsPerPage={rowsPerPage}
                  rowsPerPageOptions={[10]}
                />
                </>
              )}
            </CardContent>
          </Card>
        </Grid>

        {/* Top Expensive Job Runs */}
        <Grid item xs={12} md={6}>
          <Card>
            <CardHeader title="Top Expensive Job Runs" />
            <CardContent>
              {loading ? (
                <Skeleton variant="rounded" height={300} />
              ) : (
                <>
                <TableContainer component={Paper} variant="outlined">
                  <Table size="small" stickyHeader>
                    <TableHead>
                      <TableRow>
                        <TableCell>Job Run</TableCell>
                        <TableCell align="right">Cost</TableCell>
                        <TableCell align="right">DBUs</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {topRuns
                        .slice(runsPage * rowsPerPage, runsPage * rowsPerPage + rowsPerPage)
                        .map((run) => {
                        const wsEntry = run.workspace_id ? workspaceUrls[run.workspace_id] : undefined;
                        const runUrl = wsEntry
                          ? `${wsEntry.url}/jobs/${run.job_id}/runs/${run.job_run_id}?o=${run.workspace_id}`
                          : undefined;
                        return (
                        <TableRow key={`${run.job_id}-${run.job_run_id}`} hover>
                          <TableCell>
                            {runUrl ? (
                              <Link
                                href={runUrl}
                                target="_blank"
                                rel="noopener noreferrer"
                                sx={{ display: 'inline-flex', alignItems: 'center', gap: 0.5 }}
                              >
                                <Typography variant="body2" noWrap sx={{ maxWidth: 180 }}>
                                  {run.job_name || `Job ${run.job_id}`}
                                </Typography>
                                <OpenInNew sx={{ fontSize: 14 }} />
                              </Link>
                            ) : (
                              <Typography variant="body2" noWrap sx={{ maxWidth: 180 }}>
                                {run.job_name || `Job ${run.job_id}`}
                              </Typography>
                            )}
                            <Typography variant="caption" color="text.secondary">
                              Run {run.job_run_id}
                            </Typography>
                          </TableCell>
                          <TableCell align="right">{fmt(run.cost)}</TableCell>
                          <TableCell align="right">{run.dbus.toLocaleString()}</TableCell>
                        </TableRow>
                        );
                      })}
                    </TableBody>
                  </Table>
                </TableContainer>
                <TablePagination
                  component="div"
                  count={topRuns.length}
                  page={runsPage}
                  onPageChange={(_, p) => setRunsPage(p)}
                  rowsPerPage={rowsPerPage}
                  rowsPerPageOptions={[10]}
                />
                </>
              )}
            </CardContent>
          </Card>
        </Grid>
      </Grid>
    </Box>
  );
};

export default CostAnalyticsPage;
