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
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Skeleton,
  Alert,
  Chip,
} from '@mui/material';
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
  getCostByDepartment,
  getCorrelationRate,
} from '../services/api';
import type { CostSummary, DailyCost, TopJob, CostByDepartment, CorrelationRate } from '../types';

const COLORS = ['#FF6F00', '#00B8D4', '#4CAF50', '#9C27B0', '#FF9800', '#2196F3', '#E91E63'];

const CostAnalytics: React.FC = () => {
  const [days, setDays] = useState(30);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [costSummary, setCostSummary] = useState<CostSummary | null>(null);
  const [dailyCosts, setDailyCosts] = useState<DailyCost[]>([]);
  const [topJobs, setTopJobs] = useState<TopJob[]>([]);
  const [byDepartment, setByDepartment] = useState<CostByDepartment[]>([]);
  const [correlationRate, setCorrelationRate] = useState<CorrelationRate | null>(null);

  useEffect(() => {
    loadData();
  }, [days]);

  const loadData = async () => {
    setLoading(true);
    setError(null);

    try {
      const [summaryRes, dailyRes, topRes, deptRes, corrRes] = await Promise.all([
        getCostSummary(days),
        getDailyCosts(days),
        getTopExpensiveJobs(days, 10),
        getCostByDepartment(days),
        getCorrelationRate(days),
      ]);

      setCostSummary(summaryRes.data);
      setDailyCosts(dailyRes.data);
      setTopJobs(topRes.data);
      setByDepartment(deptRes.data);
      setCorrelationRate(corrRes.data);
    } catch (err) {
      console.error('Failed to load cost data:', err);
      setError('Failed to load cost analytics. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
    }).format(value);
  };

  return (
    <Box>
      {/* Header */}
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
        <Typography variant="h4" fontWeight={600}>
          Cost Analytics
        </Typography>
        <FormControl size="small" sx={{ minWidth: 150 }}>
          <InputLabel>Time Range</InputLabel>
          <Select
            value={days}
            label="Time Range"
            onChange={(e) => setDays(Number(e.target.value))}
          >
            <MenuItem value={7}>Last 7 Days</MenuItem>
            <MenuItem value={14}>Last 14 Days</MenuItem>
            <MenuItem value={30}>Last 30 Days</MenuItem>
            <MenuItem value={60}>Last 60 Days</MenuItem>
            <MenuItem value={90}>Last 90 Days</MenuItem>
          </Select>
        </FormControl>
      </Box>

      {error && (
        <Alert severity="error" sx={{ mb: 3 }}>
          {error}
        </Alert>
      )}

      {/* KPI Cards */}
      <Grid container spacing={3} sx={{ mb: 3 }}>
        <Grid item xs={12} sm={6} md={3}>
          <MetricCard
            title="Total Cost"
            value={formatCurrency(costSummary?.total_cost_usd || 0)}
            subtitle={`Last ${days} days`}
            color="primary"
            loading={loading}
          />
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <MetricCard
            title="Avg Daily Cost"
            value={formatCurrency(costSummary?.avg_daily_cost || 0)}
            color="secondary"
            loading={loading}
          />
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <MetricCard
            title="Total DBUs"
            value={(costSummary?.total_dbus || 0).toLocaleString()}
            loading={loading}
          />
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <MetricCard
            title="Avg Cost/Run"
            value={formatCurrency(costSummary?.avg_cost_per_run || 0)}
            loading={loading}
          />
        </Grid>
      </Grid>

      {/* Tag Correlation Alert */}
      {correlationRate && correlationRate.correlation_rate_pct < 80 && (
        <Alert severity="warning" sx={{ mb: 3 }}>
          Tag correlation rate is {correlationRate.correlation_rate_pct}%.
          {correlationRate.unmatched} runs are untagged, representing potential cost attribution gaps.
        </Alert>
      )}

      {/* Charts Row */}
      <Grid container spacing={3} sx={{ mb: 3 }}>
        {/* Daily Cost Trend */}
        <Grid item xs={12} md={8}>
          <Card>
            <CardHeader title="Daily Cost Trend" />
            <CardContent>
              {loading ? (
                <Skeleton variant="rectangular" height={300} />
              ) : (
                <ResponsiveContainer width="100%" height={300}>
                  <AreaChart data={dailyCosts}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                    <XAxis
                      dataKey="date"
                      stroke="#888"
                      tick={{ fill: '#888' }}
                      tickFormatter={(value) => new Date(value).toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}
                    />
                    <YAxis
                      stroke="#888"
                      tick={{ fill: '#888' }}
                      tickFormatter={(value) => `$${value}`}
                    />
                    <Tooltip
                      contentStyle={{ backgroundColor: '#1A1A1A', border: '1px solid #333' }}
                      formatter={(value: number) => [formatCurrency(value), 'Cost']}
                    />
                    <Area
                      type="monotone"
                      dataKey="cost"
                      stroke="#FF6F00"
                      fill="#FF6F00"
                      fillOpacity={0.3}
                      name="Daily Cost"
                    />
                  </AreaChart>
                </ResponsiveContainer>
              )}
            </CardContent>
          </Card>
        </Grid>

        {/* Cost by Department */}
        <Grid item xs={12} md={4}>
          <Card>
            <CardHeader title="Cost by Department" />
            <CardContent>
              {loading ? (
                <Skeleton variant="circular" width={200} height={200} sx={{ mx: 'auto' }} />
              ) : byDepartment.length > 0 ? (
                <ResponsiveContainer width="100%" height={300}>
                  <PieChart>
                    <Pie
                      data={byDepartment}
                      dataKey="total_cost"
                      nameKey="department"
                      cx="50%"
                      cy="50%"
                      outerRadius={80}
                      label={({ percent }) => `${(percent * 100).toFixed(0)}%`}
                    >
                      {byDepartment.map((_, index) => (
                        <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                      ))}
                    </Pie>
                    <Tooltip
                      contentStyle={{ backgroundColor: '#1A1A1A', border: '1px solid #333' }}
                      formatter={(value: number) => formatCurrency(value)}
                    />
                    <Legend />
                  </PieChart>
                </ResponsiveContainer>
              ) : (
                <Box sx={{ textAlign: 'center', py: 4 }}>
                  <Typography color="text.secondary">
                    No department data available.
                    <br />
                    Run the setup script to enable tag correlation.
                  </Typography>
                </Box>
              )}
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Top Expensive Jobs */}
      <Card>
        <CardHeader
          title="Top Expensive Jobs"
          subheader={`Top 10 most expensive jobs in the last ${days} days`}
        />
        <TableContainer>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell>Rank</TableCell>
                <TableCell>Job ID</TableCell>
                <TableCell>Notebook Path</TableCell>
                <TableCell align="right">Total Cost</TableCell>
                <TableCell align="right">DBUs</TableCell>
                <TableCell align="right">Run Count</TableCell>
                <TableCell align="right">Avg Cost/Run</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {loading ? (
                Array.from({ length: 5 }).map((_, i) => (
                  <TableRow key={i}>
                    {Array.from({ length: 7 }).map((_, j) => (
                      <TableCell key={j}>
                        <Skeleton variant="text" />
                      </TableCell>
                    ))}
                  </TableRow>
                ))
              ) : (
                topJobs.map((job, index) => (
                  <TableRow key={job.job_id} hover>
                    <TableCell>
                      <Chip
                        label={index + 1}
                        size="small"
                        color={index < 3 ? 'error' : 'default'}
                      />
                    </TableCell>
                    <TableCell>{job.job_id}</TableCell>
                    <TableCell>
                      <Typography variant="body2" noWrap sx={{ maxWidth: 300 }}>
                        {job.notebook_path || '-'}
                      </Typography>
                    </TableCell>
                    <TableCell align="right">
                      <Typography fontWeight={600} color="primary">
                        {formatCurrency(job.total_cost)}
                      </Typography>
                    </TableCell>
                    <TableCell align="right">{job.total_dbus.toLocaleString()}</TableCell>
                    <TableCell align="right">{job.run_count}</TableCell>
                    <TableCell align="right">{formatCurrency(job.avg_cost_per_run)}</TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </TableContainer>
      </Card>
    </Box>
  );
};

export default CostAnalytics;
