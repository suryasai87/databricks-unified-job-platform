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
} from '@mui/material';
import {
  CheckCircle,
  Error,
  HourglassEmpty,
  AttachMoney,
  TrendingUp,
  LocalOffer,
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
  getRunSummary,
  getDailyRuns,
  getRunsByType,
  getCostSummary,
  getCorrelationRate,
} from '../services/api';
import type { RunSummary, DailyRuns, CostSummary, CorrelationRate } from '../types';

const COLORS = ['#4CAF50', '#F44336', '#FF9800', '#2196F3', '#9C27B0'];

const Dashboard: React.FC = () => {
  const [days, setDays] = useState(7);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [runSummary, setRunSummary] = useState<RunSummary | null>(null);
  const [dailyRuns, setDailyRuns] = useState<DailyRuns[]>([]);
  const [runsByType, setRunsByType] = useState<{ run_type: string; count: number }[]>([]);
  const [costSummary, setCostSummary] = useState<CostSummary | null>(null);
  const [correlationRate, setCorrelationRate] = useState<CorrelationRate | null>(null);

  useEffect(() => {
    loadData();
  }, [days]);

  const loadData = async () => {
    setLoading(true);
    setError(null);

    try {
      const [summaryRes, dailyRes, typeRes, costRes, corrRes] = await Promise.all([
        getRunSummary(days),
        getDailyRuns(days),
        getRunsByType(days),
        getCostSummary(30),
        getCorrelationRate(days),
      ]);

      setRunSummary(summaryRes.data);
      setDailyRuns(dailyRes.data);
      setRunsByType(typeRes.data);
      setCostSummary(costRes.data);
      setCorrelationRate(corrRes.data);
    } catch (err) {
      console.error('Failed to load dashboard data:', err);
      setError('Failed to load dashboard data. Please try again.');
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
          Dashboard
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
            title="Total Runs"
            value={runSummary?.total_runs?.toLocaleString() || '0'}
            subtitle={`Last ${days} days`}
            icon={<TrendingUp />}
            color="primary"
            loading={loading}
          />
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <MetricCard
            title="Succeeded"
            value={runSummary?.succeeded?.toLocaleString() || '0'}
            subtitle={`${runSummary?.success_rate?.toFixed(1) || 0}% success rate`}
            icon={<CheckCircle />}
            color="success"
            loading={loading}
          />
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <MetricCard
            title="Failed"
            value={runSummary?.failed?.toLocaleString() || '0'}
            icon={<Error />}
            color="error"
            loading={loading}
          />
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <MetricCard
            title="Running"
            value={runSummary?.running?.toLocaleString() || '0'}
            icon={<HourglassEmpty />}
            color="warning"
            loading={loading}
          />
        </Grid>
      </Grid>

      {/* Cost & Correlation Cards */}
      <Grid container spacing={3} sx={{ mb: 3 }}>
        <Grid item xs={12} sm={6} md={3}>
          <MetricCard
            title="Total Cost (30d)"
            value={formatCurrency(costSummary?.total_cost_usd || 0)}
            subtitle={`${formatCurrency(costSummary?.avg_daily_cost || 0)}/day avg`}
            icon={<AttachMoney />}
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
            title="Tag Correlation Rate"
            value={`${correlationRate?.correlation_rate_pct || 0}%`}
            subtitle={`${correlationRate?.matched || 0} matched`}
            icon={<LocalOffer />}
            color={correlationRate?.correlation_rate_pct && correlationRate.correlation_rate_pct >= 80 ? 'success' : 'warning'}
            loading={loading}
          />
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <MetricCard
            title="Unique Jobs"
            value={costSummary?.unique_jobs?.toLocaleString() || '0'}
            loading={loading}
          />
        </Grid>
      </Grid>

      {/* Charts */}
      <Grid container spacing={3}>
        {/* Daily Runs Chart */}
        <Grid item xs={12} md={8}>
          <Card>
            <CardHeader title="Daily Job Runs" />
            <CardContent>
              {loading ? (
                <Skeleton variant="rectangular" height={300} />
              ) : (
                <ResponsiveContainer width="100%" height={300}>
                  <AreaChart data={dailyRuns}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                    <XAxis
                      dataKey="date"
                      stroke="#888"
                      tick={{ fill: '#888' }}
                      tickFormatter={(value) => new Date(value).toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}
                    />
                    <YAxis stroke="#888" tick={{ fill: '#888' }} />
                    <Tooltip
                      contentStyle={{ backgroundColor: '#1A1A1A', border: '1px solid #333' }}
                    />
                    <Area
                      type="monotone"
                      dataKey="succeeded"
                      stackId="1"
                      stroke="#4CAF50"
                      fill="#4CAF50"
                      fillOpacity={0.6}
                      name="Succeeded"
                    />
                    <Area
                      type="monotone"
                      dataKey="failed"
                      stackId="1"
                      stroke="#F44336"
                      fill="#F44336"
                      fillOpacity={0.6}
                      name="Failed"
                    />
                  </AreaChart>
                </ResponsiveContainer>
              )}
            </CardContent>
          </Card>
        </Grid>

        {/* Runs by Type Pie Chart */}
        <Grid item xs={12} md={4}>
          <Card>
            <CardHeader title="Runs by Type" />
            <CardContent>
              {loading ? (
                <Skeleton variant="circular" width={200} height={200} sx={{ mx: 'auto' }} />
              ) : (
                <ResponsiveContainer width="100%" height={300}>
                  <PieChart>
                    <Pie
                      data={runsByType}
                      dataKey="count"
                      nameKey="run_type"
                      cx="50%"
                      cy="50%"
                      outerRadius={80}
                      label={({ name, percent }) => `${name} (${(percent * 100).toFixed(0)}%)`}
                    >
                      {runsByType.map((_, index) => (
                        <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                      ))}
                    </Pie>
                    <Tooltip
                      contentStyle={{ backgroundColor: '#1A1A1A', border: '1px solid #333' }}
                    />
                    <Legend />
                  </PieChart>
                </ResponsiveContainer>
              )}
            </CardContent>
          </Card>
        </Grid>
      </Grid>
    </Box>
  );
};

export default Dashboard;
