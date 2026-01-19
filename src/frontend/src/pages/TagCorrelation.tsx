import React, { useState, useEffect } from 'react';
import {
  Box,
  Grid,
  Card,
  CardContent,
  CardHeader,
  Typography,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Skeleton,
  Alert,
  AlertTitle,
  Chip,
  LinearProgress,
  Button,
} from '@mui/material';
import { LocalOffer, ContentCopy } from '@mui/icons-material';
import {
  PieChart,
  Pie,
  Cell,
  ResponsiveContainer,
  Tooltip,
  Legend,
} from 'recharts';

import { MetricCard, StatusChip } from '../components';
import {
  getTagSummary,
  getTagCorrelations,
  getTagsByPipeline,
} from '../services/api';
/* eslint-disable @typescript-eslint/no-unused-vars */
import type { TagSummary, TagCorrelation } from '../types';

const TagCorrelationPage: React.FC = () => {
  const [days, setDays] = useState(30);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);

  const [summary, setSummary] = useState<TagSummary | null>(null);
  const [correlations, setCorrelations] = useState<TagCorrelation[]>([]);
  const [byPipeline, setByPipeline] = useState<any[]>([]);

  useEffect(() => {
    loadData();
  }, [days]);

  const loadData = async () => {
    setLoading(true);
    setError(null);

    try {
      const [summaryRes, corrRes, pipelineRes] = await Promise.all([
        getTagSummary(days),
        getTagCorrelations(days, 50),
        getTagsByPipeline(days, 10),
      ]);

      setSummary(summaryRes.data);
      setCorrelations(corrRes.data);
      setByPipeline(pipelineRes.data);
    } catch (err) {
      console.error('Failed to load tag data:', err);
      setError('Failed to load tag correlation data. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  const handleCopySetupCommand = () => {
    const command = `-- Run this in Databricks SQL to create tag correlation infrastructure
%run /Users/\${current_user}/unified-job-platform/sql/01_create_infrastructure.sql`;
    navigator.clipboard.writeText(command);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  // Show setup required message if no data
  if (!loading && summary?.setup_required) {
    return (
      <Box>
        <Typography variant="h4" fontWeight={600} gutterBottom>
          Tag Correlation
        </Typography>

        <Alert severity="info" sx={{ mb: 3 }}>
          <AlertTitle>Setup Required</AlertTitle>
          The tag correlation tables need to be created before you can use this feature.
          Run the setup script to create the required infrastructure.
        </Alert>

        <Card>
          <CardContent>
            <Typography variant="h6" gutterBottom>
              Quick Setup
            </Typography>
            <Typography variant="body2" color="text.secondary" paragraph>
              Run the following command in a Databricks notebook to create the required tables and views:
            </Typography>
            <Box
              sx={{
                bgcolor: '#1E1E1E',
                p: 2,
                borderRadius: 1,
                fontFamily: 'monospace',
                fontSize: '0.9rem',
                mb: 2,
              }}
            >
              <code>%sql</code>
              <br />
              <code>-- Deploy the infrastructure SQL script</code>
              <br />
              <code>-- See: /sql/01_create_infrastructure.sql</code>
            </Box>
            <Button
              variant="contained"
              startIcon={<ContentCopy />}
              onClick={handleCopySetupCommand}
            >
              {copied ? 'Copied!' : 'Copy Setup Command'}
            </Button>
          </CardContent>
        </Card>
      </Box>
    );
  }

  return (
    <Box>
      {/* Header */}
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
        <Typography variant="h4" fontWeight={600}>
          Tag Correlation
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

      {/* Summary Cards */}
      <Grid container spacing={3} sx={{ mb: 3 }}>
        <Grid item xs={12} sm={6} md={3}>
          <MetricCard
            title="Total Records"
            value={summary?.total_records?.toLocaleString() || '0'}
            icon={<LocalOffer />}
            loading={loading}
          />
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <MetricCard
            title="Unique Projects"
            value={summary?.unique_projects || 0}
            loading={loading}
          />
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <MetricCard
            title="Unique Departments"
            value={summary?.unique_departments || 0}
            loading={loading}
          />
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <MetricCard
            title="ADF Pipelines"
            value={summary?.unique_pipelines || 0}
            loading={loading}
          />
        </Grid>
      </Grid>

      {/* Tagging Completeness */}
      <Grid container spacing={3} sx={{ mb: 3 }}>
        <Grid item xs={12} md={6}>
          <Card>
            <CardHeader title="Tagging Completeness" />
            <CardContent>
              {loading ? (
                <Skeleton variant="rectangular" height={100} />
              ) : (
                <Box>
                  <Box sx={{ mb: 2 }}>
                    <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 1 }}>
                      <Typography variant="body2">Project Code</Typography>
                      <Typography variant="body2">
                        {summary?.tagging_completeness?.project_code_pct || 0}%
                      </Typography>
                    </Box>
                    <LinearProgress
                      variant="determinate"
                      value={summary?.tagging_completeness?.project_code_pct || 0}
                      sx={{ height: 10, borderRadius: 5 }}
                      color={
                        (summary?.tagging_completeness?.project_code_pct || 0) >= 80
                          ? 'success'
                          : 'warning'
                      }
                    />
                  </Box>
                  <Box>
                    <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 1 }}>
                      <Typography variant="body2">Department</Typography>
                      <Typography variant="body2">
                        {summary?.tagging_completeness?.department_pct || 0}%
                      </Typography>
                    </Box>
                    <LinearProgress
                      variant="determinate"
                      value={summary?.tagging_completeness?.department_pct || 0}
                      sx={{ height: 10, borderRadius: 5 }}
                      color={
                        (summary?.tagging_completeness?.department_pct || 0) >= 80
                          ? 'success'
                          : 'warning'
                      }
                    />
                  </Box>
                </Box>
              )}
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} md={6}>
          <Card>
            <CardHeader title="Run Status Distribution" />
            <CardContent>
              {loading ? (
                <Skeleton variant="circular" width={150} height={150} sx={{ mx: 'auto' }} />
              ) : (
                <ResponsiveContainer width="100%" height={150}>
                  <PieChart>
                    <Pie
                      data={[
                        { name: 'Successful', value: summary?.run_status?.successful || 0 },
                        { name: 'Failed', value: summary?.run_status?.failed || 0 },
                      ]}
                      dataKey="value"
                      nameKey="name"
                      cx="50%"
                      cy="50%"
                      outerRadius={60}
                    >
                      <Cell fill="#4CAF50" />
                      <Cell fill="#F44336" />
                    </Pie>
                    <Tooltip />
                    <Legend />
                  </PieChart>
                </ResponsiveContainer>
              )}
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* By Pipeline */}
      <Card sx={{ mb: 3 }}>
        <CardHeader title="Correlation by ADF Pipeline" />
        <TableContainer>
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>Pipeline Name</TableCell>
                <TableCell align="right">Job Runs</TableCell>
                <TableCell align="right">Projects</TableCell>
                <TableCell align="right">Departments</TableCell>
                <TableCell align="right">Success Rate</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {loading ? (
                Array.from({ length: 5 }).map((_, i) => (
                  <TableRow key={i}>
                    {Array.from({ length: 5 }).map((_, j) => (
                      <TableCell key={j}>
                        <Skeleton variant="text" />
                      </TableCell>
                    ))}
                  </TableRow>
                ))
              ) : byPipeline.length > 0 ? (
                byPipeline.map((pipeline) => (
                  <TableRow key={pipeline.pipeline_name} hover>
                    <TableCell>{pipeline.pipeline_name}</TableCell>
                    <TableCell align="right">{pipeline.job_runs}</TableCell>
                    <TableCell align="right">{pipeline.unique_projects}</TableCell>
                    <TableCell align="right">{pipeline.unique_departments}</TableCell>
                    <TableCell align="right">
                      <Chip
                        label={`${pipeline.success_rate_pct}%`}
                        size="small"
                        color={pipeline.success_rate_pct >= 90 ? 'success' : 'warning'}
                      />
                    </TableCell>
                  </TableRow>
                ))
              ) : (
                <TableRow>
                  <TableCell colSpan={5} align="center">
                    No pipeline data available
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </TableContainer>
      </Card>

      {/* Recent Correlations */}
      <Card>
        <CardHeader title="Recent Tag Correlations" />
        <TableContainer sx={{ maxHeight: 400 }}>
          <Table size="small" stickyHeader>
            <TableHead>
              <TableRow>
                <TableCell>Time</TableCell>
                <TableCell>Job Run</TableCell>
                <TableCell>Pipeline</TableCell>
                <TableCell>Project</TableCell>
                <TableCell>Department</TableCell>
                <TableCell>Status</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {loading ? (
                Array.from({ length: 10 }).map((_, i) => (
                  <TableRow key={i}>
                    {Array.from({ length: 6 }).map((_, j) => (
                      <TableCell key={j}>
                        <Skeleton variant="text" />
                      </TableCell>
                    ))}
                  </TableRow>
                ))
              ) : correlations.length > 0 ? (
                correlations.map((corr) => (
                  <TableRow key={corr.correlation_id} hover>
                    <TableCell>
                      {corr.run_start_time
                        ? new Date(corr.run_start_time).toLocaleString()
                        : '-'}
                    </TableCell>
                    <TableCell>{corr.job_run_id || '-'}</TableCell>
                    <TableCell>{corr.adf_pipeline_name || 'Direct'}</TableCell>
                    <TableCell>
                      <Chip label={corr.project_code || 'Untagged'} size="small" />
                    </TableCell>
                    <TableCell>{corr.department || '-'}</TableCell>
                    <TableCell>
                      <StatusChip status={corr.run_status} />
                    </TableCell>
                  </TableRow>
                ))
              ) : (
                <TableRow>
                  <TableCell colSpan={6} align="center">
                    No correlation records found
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </TableContainer>
      </Card>
    </Box>
  );
};

export default TagCorrelationPage;
