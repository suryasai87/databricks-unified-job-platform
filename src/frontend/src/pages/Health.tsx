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
  Skeleton,
  Alert,
  Chip,
  LinearProgress,
} from '@mui/material';
import { Warning, Error as ErrorIcon, CheckCircle } from '@mui/icons-material';

import { MetricCard, StatusChip } from '../components';
import {
  getFailedJobs,
  getProlongedJobs,
  getAnomalies,
  getSLAStatus,
} from '../services/api';
import type { FailedJob, ProlongedJob, Anomaly, SLAStatus } from '../types';

const Health: React.FC = () => {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [failedJobs, setFailedJobs] = useState<FailedJob[]>([]);
  const [prolongedJobs, setProlongedJobs] = useState<ProlongedJob[]>([]);
  const [anomalies, setAnomalies] = useState<Anomaly[]>([]);
  const [slaStatus, setSLAStatus] = useState<SLAStatus[]>([]);

  useEffect(() => {
    loadData();
  }, []);

  const loadData = async () => {
    setLoading(true);
    setError(null);

    try {
      const [failedRes, prolongedRes, anomaliesRes, slaRes] = await Promise.all([
        getFailedJobs(7, 3, 10),
        getProlongedJobs(1.5, 2.0),
        getAnomalies(7, 30, 2.0),
        getSLAStatus(7, 2.0),
      ]);

      setFailedJobs(failedRes.data);
      setProlongedJobs(prolongedRes.data);
      setAnomalies(anomaliesRes.data);
      setSLAStatus(slaRes.data);
    } catch (err) {
      console.error('Failed to load health data:', err);
      setError('Failed to load health metrics. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  const formatDuration = (seconds: number) => {
    if (seconds < 60) return `${seconds.toFixed(0)}s`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.floor(seconds % 60)}s`;
    return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
  };

  const getHealthScore = () => {
    const slaCompliant = slaStatus.filter((s) => s.status === 'compliant').length;
    const total = slaStatus.length;
    if (total === 0) return 100;
    return Math.round((slaCompliant / total) * 100);
  };

  const healthScore = getHealthScore();

  return (
    <Box>
      <Typography variant="h4" fontWeight={600} gutterBottom>
        Health Monitor
      </Typography>

      {error && (
        <Alert severity="error" sx={{ mb: 3 }}>
          {error}
        </Alert>
      )}

      {/* Summary Cards */}
      <Grid container spacing={3} sx={{ mb: 3 }}>
        <Grid item xs={12} sm={6} md={3}>
          <MetricCard
            title="Health Score"
            value={`${healthScore}%`}
            icon={healthScore >= 80 ? <CheckCircle /> : <Warning />}
            color={healthScore >= 80 ? 'success' : healthScore >= 60 ? 'warning' : 'error'}
            loading={loading}
          />
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <MetricCard
            title="Failed Jobs"
            value={failedJobs.length}
            subtitle="Jobs with recent failures"
            icon={<ErrorIcon />}
            color="error"
            loading={loading}
          />
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <MetricCard
            title="Prolonged Runs"
            value={prolongedJobs.length}
            subtitle="Running longer than expected"
            icon={<Warning />}
            color="warning"
            loading={loading}
          />
        </Grid>
        <Grid item xs={12} sm={6} md={3}>
          <MetricCard
            title="Anomalies"
            value={anomalies.length}
            subtitle="Unusual patterns detected"
            color="secondary"
            loading={loading}
          />
        </Grid>
      </Grid>

      <Grid container spacing={3}>
        {/* Failed Jobs */}
        <Grid item xs={12} md={6}>
          <Card>
            <CardHeader
              title="Failed Jobs"
              subheader="Jobs with failures in the last 7 days"
              avatar={<ErrorIcon color="error" />}
            />
            <TableContainer sx={{ maxHeight: 400 }}>
              <Table size="small" stickyHeader>
                <TableHead>
                  <TableRow>
                    <TableCell>Job</TableCell>
                    <TableCell align="right">Failed</TableCell>
                    <TableCell align="right">Total</TableCell>
                    <TableCell align="right">Success Rate</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {loading ? (
                    Array.from({ length: 5 }).map((_, i) => (
                      <TableRow key={i}>
                        {Array.from({ length: 4 }).map((_, j) => (
                          <TableCell key={j}>
                            <Skeleton variant="text" />
                          </TableCell>
                        ))}
                      </TableRow>
                    ))
                  ) : failedJobs.length > 0 ? (
                    failedJobs.map((job) => (
                      <TableRow key={job.job_id} hover>
                        <TableCell>
                          <Typography variant="body2" noWrap sx={{ maxWidth: 150 }}>
                            {job.job_name || `Job ${job.job_id}`}
                          </Typography>
                        </TableCell>
                        <TableCell align="right">
                          <Chip label={job.failed_runs} size="small" color="error" />
                        </TableCell>
                        <TableCell align="right">{job.total_runs}</TableCell>
                        <TableCell align="right">
                          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                            <LinearProgress
                              variant="determinate"
                              value={job.success_rate}
                              sx={{ flex: 1, height: 8, borderRadius: 4 }}
                              color={job.success_rate >= 80 ? 'success' : job.success_rate >= 50 ? 'warning' : 'error'}
                            />
                            <Typography variant="body2" sx={{ minWidth: 40 }}>
                              {job.success_rate}%
                            </Typography>
                          </Box>
                        </TableCell>
                      </TableRow>
                    ))
                  ) : (
                    <TableRow>
                      <TableCell colSpan={4} align="center">
                        <Typography color="success.main">No failed jobs found</Typography>
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
            </TableContainer>
          </Card>
        </Grid>

        {/* Prolonged Jobs */}
        <Grid item xs={12} md={6}>
          <Card>
            <CardHeader
              title="Prolonged Runs"
              subheader="Currently running jobs exceeding expected duration"
              avatar={<Warning color="warning" />}
            />
            <TableContainer sx={{ maxHeight: 400 }}>
              <Table size="small" stickyHeader>
                <TableHead>
                  <TableRow>
                    <TableCell>Job</TableCell>
                    <TableCell>Duration</TableCell>
                    <TableCell>Expected</TableCell>
                    <TableCell>Status</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {loading ? (
                    Array.from({ length: 5 }).map((_, i) => (
                      <TableRow key={i}>
                        {Array.from({ length: 4 }).map((_, j) => (
                          <TableCell key={j}>
                            <Skeleton variant="text" />
                          </TableCell>
                        ))}
                      </TableRow>
                    ))
                  ) : prolongedJobs.length > 0 ? (
                    prolongedJobs.map((job) => (
                      <TableRow key={job.run_id} hover>
                        <TableCell>
                          <Typography variant="body2" noWrap sx={{ maxWidth: 150 }}>
                            {job.job_name || `Job ${job.job_id}`}
                          </Typography>
                        </TableCell>
                        <TableCell>{formatDuration(job.duration_seconds)}</TableCell>
                        <TableCell>{formatDuration(job.avg_duration_seconds)}</TableCell>
                        <TableCell>
                          <Chip
                            label={job.status}
                            size="small"
                            color={job.status === 'critical' ? 'error' : 'warning'}
                          />
                        </TableCell>
                      </TableRow>
                    ))
                  ) : (
                    <TableRow>
                      <TableCell colSpan={4} align="center">
                        <Typography color="success.main">No prolonged runs detected</Typography>
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
            </TableContainer>
          </Card>
        </Grid>

        {/* SLA Status */}
        <Grid item xs={12}>
          <Card>
            <CardHeader
              title="SLA Compliance"
              subheader="Job performance against SLA targets (2x average duration threshold)"
            />
            <TableContainer sx={{ maxHeight: 400 }}>
              <Table size="small" stickyHeader>
                <TableHead>
                  <TableRow>
                    <TableCell>Job</TableCell>
                    <TableCell align="right">Runs</TableCell>
                    <TableCell align="right">Avg Duration</TableCell>
                    <TableCell align="right">Violations</TableCell>
                    <TableCell align="right">Compliance</TableCell>
                    <TableCell>Status</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {loading ? (
                    Array.from({ length: 5 }).map((_, i) => (
                      <TableRow key={i}>
                        {Array.from({ length: 6 }).map((_, j) => (
                          <TableCell key={j}>
                            <Skeleton variant="text" />
                          </TableCell>
                        ))}
                      </TableRow>
                    ))
                  ) : (
                    slaStatus.slice(0, 20).map((job) => (
                      <TableRow key={job.job_id} hover>
                        <TableCell>
                          <Typography variant="body2" noWrap sx={{ maxWidth: 200 }}>
                            {job.job_name || `Job ${job.job_id}`}
                          </Typography>
                        </TableCell>
                        <TableCell align="right">{job.total_runs}</TableCell>
                        <TableCell align="right">{formatDuration(job.avg_duration_seconds)}</TableCell>
                        <TableCell align="right">
                          {job.sla_violations > 0 ? (
                            <Chip label={job.sla_violations} size="small" color="error" />
                          ) : (
                            <Chip label="0" size="small" color="success" />
                          )}
                        </TableCell>
                        <TableCell align="right">{job.compliance_rate_pct}%</TableCell>
                        <TableCell>
                          <StatusChip status={job.status} />
                        </TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </TableContainer>
          </Card>
        </Grid>
      </Grid>
    </Box>
  );
};

export default Health;
