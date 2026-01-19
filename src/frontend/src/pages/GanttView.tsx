import React, { useState, useEffect } from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Skeleton,
  Alert,
  Chip,
} from '@mui/material';
import {
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  LineChart,
  Line,
} from 'recharts';

import { getJobRuns, getConcurrentJobs } from '../services/api';
import type { JobRun } from '../types';

const GanttView: React.FC = () => {
  const [hours, setHours] = useState(24);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [jobs, setJobs] = useState<JobRun[]>([]);
  const [concurrent, setConcurrent] = useState<{ time: string; concurrent_jobs: number }[]>([]);

  useEffect(() => {
    loadData();
  }, [hours]);

  const loadData = async () => {
    setLoading(true);
    setError(null);
    try {
      const days = Math.ceil(hours / 24);
      const [jobsRes, concurrentRes] = await Promise.all([
        getJobRuns(days, 200),
        getConcurrentJobs(hours),
      ]);
      setJobs(jobsRes.data);
      setConcurrent(concurrentRes.data);
    } catch (err) {
      console.error('Failed to load gantt data:', err);
      setError('Failed to load timeline data. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  const getStatusColor = (status: string | null) => {
    switch (status?.toUpperCase()) {
      case 'SUCCESS':
      case 'SUCCEEDED':
        return '#4CAF50';
      case 'FAILED':
      case 'ERROR':
        return '#F44336';
      case 'RUNNING':
      case 'PENDING':
        return '#FF9800';
      default:
        return '#9E9E9E';
    }
  };

  // Process jobs for timeline visualization
  const processedJobs = jobs
    .filter((job) => job.start_time)
    .map((job) => {
      const startTime = new Date(job.start_time!);
      const endTime = job.end_time ? new Date(job.end_time) : new Date();
      const duration = (endTime.getTime() - startTime.getTime()) / 1000; // seconds

      return {
        ...job,
        startTime,
        endTime,
        duration,
        displayName: job.job_name || `Job ${job.job_id}`,
      };
    })
    .sort((a, b) => b.startTime.getTime() - a.startTime.getTime())
    .slice(0, 30);

  // Find time range
  const minTime = processedJobs.length > 0
    ? Math.min(...processedJobs.map((j) => j.startTime.getTime()))
    : Date.now() - hours * 3600000;
  const maxTime = Date.now();
  const timeRange = maxTime - minTime;

  return (
    <Box>
      <Typography variant="h4" fontWeight={600} gutterBottom>
        Gantt View
      </Typography>

      {error && (
        <Alert severity="error" sx={{ mb: 3 }}>
          {error}
        </Alert>
      )}

      {/* Controls */}
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Box sx={{ display: 'flex', gap: 2, alignItems: 'center' }}>
            <FormControl size="small" sx={{ minWidth: 150 }}>
              <InputLabel>Time Range</InputLabel>
              <Select
                value={hours}
                label="Time Range"
                onChange={(e) => setHours(Number(e.target.value))}
              >
                <MenuItem value={6}>Last 6 Hours</MenuItem>
                <MenuItem value={12}>Last 12 Hours</MenuItem>
                <MenuItem value={24}>Last 24 Hours</MenuItem>
                <MenuItem value={48}>Last 48 Hours</MenuItem>
                <MenuItem value={72}>Last 72 Hours</MenuItem>
              </Select>
            </FormControl>
            <Typography variant="body2" color="text.secondary">
              Showing {processedJobs.length} jobs
            </Typography>
          </Box>
        </CardContent>
      </Card>

      {/* Legend */}
      <Box sx={{ display: 'flex', gap: 3, mb: 3 }}>
        {[
          { label: 'Success', color: '#4CAF50' },
          { label: 'Failed', color: '#F44336' },
          { label: 'Running', color: '#FF9800' },
        ].map((item) => (
          <Box key={item.label} sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <Box
              sx={{
                width: 16,
                height: 16,
                borderRadius: 1,
                bgcolor: item.color,
              }}
            />
            <Typography variant="body2">{item.label}</Typography>
          </Box>
        ))}
      </Box>

      {/* Concurrent Jobs Chart */}
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Typography variant="h6" gutterBottom>
            Concurrent Jobs Over Time
          </Typography>
          {loading ? (
            <Skeleton variant="rectangular" height={200} />
          ) : (
            <ResponsiveContainer width="100%" height={200}>
              <LineChart data={concurrent.slice(-50)}>
                <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                <XAxis
                  dataKey="time"
                  stroke="#888"
                  tick={{ fill: '#888' }}
                  tickFormatter={(value) => new Date(value).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' })}
                />
                <YAxis stroke="#888" tick={{ fill: '#888' }} />
                <Tooltip
                  contentStyle={{ backgroundColor: '#1A1A1A', border: '1px solid #333' }}
                  labelFormatter={(value) => new Date(value).toLocaleString()}
                />
                <Line
                  type="stepAfter"
                  dataKey="concurrent_jobs"
                  stroke="#FF6F00"
                  strokeWidth={2}
                  dot={false}
                  name="Concurrent Jobs"
                />
              </LineChart>
            </ResponsiveContainer>
          )}
        </CardContent>
      </Card>

      {/* Timeline */}
      <Card>
        <CardContent>
          <Typography variant="h6" gutterBottom>
            Job Timeline
          </Typography>
          {loading ? (
            <Box>
              {Array.from({ length: 10 }).map((_, i) => (
                <Box key={i} sx={{ display: 'flex', gap: 2, mb: 2 }}>
                  <Skeleton variant="text" width={150} height={24} />
                  <Skeleton variant="rectangular" width="100%" height={24} />
                </Box>
              ))}
            </Box>
          ) : processedJobs.length > 0 ? (
            <Box sx={{ overflowX: 'auto' }}>
              {/* Time axis */}
              <Box sx={{ display: 'flex', mb: 2, pl: '170px' }}>
                {Array.from({ length: 5 }).map((_, i) => {
                  const time = new Date(minTime + (timeRange * i) / 4);
                  return (
                    <Box
                      key={i}
                      sx={{
                        flex: 1,
                        textAlign: i === 4 ? 'right' : i === 0 ? 'left' : 'center',
                      }}
                    >
                      <Typography variant="caption" color="text.secondary">
                        {time.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' })}
                      </Typography>
                    </Box>
                  );
                })}
              </Box>

              {/* Job bars */}
              {processedJobs.map((job) => {
                const leftPct = ((job.startTime.getTime() - minTime) / timeRange) * 100;
                const widthPct = ((job.endTime.getTime() - job.startTime.getTime()) / timeRange) * 100;

                return (
                  <Box
                    key={`${job.job_id}-${job.run_id}`}
                    sx={{
                      display: 'flex',
                      alignItems: 'center',
                      mb: 1,
                      '&:hover': { bgcolor: 'action.hover' },
                      borderRadius: 1,
                      p: 0.5,
                    }}
                  >
                    <Typography
                      variant="body2"
                      noWrap
                      sx={{ width: 160, flexShrink: 0, pr: 1 }}
                    >
                      {job.displayName}
                    </Typography>
                    <Box
                      sx={{
                        flex: 1,
                        position: 'relative',
                        height: 24,
                        bgcolor: '#2A2A2A',
                        borderRadius: 1,
                      }}
                    >
                      <Box
                        sx={{
                          position: 'absolute',
                          left: `${leftPct}%`,
                          width: `${Math.max(widthPct, 0.5)}%`,
                          height: '100%',
                          bgcolor: getStatusColor(job.result_state),
                          borderRadius: 1,
                          cursor: 'pointer',
                          '&:hover': {
                            opacity: 0.8,
                          },
                        }}
                        title={`${job.displayName}\nStatus: ${job.result_state}\nDuration: ${Math.round(job.duration / 60)}m`}
                      />
                    </Box>
                    <Box sx={{ width: 80, pl: 1, flexShrink: 0 }}>
                      <Chip
                        label={job.result_state || 'UNKNOWN'}
                        size="small"
                        sx={{
                          bgcolor: getStatusColor(job.result_state),
                          color: 'white',
                          height: 20,
                          fontSize: '0.7rem',
                        }}
                      />
                    </Box>
                  </Box>
                );
              })}
            </Box>
          ) : (
            <Box sx={{ textAlign: 'center', py: 4 }}>
              <Typography color="text.secondary">
                No jobs found in the selected time range
              </Typography>
            </Box>
          )}
        </CardContent>
      </Card>
    </Box>
  );
};

export default GanttView;
