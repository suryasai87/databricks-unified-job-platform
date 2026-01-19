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
  Tooltip,
  TextField,
  InputAdornment,
} from '@mui/material';
import { Search } from '@mui/icons-material';

import { getJobsMatrix } from '../services/api';
import type { MatrixJob } from '../types';

const MatrixView: React.FC = () => {
  const [runsPerJob, setRunsPerJob] = useState(10);
  const [search, setSearch] = useState('');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [jobs, setJobs] = useState<MatrixJob[]>([]);

  useEffect(() => {
    loadData();
  }, [runsPerJob]);

  const loadData = async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await getJobsMatrix(100, runsPerJob);
      setJobs(res.data);
    } catch (err) {
      console.error('Failed to load matrix data:', err);
      setError('Failed to load matrix view. Please try again.');
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
      case 'CANCELLED':
      case 'SKIPPED':
        return '#9E9E9E';
      default:
        return '#616161';
    }
  };

  const filteredJobs = jobs.filter((job) =>
    job.job_name?.toLowerCase().includes(search.toLowerCase()) ||
    job.job_id.toString().includes(search)
  );

  const formatDuration = (seconds: number | null) => {
    if (!seconds) return '-';
    if (seconds < 60) return `${seconds.toFixed(0)}s`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
    return `${Math.floor(seconds / 3600)}h`;
  };

  return (
    <Box>
      <Typography variant="h4" fontWeight={600} gutterBottom>
        Matrix View
      </Typography>

      {error && (
        <Alert severity="error" sx={{ mb: 3 }}>
          {error}
        </Alert>
      )}

      {/* Controls */}
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap' }}>
            <TextField
              size="small"
              placeholder="Search jobs..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              InputProps={{
                startAdornment: (
                  <InputAdornment position="start">
                    <Search />
                  </InputAdornment>
                ),
              }}
              sx={{ minWidth: 250 }}
            />
            <FormControl size="small" sx={{ minWidth: 150 }}>
              <InputLabel>Runs Per Job</InputLabel>
              <Select
                value={runsPerJob}
                label="Runs Per Job"
                onChange={(e) => setRunsPerJob(Number(e.target.value))}
              >
                <MenuItem value={5}>5 runs</MenuItem>
                <MenuItem value={10}>10 runs</MenuItem>
                <MenuItem value={20}>20 runs</MenuItem>
                <MenuItem value={30}>30 runs</MenuItem>
              </Select>
            </FormControl>
          </Box>
        </CardContent>
      </Card>

      {/* Legend */}
      <Box sx={{ display: 'flex', gap: 3, mb: 3 }}>
        {[
          { label: 'Success', color: '#4CAF50' },
          { label: 'Failed', color: '#F44336' },
          { label: 'Running', color: '#FF9800' },
          { label: 'Cancelled', color: '#9E9E9E' },
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

      {/* Matrix Grid */}
      <Card>
        <CardContent>
          {loading ? (
            <Box>
              {Array.from({ length: 10 }).map((_, i) => (
                <Box key={i} sx={{ display: 'flex', gap: 1, mb: 1 }}>
                  <Skeleton variant="text" width={200} height={32} />
                  {Array.from({ length: runsPerJob }).map((_, j) => (
                    <Skeleton key={j} variant="rectangular" width={32} height={32} />
                  ))}
                </Box>
              ))}
            </Box>
          ) : filteredJobs.length > 0 ? (
            <Box sx={{ overflowX: 'auto' }}>
              {/* Header */}
              <Box sx={{ display: 'flex', gap: 1, mb: 2 }}>
                <Box sx={{ width: 200, flexShrink: 0 }}>
                  <Typography variant="subtitle2" color="text.secondary">
                    Job Name
                  </Typography>
                </Box>
                <Box sx={{ display: 'flex', gap: 1 }}>
                  {Array.from({ length: runsPerJob }).map((_, i) => (
                    <Box
                      key={i}
                      sx={{ width: 32, textAlign: 'center' }}
                    >
                      <Typography variant="caption" color="text.secondary">
                        {i + 1}
                      </Typography>
                    </Box>
                  ))}
                </Box>
              </Box>

              {/* Rows */}
              {filteredJobs.map((job) => (
                <Box
                  key={job.job_id}
                  sx={{
                    display: 'flex',
                    gap: 1,
                    mb: 1,
                    '&:hover': { bgcolor: 'action.hover' },
                    borderRadius: 1,
                    p: 0.5,
                  }}
                >
                  <Box sx={{ width: 200, flexShrink: 0 }}>
                    <Tooltip title={`Job ID: ${job.job_id}`}>
                      <Typography variant="body2" noWrap>
                        {job.job_name || `Job ${job.job_id}`}
                      </Typography>
                    </Tooltip>
                  </Box>
                  <Box sx={{ display: 'flex', gap: 1 }}>
                    {job.runs.map((run, index) => (
                      <Tooltip
                        key={`${job.job_id}-${run.run_id}-${index}`}
                        title={
                          <Box>
                            <Typography variant="body2">Run ID: {run.run_id}</Typography>
                            <Typography variant="body2">Status: {run.result_state || 'Unknown'}</Typography>
                            <Typography variant="body2">Duration: {formatDuration(run.duration_seconds)}</Typography>
                            <Typography variant="body2">
                              Time: {run.start_time ? new Date(run.start_time).toLocaleString() : '-'}
                            </Typography>
                          </Box>
                        }
                      >
                        <Box
                          sx={{
                            width: 32,
                            height: 32,
                            borderRadius: 1,
                            bgcolor: getStatusColor(run.result_state),
                            cursor: 'pointer',
                            transition: 'transform 0.2s',
                            '&:hover': {
                              transform: 'scale(1.2)',
                              zIndex: 1,
                            },
                          }}
                        />
                      </Tooltip>
                    ))}
                    {/* Empty cells for missing runs */}
                    {Array.from({ length: Math.max(0, runsPerJob - job.runs.length) }).map((_, i) => (
                      <Box
                        key={`empty-${i}`}
                        sx={{
                          width: 32,
                          height: 32,
                          borderRadius: 1,
                          bgcolor: '#2A2A2A',
                          border: '1px dashed #444',
                        }}
                      />
                    ))}
                  </Box>
                </Box>
              ))}
            </Box>
          ) : (
            <Box sx={{ textAlign: 'center', py: 4 }}>
              <Typography color="text.secondary">
                No job data available
              </Typography>
            </Box>
          )}
        </CardContent>
      </Card>
    </Box>
  );
};

export default MatrixView;
