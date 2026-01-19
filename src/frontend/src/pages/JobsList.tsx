import React, { useState, useEffect } from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TablePagination,
  TextField,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  InputAdornment,
  Skeleton,
  Alert,
} from '@mui/material';
import { Search } from '@mui/icons-material';

import { StatusChip } from '../components';
import { getJobRuns } from '../services/api';
import type { JobRun } from '../types';

const JobsList: React.FC = () => {
  const [days, setDays] = useState(7);
  const [status, setStatus] = useState<string>('');
  const [search, setSearch] = useState('');
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(25);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [jobs, setJobs] = useState<JobRun[]>([]);

  useEffect(() => {
    loadJobs();
  }, [days, status]);

  const loadJobs = async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await getJobRuns(days, 500, status || undefined);
      setJobs(res.data);
    } catch (err) {
      console.error('Failed to load jobs:', err);
      setError('Failed to load job runs. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  const filteredJobs = jobs.filter((job) => {
    const searchLower = search.toLowerCase();
    return (
      job.job_name?.toLowerCase().includes(searchLower) ||
      job.job_id.toString().includes(searchLower) ||
      job.creator_user_name?.toLowerCase().includes(searchLower)
    );
  });

  const formatDuration = (seconds: number | null) => {
    if (!seconds) return '-';
    if (seconds < 60) return `${seconds.toFixed(0)}s`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.floor(seconds % 60)}s`;
    return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
  };

  const formatDate = (dateStr: string | null) => {
    if (!dateStr) return '-';
    return new Date(dateStr).toLocaleString();
  };

  return (
    <Box>
      <Typography variant="h4" fontWeight={600} gutterBottom>
        Job Runs
      </Typography>

      {error && (
        <Alert severity="error" sx={{ mb: 3 }}>
          {error}
        </Alert>
      )}

      {/* Filters */}
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
              <InputLabel>Time Range</InputLabel>
              <Select
                value={days}
                label="Time Range"
                onChange={(e) => setDays(Number(e.target.value))}
              >
                <MenuItem value={1}>Last 24 Hours</MenuItem>
                <MenuItem value={7}>Last 7 Days</MenuItem>
                <MenuItem value={14}>Last 14 Days</MenuItem>
                <MenuItem value={30}>Last 30 Days</MenuItem>
              </Select>
            </FormControl>
            <FormControl size="small" sx={{ minWidth: 150 }}>
              <InputLabel>Status</InputLabel>
              <Select
                value={status}
                label="Status"
                onChange={(e) => setStatus(e.target.value)}
              >
                <MenuItem value="">All</MenuItem>
                <MenuItem value="SUCCESS">Success</MenuItem>
                <MenuItem value="FAILED">Failed</MenuItem>
                <MenuItem value="RUNNING">Running</MenuItem>
              </Select>
            </FormControl>
          </Box>
        </CardContent>
      </Card>

      {/* Jobs Table */}
      <Card>
        <TableContainer>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell>Job ID</TableCell>
                <TableCell>Job Name</TableCell>
                <TableCell>Run ID</TableCell>
                <TableCell>Status</TableCell>
                <TableCell>Type</TableCell>
                <TableCell>Start Time</TableCell>
                <TableCell>Duration</TableCell>
                <TableCell>Creator</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {loading ? (
                Array.from({ length: 10 }).map((_, i) => (
                  <TableRow key={i}>
                    {Array.from({ length: 8 }).map((_, j) => (
                      <TableCell key={j}>
                        <Skeleton variant="text" />
                      </TableCell>
                    ))}
                  </TableRow>
                ))
              ) : (
                filteredJobs
                  .slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage)
                  .map((job) => (
                    <TableRow key={`${job.job_id}-${job.run_id}`} hover>
                      <TableCell>{job.job_id}</TableCell>
                      <TableCell>
                        <Typography variant="body2" noWrap sx={{ maxWidth: 200 }}>
                          {job.job_name || '-'}
                        </Typography>
                      </TableCell>
                      <TableCell>{job.run_id}</TableCell>
                      <TableCell>
                        <StatusChip status={job.result_state} />
                      </TableCell>
                      <TableCell>{job.run_type || '-'}</TableCell>
                      <TableCell>{formatDate(job.start_time)}</TableCell>
                      <TableCell>{formatDuration(job.execution_duration)}</TableCell>
                      <TableCell>
                        <Typography variant="body2" noWrap sx={{ maxWidth: 150 }}>
                          {job.creator_user_name || '-'}
                        </Typography>
                      </TableCell>
                    </TableRow>
                  ))
              )}
            </TableBody>
          </Table>
        </TableContainer>
        <TablePagination
          component="div"
          count={filteredJobs.length}
          page={page}
          onPageChange={(_, newPage) => setPage(newPage)}
          rowsPerPage={rowsPerPage}
          onRowsPerPageChange={(e) => {
            setRowsPerPage(parseInt(e.target.value, 10));
            setPage(0);
          }}
          rowsPerPageOptions={[10, 25, 50, 100]}
        />
      </Card>
    </Box>
  );
};

export default JobsList;
