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
  TableSortLabel,
  TextField,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  InputAdornment,
  Skeleton,
  Alert,
  Link,
  Tooltip,
  Autocomplete,
} from '@mui/material';
import { Search, OpenInNew } from '@mui/icons-material';

import { StatusChip } from '../components';
import { getJobRuns, getWorkspaceUrls } from '../services/api';
import type { JobRun } from '../types';

const JobsList: React.FC = () => {
  const [days, setDays] = useState(7);
  const [status, setStatus] = useState<string>('');
  const [search, setSearch] = useState('');
  const [debouncedSearch, setDebouncedSearch] = useState('');
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(25);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [jobs, setJobs] = useState<JobRun[]>([]);
  const [workspaceUrls, setWorkspaceUrls] = useState<Record<string, { name: string; url: string }>>({});
  const [workspaceFilter, setWorkspaceFilter] = useState<string>('');
  const [sortBy, setSortBy] = useState<'start_time' | 'execution_duration'>('start_time');
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc');

  useEffect(() => {
    getWorkspaceUrls().then((res) => {
      setWorkspaceUrls(res.data);
    }).catch(() => {});
  }, []);

  // Debounce search input
  useEffect(() => {
    const timer = setTimeout(() => setDebouncedSearch(search), 400);
    return () => clearTimeout(timer);
  }, [search]);

  useEffect(() => {
    loadJobs();
  }, [days, status, debouncedSearch, workspaceFilter]);

  const loadJobs = async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await getJobRuns(days, 1000, status || undefined, debouncedSearch || undefined, workspaceFilter || undefined);
      // Deduplicate by (job_id, run_id) — MV can produce duplicates across account_ids
      const seen = new Set<string>();
      const unique = res.data.filter((job: JobRun) => {
        const key = `${job.job_id}-${job.run_id}`;
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });
      setJobs(unique);
      setPage(0);
    } catch (err) {
      console.error('Failed to load jobs:', err);
      setJobs([]);
      setError('Failed to load job runs. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  const handleSort = (column: 'start_time' | 'execution_duration') => {
    if (sortBy === column) {
      setSortDir(sortDir === 'asc' ? 'desc' : 'asc');
    } else {
      setSortBy(column);
      setSortDir(column === 'execution_duration' ? 'desc' : 'desc');
    }
  };

  const sortedJobs = [...jobs].sort((a, b) => {
    let cmp = 0;
    if (sortBy === 'start_time') {
      cmp = (a.start_time || '').localeCompare(b.start_time || '');
    } else {
      cmp = (a.execution_duration || 0) - (b.execution_duration || 0);
    }
    return sortDir === 'asc' ? cmp : -cmp;
  });

  const formatDuration = (seconds: number | null) => {
    if (seconds === null || seconds === undefined) return '-';
    if (seconds === 0) return '< 1s';
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
                <MenuItem value={60}>Last 60 Days</MenuItem>
                <MenuItem value={90}>Last 90 Days</MenuItem>
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
                <MenuItem value="SUCCEEDED">Succeeded</MenuItem>
                <MenuItem value="FAILED">Failed</MenuItem>
                <MenuItem value="ERROR">Error</MenuItem>
                <MenuItem value="CANCELLED">Cancelled</MenuItem>
                <MenuItem value="RUNNING">Running</MenuItem>
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
                <TableCell>Workspace ID</TableCell>
                <TableCell>Status</TableCell>
                <TableCell>Type</TableCell>
                <TableCell sortDirection={sortBy === 'start_time' ? sortDir : false}>
                  <TableSortLabel
                    active={sortBy === 'start_time'}
                    direction={sortBy === 'start_time' ? sortDir : 'desc'}
                    onClick={() => handleSort('start_time')}
                  >
                    Start Time
                  </TableSortLabel>
                </TableCell>
                <TableCell sortDirection={sortBy === 'execution_duration' ? sortDir : false}>
                  <TableSortLabel
                    active={sortBy === 'execution_duration'}
                    direction={sortBy === 'execution_duration' ? sortDir : 'desc'}
                    onClick={() => handleSort('execution_duration')}
                  >
                    Duration
                  </TableSortLabel>
                </TableCell>
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
                sortedJobs
                  .slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage)
                  .map((job) => (
                    <TableRow key={`${job.job_id}-${job.run_id}`} hover>
                      <TableCell>
                        {job.workspace_id && workspaceUrls[job.workspace_id] ? (
                          <Link
                            href={`${workspaceUrls[job.workspace_id].url}/jobs/${job.job_id}/tasks?o=${job.workspace_id}`}
                            target="_blank"
                            rel="noopener noreferrer"
                            sx={{ display: 'inline-flex', alignItems: 'center', gap: 0.5 }}
                          >
                            {job.job_id}
                            <OpenInNew sx={{ fontSize: 14 }} />
                          </Link>
                        ) : (
                          job.job_id
                        )}
                      </TableCell>
                      <TableCell>
                        <Tooltip title={job.job_name || '-'} arrow>
                          <Typography variant="body2" noWrap sx={{ maxWidth: 200 }}>
                            {job.job_name || '-'}
                          </Typography>
                        </Tooltip>
                      </TableCell>
                      <TableCell>{job.run_id}</TableCell>
                      <TableCell>{job.workspace_id || '-'}</TableCell>
                      <TableCell>
                        <StatusChip status={job.result_state} />
                      </TableCell>
                      <TableCell>{job.run_type || '-'}</TableCell>
                      <TableCell>{formatDate(job.start_time)}</TableCell>
                      <TableCell>{formatDuration(job.execution_duration)}</TableCell>
                    </TableRow>
                  ))
              )}
            </TableBody>
          </Table>
        </TableContainer>
        <TablePagination
          component="div"
          count={sortedJobs.length}
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
