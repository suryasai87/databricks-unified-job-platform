import React, { useState, useEffect, useMemo } from 'react';
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
  Divider,
} from '@mui/material';
import {
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip as RechartsTooltip,
  ResponsiveContainer,
  LineChart,
  Line,
} from 'recharts';

import { getJobRuns } from '../services/api';
import type { JobRun } from '../types';

/** Match backend / warehouse row cap so dense windows still populate the timeline. */
const GANTT_JOB_RUN_LIMIT = 10000;

interface GroupedJob {
  jobId: number;
  jobName: string;
  runs: {
    runId: number;
    startTime: Date;
    endTime: Date;
    duration: number;
    resultState: string | null;
  }[];
}

/** Parse timestamps from warehouse / Lakebase (ISO, Spark strings, epoch, API cell shapes). */
function parseJobTimeMs(value: string | number | unknown[] | Record<string, unknown> | null | undefined): number | null {
  if (value == null || value === '') return null;
  if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
    const inner = (value as { value?: unknown }).value;
    if (inner !== undefined) return parseJobTimeMs(inner as string | number | null);
    return null;
  }
  if (Array.isArray(value) && value.length > 0) {
    const nums = value.map((x) => Number(x)).filter((x) => Number.isFinite(x));
    if (nums.length === 1) return normalizeEpochMs(nums[0]);
    if (nums.length >= 3) {
      const [y, mo, d, h = 0, mi = 0, sec = 0] = nums;
      const frac = nums.length > 6 ? nums[6] / 1e6 : 0;
      const t = Date.UTC(y, mo - 1, d, h, mi, sec, frac);
      return Number.isNaN(t) ? null : t;
    }
    return null;
  }
  if (typeof value === 'number' && Number.isFinite(value)) {
    return normalizeEpochMs(value);
  }
  const s = String(value).trim();
  if (/^-?\d+(\.\d+)?([eE][+-]?\d+)?$/.test(s)) {
    const n = Number(s);
    return Number.isFinite(n) ? normalizeEpochMs(n) : null;
  }
  // Normalize to ISO 8601 that Date.parse accepts reliably.
  const spaced = s.includes('T') ? s : s.replace(' ', 'T');
  let normalized: string;
  if (/[Zz]$/.test(spaced)) {
    normalized = spaced;                                          // already Z
  } else if (/[+-]\d{2}:\d{2}$/.test(spaced)) {
    normalized = spaced;                                          // ±HH:MM — valid
  } else if (/[+-]\d{4}$/.test(spaced)) {
    normalized = `${spaced.slice(0, -2)}:${spaced.slice(-2)}`;   // ±HHMM → ±HH:MM
  } else if (/[+-]\d{2}$/.test(spaced)) {
    normalized = `${spaced}:00`;                                  // ±HH (Postgres UTC "+00") → ±HH:00
  } else {
    normalized = `${spaced}Z`;                                    // no tz (Databricks) → UTC
  }
  const ms = Date.parse(normalized);
  return Number.isNaN(ms) ? null : ms;
}

function normalizeEpochMs(n: number): number {
  const x = Math.trunc(n);
  if (x >= 1e15) return Math.round(x / 1000);
  if (x >= 1e12) return x;
  if (x >= 1e9) return x * 1000;
  return x;
}

const GanttView: React.FC = () => {
  const [hours, setHours] = useState(24);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [jobs, setJobs] = useState<JobRun[]>([]);

  useEffect(() => {
    loadData();
  }, [hours]);

  const loadData = async () => {
    setLoading(true);
    setError(null);
    try {
      const days = Math.max(7, Math.ceil(hours / 24));
      const jobsRes = await getJobRuns(days, GANTT_JOB_RUN_LIMIT, undefined, undefined, undefined, hours);
      setJobs(jobsRes.data);
    } catch (err) {
      console.error('Failed to load gantt data:', err);
      setError('Failed to load timeline data. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  const getStatusColor = (status: string | null) => {
    switch (status?.toUpperCase()) {
      case 'SUCCEEDED':
        return '#4CAF50';
      case 'FAILED':
      case 'ERROR':
        return '#F44336';
      case 'RUNNING':
        return '#FF9800';
      case 'CANCELLED':
        return '#9E9E9E';
      default:
        return '#607D8B';
    }
  };

  const { groupedJobs, minTime, maxTime } = useMemo(() => {
    const maxT = Date.now();
    const minT = maxT - hours * 3600000;
    const jobMap = new Map<number, GroupedJob>();

    jobs.forEach((job) => {
      const startMs = parseJobTimeMs(job.start_time as string | number | null);
      if (startMs == null) return;
      let endMs = parseJobTimeMs(job.end_time as string | number | null);
      if (endMs == null) endMs = maxT;
      if (endMs < startMs) endMs = startMs + 1000;
      if (endMs < minT || startMs > maxT) return;

      const startTime = new Date(startMs);
      const endTime = new Date(endMs);
      const run = {
        runId: job.run_id,
        startTime,
        endTime,
        duration: (endTime.getTime() - startTime.getTime()) / 1000,
        resultState: job.result_state,
      };

      if (jobMap.has(job.job_id)) {
        jobMap.get(job.job_id)!.runs.push(run);
      } else {
        jobMap.set(job.job_id, {
          jobId: job.job_id,
          jobName: job.job_name || `Job ${job.job_id}`,
          runs: [run],
        });
      }
    });

    const grouped = Array.from(jobMap.values())
      .map((g) => {
        g.runs.sort((a, b) => a.startTime.getTime() - b.startTime.getTime());
        return g;
      })
      .sort((a, b) => {
        const aMin = Math.min(...a.runs.map((r) => Math.max(r.startTime.getTime(), minT)));
        const bMin = Math.min(...b.runs.map((r) => Math.max(r.startTime.getTime(), minT)));
        return aMin - bMin;
      })
      .slice(0, 200);

    return { groupedJobs: grouped, minTime: minT, maxTime: maxT };
  }, [jobs, hours]);

  const allRuns = groupedJobs.flatMap((g) => g.runs);
  const timeRange = Math.max(maxTime - minTime, 1);

  const formatDuration = (seconds: number) => {
    if (seconds < 60) return `${seconds.toFixed(0)}s`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.floor(seconds % 60)}s`;
    return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
  };

  const concurrentChart = useMemo(() => {
    const windowEnd = Date.now();
    const windowStart = windowEnd - hours * 3600000;

    // Collect parsed runs that overlap the window
    const parsedRuns: { startMs: number; endMs: number }[] = [];
    for (const job of jobs) {
      const startMs = parseJobTimeMs(job.start_time as string | number | null);
      if (startMs == null) continue;
      let endMs = parseJobTimeMs(job.end_time as string | number | null);
      if (endMs == null) endMs = windowEnd; // still running
      if (endMs < windowStart || startMs > windowEnd) continue;
      parsedRuns.push({ startMs, endMs });
    }

    // Sample at fixed intervals — same cadence as backend bucketing
    const stepMs =
      hours <= 6 ? 5 * 60000 :
      hours <= 12 ? 10 * 60000 :
      hours <= 24 ? 15 * 60000 :
      hours <= 48 ? 30 * 60000 :
      60 * 60000;

    const rows: { timeMs: number; concurrent_jobs: number }[] = [];
    for (let t = windowStart; t <= windowEnd + stepMs / 2; t += stepMs) {
      const count = parsedRuns.filter((r) => r.startMs <= t && r.endMs >= t).length;
      rows.push({ timeMs: t, concurrent_jobs: count });
    }

    // x-axis ticks
    const tickStepMs =
      hours <= 6 ? 30 * 60000 :
      hours <= 24 ? 2 * 3600000 :
      hours <= 72 ? 6 * 3600000 :
      24 * 3600000;

    const xTicks: number[] = [];
    const firstTick = Math.ceil(windowStart / tickStepMs) * tickStepMs;
    for (let t = firstTick; t <= windowEnd + 1e-6; t += tickStepMs) {
      xTicks.push(t);
    }
    if (xTicks.length === 0) xTicks.push(windowStart, windowEnd);

    return { rows, xTicks, windowStart, windowEnd };
  }, [jobs, hours]);

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
              {groupedJobs.length} jobs, {allRuns.length} runs
            </Typography>
          </Box>
        </CardContent>
      </Card>

      {/* Legend */}
      <Box sx={{ display: 'flex', gap: 3, mb: 3 }}>
        {[
          { label: 'Succeeded', color: '#4CAF50' },
          { label: 'Failed/Error', color: '#F44336' },
          { label: 'Running', color: '#FF9800' },
          { label: 'Cancelled', color: '#9E9E9E' },
        ].map((item) => (
          <Box key={item.label} sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <Box sx={{ width: 16, height: 16, borderRadius: 1, bgcolor: item.color }} />
            <Typography variant="body2">{item.label}</Typography>
          </Box>
        ))}
      </Box>

      {/* Combined chart card */}
      <Card>
        <CardContent>
          {/* Concurrent Runs */}
          <Typography variant="h6" gutterBottom>
            Concurrent Runs Over Time
          </Typography>
          {loading ? (
            <Skeleton variant="rectangular" height={200} />
          ) : (
            <ResponsiveContainer width="100%" height={260}>
              <LineChart data={concurrentChart.rows} margin={{ top: 8, right: 16, left: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                <XAxis
                  type="number"
                  dataKey="timeMs"
                  domain={[concurrentChart.windowStart, concurrentChart.windowEnd]}
                  scale="time"
                  ticks={concurrentChart.xTicks}
                  stroke="#888"
                  tick={{ fill: '#888', fontSize: 11 }}
                  tickFormatter={(ms) =>
                    hours > 24
                      ? new Date(ms).toLocaleString('en-US', {
                          month: 'short',
                          day: 'numeric',
                          hour: '2-digit',
                          minute: '2-digit',
                        })
                      : new Date(ms).toLocaleTimeString('en-US', {
                          hour: '2-digit',
                          minute: '2-digit',
                        })
                  }
                />
                <YAxis
                  stroke="#888"
                  tick={{ fill: '#888' }}
                  allowDecimals={false}
                  label={{ value: 'Concurrent runs', angle: -90, position: 'insideLeft', fill: '#888', fontSize: 11 }}
                />
                <RechartsTooltip
                  contentStyle={{ backgroundColor: '#1A1A1A', border: '1px solid #333' }}
                  labelFormatter={(ms) => new Date(Number(ms)).toLocaleString()}
                  formatter={(value: number) => [value, 'Concurrent runs']}
                />
                <Line
                  type="linear"
                  dataKey="concurrent_jobs"
                  stroke="#FF6F00"
                  strokeWidth={2}
                  dot={false}
                  name="Concurrent runs"
                />
              </LineChart>
            </ResponsiveContainer>
          )}

          <Divider sx={{ my: 2 }} />

          {/* Job Timeline */}
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
          ) : groupedJobs.length > 0 ? (
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
                        {hours > 24
                          ? time.toLocaleString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
                          : time.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' })}
                      </Typography>
                    </Box>
                  );
                })}
              </Box>

              {/* Job rows — one row per job, multiple run bars */}
              {groupedJobs.map((group) => (
                <Box
                  key={group.jobId}
                  sx={{
                    display: 'flex',
                    alignItems: 'center',
                    mb: 0.5,
                    minWidth: 0,
                    '&:hover': { bgcolor: 'action.hover' },
                    borderRadius: 1,
                    p: 0.5,
                  }}
                >
                  <Tooltip title={group.jobName} arrow>
                    <Typography
                      variant="body2"
                      noWrap
                      sx={{ width: 160, flexShrink: 0, pr: 1 }}
                    >
                      {group.jobName}
                    </Typography>
                  </Tooltip>
                  <Box
                    sx={{
                      flex: '1 1 0%',
                      minWidth: 0,
                      position: 'relative',
                      height: 22,
                      bgcolor: '#2A2A2A',
                      borderRadius: 1,
                      overflow: 'hidden',
                    }}
                  >
                    {group.runs.flatMap((run) => {
                      const runStart = run.startTime.getTime();
                      const runEnd = run.endTime.getTime();
                      const visStart = Math.max(runStart, minTime);
                      const visEnd = Math.min(runEnd, maxTime);
                      if (visEnd <= visStart) return [];
                      const leftPct = ((visStart - minTime) / timeRange) * 100;
                      const rawWidthPct = ((visEnd - visStart) / timeRange) * 100;
                      const widthPct = Math.min(
                        Math.max(rawWidthPct, 0.35),
                        Math.max(0, 100 - leftPct),
                      );

                      return [
                        <Tooltip
                          key={run.runId}
                          title={
                            <Box>
                              <Typography variant="caption" display="block">Run {run.runId}</Typography>
                              <Typography variant="caption" display="block">Status: {run.resultState || 'RUNNING'}</Typography>
                              <Typography variant="caption" display="block">Duration: {formatDuration(run.duration)}</Typography>
                              <Typography variant="caption" display="block">
                                {run.startTime.toLocaleString()} — {run.endTime.toLocaleString()}
                              </Typography>
                            </Box>
                          }
                          arrow
                        >
                          <Box
                            sx={{
                              position: 'absolute',
                              left: `${leftPct}%`,
                              width: `${widthPct}%`,
                              height: '100%',
                              bgcolor: getStatusColor(run.resultState),
                              borderRadius: 1,
                              cursor: 'pointer',
                              '&:hover': { opacity: 0.8 },
                            }}
                          />
                        </Tooltip>,
                      ];
                    })}
                  </Box>
                  <Box sx={{ width: 50, pl: 1, flexShrink: 0 }}>
                    <Typography variant="caption" color="text.secondary">
                      {group.runs.length}x
                    </Typography>
                  </Box>
                </Box>
              ))}
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
