import React, { useState, useEffect } from 'react';
import {
  Box,
  Card,
  CardContent,
  CardHeader,
  Typography,
  Button,
  Alert,
  Chip,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
} from '@mui/material';
import { Refresh, Speed, Storage } from '@mui/icons-material';

import { getPerformanceStats, getAuthStatus } from '../services/api';
import type { PerformanceStats, AuthStatus } from '../types';

const Settings: React.FC = () => {
  const [loading, setLoading] = useState(false);
  const [authStatus, setAuthStatus] = useState<AuthStatus | null>(null);
  const [performanceStats, setPerformanceStats] = useState<PerformanceStats | null>(null);

  useEffect(() => {
    loadStatus();
  }, []);

  const loadStatus = async () => {
    setLoading(true);
    try {
      const [authRes, perfRes] = await Promise.all([
        getAuthStatus(),
        getPerformanceStats(),
      ]);
      setAuthStatus(authRes.data);
      setPerformanceStats(perfRes.data);
    } catch (err) {
      console.error('Failed to load status:', err);
    } finally {
      setLoading(false);
    }
  };

  const handleRefreshPerformance = async () => {
    setLoading(true);
    try {
      const res = await getPerformanceStats();
      setPerformanceStats(res.data);
    } catch (err) {
      console.error('Failed to refresh performance stats:', err);
    } finally {
      setLoading(false);
    }
  };

  return (
    <Box>
      <Typography variant="h4" fontWeight={600} gutterBottom>
        Settings
      </Typography>

      {/* Authentication Status */}
      <Card sx={{ mb: 3 }}>
        <CardHeader
          title="Authentication"
          subheader="Current authentication status and method"
        />
        <CardContent>
          {authStatus ? (
            <Box>
              <Box sx={{ display: 'flex', gap: 2, alignItems: 'center', mb: 2 }}>
                <Chip
                  label={authStatus.authenticated ? 'Authenticated' : 'Not Authenticated'}
                  color={authStatus.authenticated ? 'success' : 'error'}
                />
                <Chip
                  label={`Method: ${authStatus.auth_method}`}
                  variant="outlined"
                />
              </Box>
              {authStatus.user && (
                <TableContainer>
                  <Table size="small">
                    <TableBody>
                      <TableRow>
                        <TableCell>Email</TableCell>
                        <TableCell>{authStatus.user.email}</TableCell>
                      </TableRow>
                      <TableRow>
                        <TableCell>Name</TableCell>
                        <TableCell>{authStatus.user.name || '-'}</TableCell>
                      </TableRow>
                      <TableRow>
                        <TableCell>Auth Source</TableCell>
                        <TableCell>{authStatus.user.source}</TableCell>
                      </TableRow>
                    </TableBody>
                  </Table>
                </TableContainer>
              )}
            </Box>
          ) : (
            <Typography color="text.secondary">Loading...</Typography>
          )}
        </CardContent>
      </Card>

      {/* Data Source Performance */}
      <Card sx={{ mb: 3 }}>
        <CardHeader
          title="Data Source Performance"
          subheader="Compare Lakebase vs SQL Warehouse query performance"
          action={
            <Button
              startIcon={<Refresh />}
              onClick={handleRefreshPerformance}
              disabled={loading}
            >
              Refresh
            </Button>
          }
        />
        <CardContent>
          {performanceStats ? (
            <Box>
              <TableContainer>
                <Table>
                  <TableHead>
                    <TableRow>
                      <TableCell>Data Source</TableCell>
                      <TableCell align="right">Query Time</TableCell>
                      <TableCell>Status</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    <TableRow>
                      <TableCell>
                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                          <Speed color="primary" />
                          Lakebase (PostgreSQL)
                        </Box>
                      </TableCell>
                      <TableCell align="right">
                        {performanceStats.lakebase?.status === 'success'
                          ? `${performanceStats.lakebase.execution_time_ms.toFixed(0)} ms`
                          : '-'}
                      </TableCell>
                      <TableCell>
                        <Chip
                          label={performanceStats.lakebase?.status || 'N/A'}
                          color={performanceStats.lakebase?.status === 'success' ? 'success' : 'default'}
                          size="small"
                        />
                      </TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell>
                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                          <Storage />
                          SQL Warehouse
                        </Box>
                      </TableCell>
                      <TableCell align="right">
                        {performanceStats.warehouse?.status === 'success'
                          ? `${performanceStats.warehouse.execution_time_ms.toFixed(0)} ms`
                          : '-'}
                      </TableCell>
                      <TableCell>
                        <Chip
                          label={performanceStats.warehouse?.status || 'N/A'}
                          color={performanceStats.warehouse?.status === 'success' ? 'success' : 'default'}
                          size="small"
                        />
                      </TableCell>
                    </TableRow>
                  </TableBody>
                </Table>
              </TableContainer>

              {performanceStats.speedup_factor && (
                <Alert severity="success" sx={{ mt: 2 }}>
                  Lakebase is <strong>{performanceStats.speedup_factor}x faster</strong> than SQL Warehouse for this query.
                </Alert>
              )}
            </Box>
          ) : (
            <Typography color="text.secondary">Loading performance data...</Typography>
          )}
        </CardContent>
      </Card>

      {/* Configuration Reference */}
      <Card>
        <CardHeader
          title="Configuration Reference"
          subheader="Environment variables and configuration settings"
        />
        <CardContent>
          <TableContainer>
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>Variable</TableCell>
                  <TableCell>Description</TableCell>
                  <TableCell>Default</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                <TableRow>
                  <TableCell><code>DATABRICKS_HOST</code></TableCell>
                  <TableCell>Databricks workspace URL</TableCell>
                  <TableCell>fe-vm-hls-amer.cloud.databricks.com</TableCell>
                </TableRow>
                <TableRow>
                  <TableCell><code>CATALOG</code></TableCell>
                  <TableCell>Unity Catalog name</TableCell>
                  <TableCell>hls_amer_catalog</TableCell>
                </TableRow>
                <TableRow>
                  <TableCell><code>SCHEMA</code></TableCell>
                  <TableCell>Schema for cost management tables</TableCell>
                  <TableCell>cost_management</TableCell>
                </TableRow>
                <TableRow>
                  <TableCell><code>WAREHOUSE_ID</code></TableCell>
                  <TableCell>SQL Warehouse ID for queries</TableCell>
                  <TableCell>4b28691c780d9875</TableCell>
                </TableRow>
                <TableRow>
                  <TableCell><code>LAKEBASE_INSTANCE_ID</code></TableCell>
                  <TableCell>Lakebase instance ID for real-time queries</TableCell>
                  <TableCell>6b59171b-cee8-4acc-9209-6c848ffbfbfe</TableCell>
                </TableRow>
                <TableRow>
                  <TableCell><code>LAKEBASE_ENABLED</code></TableCell>
                  <TableCell>Enable/disable Lakebase integration</TableCell>
                  <TableCell>true</TableCell>
                </TableRow>
                <TableRow>
                  <TableCell><code>CACHE_TTL</code></TableCell>
                  <TableCell>Cache time-to-live in seconds</TableCell>
                  <TableCell>300</TableCell>
                </TableRow>
              </TableBody>
            </Table>
          </TableContainer>
        </CardContent>
      </Card>
    </Box>
  );
};

export default Settings;
