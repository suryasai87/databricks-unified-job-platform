import React from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Alert,
  AlertTitle,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
  Button,
  Chip,
  Divider,
} from '@mui/material';
import {
  ErrorOutline,
  TableChart,
  ContentCopy,
  Refresh,
  AdminPanelSettings,
} from '@mui/icons-material';
import type { DataAccessStatus } from '../types';

interface DataAccessErrorProps {
  status: DataAccessStatus;
  onRetry: () => void;
}

const DataAccessError: React.FC<DataAccessErrorProps> = ({ status, onRetry }) => {
  const [copied, setCopied] = React.useState(false);

  const getPermissionCommands = () => {
    const commands = `# Get the app's service principal client ID
APP_CLIENT_ID=$(databricks apps get unified-job-platform --output json | jq -r '.service_principal_client_id')

# Grant SQL Warehouse access
databricks permissions update sql/warehouses 4b28691c780d9875 --json '{
  "access_control_list": [{
    "service_principal_name": "'$APP_CLIENT_ID'",
    "permission_level": "CAN_USE"
  }]
}'

# Grant Unity Catalog access (run in Databricks SQL)
GRANT USE CATALOG ON CATALOG hls_amer_catalog TO \`$APP_CLIENT_ID\`;
GRANT USE SCHEMA ON SCHEMA hls_amer_catalog.cost_management TO \`$APP_CLIENT_ID\`;
GRANT SELECT ON SCHEMA hls_amer_catalog.cost_management TO \`$APP_CLIENT_ID\`;

# For system tables (run in Databricks SQL)
GRANT SELECT ON TABLE system.lakeflow.job_run_timeline TO \`$APP_CLIENT_ID\`;
GRANT SELECT ON TABLE system.lakeflow.jobs TO \`$APP_CLIENT_ID\`;
GRANT SELECT ON TABLE system.billing.usage TO \`$APP_CLIENT_ID\`;`;

    return commands;
  };

  const handleCopy = () => {
    navigator.clipboard.writeText(getPermissionCommands());
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <Box
      sx={{
        display: 'flex',
        justifyContent: 'center',
        alignItems: 'center',
        minHeight: '80vh',
        p: 3,
      }}
    >
      <Card sx={{ maxWidth: 800, width: '100%' }}>
        <CardContent>
          <Box sx={{ display: 'flex', alignItems: 'center', mb: 3 }}>
            <ErrorOutline color="error" sx={{ fontSize: 48, mr: 2 }} />
            <Box>
              <Typography variant="h4" gutterBottom>
                Data Access Required
              </Typography>
              <Chip
                label={status.error_code || 'PERMISSION_DENIED'}
                color="error"
                size="small"
              />
            </Box>
          </Box>

          <Alert severity="warning" sx={{ mb: 3 }}>
            <AlertTitle>What happened?</AlertTitle>
            {status.message || 'The app does not have access to the required data tables.'}
          </Alert>

          {status.tables_affected && status.tables_affected.length > 0 && (
            <>
              <Typography variant="h6" gutterBottom sx={{ mt: 3 }}>
                Affected Tables
              </Typography>
              <List dense>
                {status.tables_affected.map((table) => (
                  <ListItem key={table}>
                    <ListItemIcon>
                      <TableChart color="error" />
                    </ListItemIcon>
                    <ListItemText
                      primary={table}
                      primaryTypographyProps={{ fontFamily: 'monospace' }}
                    />
                  </ListItem>
                ))}
              </List>
            </>
          )}

          <Divider sx={{ my: 3 }} />

          <Typography variant="h6" gutterBottom>
            <AdminPanelSettings sx={{ mr: 1, verticalAlign: 'middle' }} />
            How to Fix
          </Typography>

          <Typography variant="body2" color="text.secondary" paragraph>
            {status.resolution || 'Grant the required permissions to the app\'s service principal.'}
          </Typography>

          <Alert severity="info" sx={{ mb: 2 }}>
            Run these commands in your terminal or Databricks SQL editor:
          </Alert>

          <Box
            sx={{
              bgcolor: '#1E1E1E',
              p: 2,
              borderRadius: 1,
              fontFamily: 'monospace',
              fontSize: '0.85rem',
              overflow: 'auto',
              maxHeight: 300,
              whiteSpace: 'pre-wrap',
              position: 'relative',
            }}
          >
            <Button
              size="small"
              startIcon={<ContentCopy />}
              onClick={handleCopy}
              sx={{
                position: 'absolute',
                top: 8,
                right: 8,
                bgcolor: 'rgba(255,255,255,0.1)',
              }}
            >
              {copied ? 'Copied!' : 'Copy'}
            </Button>
            <Typography
              component="pre"
              sx={{ m: 0, color: '#E0E0E0', fontSize: 'inherit' }}
            >
              {getPermissionCommands()}
            </Typography>
          </Box>

          <Box sx={{ mt: 3, display: 'flex', gap: 2 }}>
            <Button
              variant="contained"
              color="primary"
              startIcon={<Refresh />}
              onClick={onRetry}
            >
              Retry Connection
            </Button>
            <Button
              variant="outlined"
              href="https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/index.html"
              target="_blank"
            >
              View Documentation
            </Button>
          </Box>
        </CardContent>
      </Card>
    </Box>
  );
};

export default DataAccessError;
