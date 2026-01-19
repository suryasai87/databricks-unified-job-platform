import React from 'react';
import { Chip, ChipProps } from '@mui/material';
import {
  CheckCircle,
  Error,
  HourglassEmpty,
  Cancel,
  Help,
} from '@mui/icons-material';

interface StatusChipProps {
  status: string | null;
  size?: ChipProps['size'];
}

const StatusChip: React.FC<StatusChipProps> = ({ status, size = 'small' }) => {
  const getStatusConfig = (status: string | null) => {
    switch (status?.toUpperCase()) {
      case 'SUCCESS':
      case 'SUCCEEDED':
      case 'COMPLETED':
      case 'HEALTHY':
      case 'COMPLIANT':
        return {
          color: 'success' as const,
          icon: <CheckCircle />,
          label: status,
        };
      case 'FAILED':
      case 'ERROR':
      case 'CRITICAL':
      case 'NON_COMPLIANT':
        return {
          color: 'error' as const,
          icon: <Error />,
          label: status,
        };
      case 'RUNNING':
      case 'PENDING':
      case 'IN_PROGRESS':
        return {
          color: 'warning' as const,
          icon: <HourglassEmpty />,
          label: status,
        };
      case 'CANCELLED':
      case 'SKIPPED':
      case 'TERMINATED':
        return {
          color: 'default' as const,
          icon: <Cancel />,
          label: status,
        };
      case 'WARNING':
      case 'AT_RISK':
        return {
          color: 'warning' as const,
          icon: <Error />,
          label: status,
        };
      default:
        return {
          color: 'default' as const,
          icon: <Help />,
          label: status || 'Unknown',
        };
    }
  };

  const config = getStatusConfig(status);

  return (
    <Chip
      icon={config.icon}
      label={config.label}
      color={config.color}
      size={size}
      variant="outlined"
    />
  );
};

export default StatusChip;
