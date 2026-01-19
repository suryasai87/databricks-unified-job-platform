import React from 'react';
import { Card, CardContent, Typography, Box, Skeleton } from '@mui/material';
import { TrendingUp, TrendingDown, TrendingFlat } from '@mui/icons-material';

interface MetricCardProps {
  title: string;
  value: string | number;
  subtitle?: string;
  trend?: number;
  loading?: boolean;
  icon?: React.ReactNode;
  color?: 'primary' | 'secondary' | 'success' | 'warning' | 'error';
}

const MetricCard: React.FC<MetricCardProps> = ({
  title,
  value,
  subtitle,
  trend,
  loading = false,
  icon,
  color = 'primary',
}) => {
  const getTrendIcon = () => {
    if (trend === undefined) return null;
    if (trend > 0) return <TrendingUp color="success" />;
    if (trend < 0) return <TrendingDown color="error" />;
    return <TrendingFlat color="disabled" />;
  };

  const getTrendColor = () => {
    if (trend === undefined) return 'text.secondary';
    if (trend > 0) return 'success.main';
    if (trend < 0) return 'error.main';
    return 'text.secondary';
  };

  if (loading) {
    return (
      <Card>
        <CardContent>
          <Skeleton variant="text" width="60%" height={24} />
          <Skeleton variant="text" width="80%" height={48} />
          <Skeleton variant="text" width="40%" height={20} />
        </CardContent>
      </Card>
    );
  }

  return (
    <Card
      sx={{
        height: '100%',
        transition: 'transform 0.2s, box-shadow 0.2s',
        '&:hover': {
          transform: 'translateY(-4px)',
          boxShadow: (theme) => `0 8px 24px ${theme.palette[color].main}20`,
        },
      }}
    >
      <CardContent>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
          <Typography variant="body2" color="text.secondary" gutterBottom>
            {title}
          </Typography>
          {icon && (
            <Box sx={{ color: `${color}.main`, opacity: 0.8 }}>
              {icon}
            </Box>
          )}
        </Box>
        <Typography
          variant="h4"
          component="div"
          sx={{ color: `${color}.main`, fontWeight: 600, my: 1 }}
        >
          {value}
        </Typography>
        {(subtitle || trend !== undefined) && (
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            {trend !== undefined && (
              <>
                {getTrendIcon()}
                <Typography variant="body2" sx={{ color: getTrendColor() }}>
                  {trend > 0 ? '+' : ''}{trend}%
                </Typography>
              </>
            )}
            {subtitle && (
              <Typography variant="body2" color="text.secondary">
                {subtitle}
              </Typography>
            )}
          </Box>
        )}
      </CardContent>
    </Card>
  );
};

export default MetricCard;
