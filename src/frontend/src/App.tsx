import React, { useState, useEffect } from 'react';
import { Routes, Route, useNavigate, useLocation } from 'react-router-dom';
import {
  Box,
  Drawer,
  AppBar,
  Toolbar,
  Typography,
  List,
  ListItem,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  IconButton,
  Chip,
  CircularProgress,
  Divider,
  Avatar,
  Tooltip,
} from '@mui/material';
import {
  Dashboard,
  WorkHistory,
  AttachMoney,
  HealthAndSafety,
  GridView,
  Timeline,
  SmartToy,
  LocalOffer,
  Settings,
  Speed,
  Menu as MenuIcon,
  Bolt,
} from '@mui/icons-material';
import { motion, AnimatePresence } from 'framer-motion';

// Pages
import DashboardPage from './pages/Dashboard';
import JobsListPage from './pages/JobsList';
import CostAnalyticsPage from './pages/CostAnalytics';
import HealthPage from './pages/Health';
import MatrixViewPage from './pages/MatrixView';
import GanttViewPage from './pages/GanttView';
import AIAssistantPage from './pages/AIAssistant';
import TagCorrelationPage from './pages/TagCorrelation';
import SettingsPage from './pages/Settings';

// Components
import { DataAccessError } from './components';

// API
import { getAuthStatus, checkDataAccess } from './services/api';
import type { DataAccessStatus, User } from './types';

const DRAWER_WIDTH = 260;

const navItems = [
  { path: '/', label: 'Dashboard', icon: <Dashboard /> },
  { path: '/jobs', label: 'Jobs List', icon: <WorkHistory /> },
  { path: '/costs', label: 'Cost Analytics', icon: <AttachMoney /> },
  { path: '/health', label: 'Health Monitor', icon: <HealthAndSafety /> },
  { path: '/matrix', label: 'Matrix View', icon: <GridView /> },
  { path: '/gantt', label: 'Gantt View', icon: <Timeline /> },
  { path: '/tags', label: 'Tag Correlation', icon: <LocalOffer /> },
  { path: '/ai', label: 'AI Assistant', icon: <SmartToy /> },
  { path: '/settings', label: 'Settings', icon: <Settings /> },
];

const App: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const [mobileOpen, setMobileOpen] = useState(false);
  const [user, setUser] = useState<User | null>(null);
  const [authLoading, setAuthLoading] = useState(true);
  const [dataAccess, setDataAccess] = useState<DataAccessStatus | null>(null);
  const [dataAccessLoading, setDataAccessLoading] = useState(true);
  const [dataSource, setDataSource] = useState<string>('checking...');

  useEffect(() => {
    // Check auth status
    getAuthStatus()
      .then((res) => {
        setUser(res.data.user);
      })
      .catch(console.error)
      .finally(() => setAuthLoading(false));

    // Check data access
    checkDataAccessStatus();
  }, []);

  const checkDataAccessStatus = async () => {
    setDataAccessLoading(true);
    try {
      const res = await checkDataAccess();
      setDataAccess(res.data);
      setDataSource(res.data.data_source || 'warehouse');
    } catch (error: unknown) {
      // Handle 403 or 500 errors
      const axiosError = error as { response?: { data?: DataAccessStatus } };
      if (axiosError.response?.data) {
        setDataAccess(axiosError.response.data);
      } else {
        setDataAccess({
          accessible: false,
          error_code: 'CONNECTION_ERROR',
          message: 'Failed to connect to the backend',
          resolution: 'Check if the app is running correctly',
          tables_affected: [],
        });
      }
    } finally {
      setDataAccessLoading(false);
    }
  };

  const handleDrawerToggle = () => {
    setMobileOpen(!mobileOpen);
  };

  const getAuthMethodColor = () => {
    switch (user?.source) {
      case 'OBO': return 'success';
      case 'U2M': return 'primary';
      case 'Cookie': return 'info';
      case 'M2M': return 'warning';
      default: return 'default';
    }
  };

  const drawer = (
    <Box sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      <Toolbar sx={{ px: 2 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <Bolt sx={{ color: 'primary.main', fontSize: 28 }} />
          <Typography variant="h6" noWrap sx={{ fontWeight: 700 }}>
            Job Platform
          </Typography>
        </Box>
      </Toolbar>
      <Divider />

      {/* Data Source Indicator */}
      <Box sx={{ px: 2, py: 1 }}>
        <Tooltip title={dataSource === 'lakebase' ? 'Real-time data via Lakebase' : 'Data via SQL Warehouse'}>
          <Chip
            icon={<Speed />}
            label={dataSource === 'lakebase' ? 'Lakebase' : 'SQL Warehouse'}
            color={dataSource === 'lakebase' ? 'success' : 'default'}
            size="small"
            sx={{ width: '100%' }}
          />
        </Tooltip>
      </Box>

      <List sx={{ flex: 1 }}>
        {navItems.map((item) => (
          <ListItem key={item.path} disablePadding>
            <ListItemButton
              selected={location.pathname === item.path}
              onClick={() => {
                navigate(item.path);
                setMobileOpen(false);
              }}
              sx={{
                mx: 1,
                borderRadius: 2,
                '&.Mui-selected': {
                  backgroundColor: 'primary.main',
                  '&:hover': {
                    backgroundColor: 'primary.dark',
                  },
                  '& .MuiListItemIcon-root': {
                    color: 'white',
                  },
                },
              }}
            >
              <ListItemIcon sx={{ minWidth: 40 }}>
                {item.icon}
              </ListItemIcon>
              <ListItemText primary={item.label} />
            </ListItemButton>
          </ListItem>
        ))}
      </List>

      <Divider />

      {/* User Info */}
      {user && (
        <Box sx={{ p: 2 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
            <Avatar sx={{ width: 32, height: 32, bgcolor: 'primary.main' }}>
              {user.email?.[0]?.toUpperCase() || '?'}
            </Avatar>
            <Box sx={{ overflow: 'hidden' }}>
              <Typography variant="body2" noWrap>
                {user.name || user.email}
              </Typography>
              <Chip
                label={user.source}
                color={getAuthMethodColor()}
                size="small"
                sx={{ height: 18, fontSize: '0.7rem' }}
              />
            </Box>
          </Box>
        </Box>
      )}
    </Box>
  );

  // Show loading state
  if (authLoading || dataAccessLoading) {
    return (
      <Box
        sx={{
          display: 'flex',
          justifyContent: 'center',
          alignItems: 'center',
          height: '100vh',
          flexDirection: 'column',
          gap: 2,
        }}
      >
        <CircularProgress size={48} />
        <Typography variant="body1" color="text.secondary">
          Initializing Unified Job Platform...
        </Typography>
      </Box>
    );
  }

  // Show data access error
  if (dataAccess && !dataAccess.accessible) {
    return <DataAccessError status={dataAccess} onRetry={checkDataAccessStatus} />;
  }

  return (
    <Box sx={{ display: 'flex' }}>
      <AppBar
        position="fixed"
        sx={{
          width: { sm: `calc(100% - ${DRAWER_WIDTH}px)` },
          ml: { sm: `${DRAWER_WIDTH}px` },
          bgcolor: 'background.paper',
          borderBottom: '1px solid',
          borderColor: 'divider',
        }}
        elevation={0}
      >
        <Toolbar>
          <IconButton
            color="inherit"
            edge="start"
            onClick={handleDrawerToggle}
            sx={{ mr: 2, display: { sm: 'none' } }}
          >
            <MenuIcon />
          </IconButton>
          <Typography variant="h6" noWrap component="div" sx={{ flexGrow: 1 }}>
            {navItems.find((item) => item.path === location.pathname)?.label || 'Dashboard'}
          </Typography>
        </Toolbar>
      </AppBar>

      <Box
        component="nav"
        sx={{ width: { sm: DRAWER_WIDTH }, flexShrink: { sm: 0 } }}
      >
        <Drawer
          variant="temporary"
          open={mobileOpen}
          onClose={handleDrawerToggle}
          ModalProps={{ keepMounted: true }}
          sx={{
            display: { xs: 'block', sm: 'none' },
            '& .MuiDrawer-paper': {
              boxSizing: 'border-box',
              width: DRAWER_WIDTH,
              bgcolor: 'background.paper',
            },
          }}
        >
          {drawer}
        </Drawer>
        <Drawer
          variant="permanent"
          sx={{
            display: { xs: 'none', sm: 'block' },
            '& .MuiDrawer-paper': {
              boxSizing: 'border-box',
              width: DRAWER_WIDTH,
              bgcolor: 'background.paper',
              borderRight: '1px solid',
              borderColor: 'divider',
            },
          }}
          open
        >
          {drawer}
        </Drawer>
      </Box>

      <Box
        component="main"
        sx={{
          flexGrow: 1,
          p: 3,
          width: { sm: `calc(100% - ${DRAWER_WIDTH}px)` },
          mt: 8,
          minHeight: 'calc(100vh - 64px)',
        }}
      >
        <AnimatePresence mode="wait">
          <motion.div
            key={location.pathname}
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -20 }}
            transition={{ duration: 0.2 }}
          >
            <Routes>
              <Route path="/" element={<DashboardPage />} />
              <Route path="/jobs" element={<JobsListPage />} />
              <Route path="/costs" element={<CostAnalyticsPage />} />
              <Route path="/health" element={<HealthPage />} />
              <Route path="/matrix" element={<MatrixViewPage />} />
              <Route path="/gantt" element={<GanttViewPage />} />
              <Route path="/tags" element={<TagCorrelationPage />} />
              <Route path="/ai" element={<AIAssistantPage />} />
              <Route path="/settings" element={<SettingsPage />} />
            </Routes>
          </motion.div>
        </AnimatePresence>
      </Box>
    </Box>
  );
};

export default App;
