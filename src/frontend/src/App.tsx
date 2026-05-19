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
  HealthAndSafety,
  Timeline,
  SmartToy,
  Speed,
  Menu as MenuIcon,
  Bolt,
  AttachMoney,
} from '@mui/icons-material';
import { motion, AnimatePresence } from 'framer-motion';

// Pages
import DashboardPage from './pages/Dashboard';
import JobsListPage from './pages/JobsList';
import HealthPage from './pages/Health';
import GanttViewPage from './pages/GanttView';
import AIAssistantPage from './pages/AIAssistant';
import CostAnalyticsPage from './pages/CostAnalytics';
// API
import { getAuthStatus, getAppHealth } from './services/api';
import type { User } from './types';

const DRAWER_WIDTH = 260;

const navItems = [
  { path: '/', label: 'Dashboard', icon: <Dashboard /> },
  { path: '/jobs', label: 'Jobs List', icon: <WorkHistory /> },
  { path: '/health', label: 'Health Monitor', icon: <HealthAndSafety /> },
  { path: '/gantt', label: 'Gantt View', icon: <Timeline /> },
  { path: '/costs', label: 'Cost Analytics', icon: <AttachMoney /> },
  { path: '/ai', label: 'AI Assistant', icon: <SmartToy /> },
];

const App: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const [mobileOpen, setMobileOpen] = useState(false);
  const [user, setUser] = useState<User | null>(null);
  const [authLoading, setAuthLoading] = useState(true);
  const [dataSource, setDataSource] = useState<string>('warehouse');

  useEffect(() => {
    // Check auth status
    getAuthStatus()
      .then((res) => {
        setUser(res.data.user);
      })
      .catch(console.error)
      .finally(() => setAuthLoading(false));

    // Check data source
    getAppHealth()
      .then((res) => setDataSource(res.data.data_source))
      .catch(() => {});

  }, []);

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
  if (authLoading) {
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
              <Route path="/health" element={<HealthPage />} />
              <Route path="/gantt" element={<GanttViewPage />} />
              <Route path="/costs" element={<CostAnalyticsPage />} />
              <Route path="/ai" element={<AIAssistantPage />} />
            </Routes>
          </motion.div>
        </AnimatePresence>
      </Box>
    </Box>
  );
};

export default App;
