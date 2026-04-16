const express = require('express');
const cors = require('cors');
const path = require('path');
require('dotenv').config({ path: path.resolve(__dirname, '../.env') });
const { initializeDatabase } = require('./db-connection');

const app = express();
const PORT = process.env.BACKEND_PORT || 5000;

// Global database query handler
let db = null;

/**
 * Middleware for request validation
 */
const validatePagination = (req, res, next) => {
  const page = parseInt(req.query.page) || 1;
  const limit = parseInt(req.query.limit) || 20;

  if (page < 1 || limit < 1 || limit > 100) {
    return res.status(400).json({
      error: 'Invalid pagination parameters',
      details: 'page must be >= 1, limit must be between 1 and 100',
    });
  }

  req.pagination = { page, limit };
  next();
};

const validateFilters = (req, res, next) => {
  const filters = {};

  // Parse skills filter
  if (req.query.skills) {
    const skillsStr = req.query.skills.toString();
    filters.skills = skillsStr
      .split(',')
      .map((s) => s.trim())
      .filter((s) => s.length > 0);
  }

  // Parse other filters
  if (req.query.location) {
    filters.location = req.query.location.toString().trim();
  }

  if (req.query.company) {
    filters.company = req.query.company.toString().trim();
  }

  if (req.query.job_type) {
    filters.job_type = req.query.job_type.toString().trim();
  }

  req.filters = filters;
  next();
};

// Middleware
app.use(cors());
app.use(express.json());

// Request logging middleware
app.use((req, res, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.path}`);
  next();
});

/**
 * Health check endpoint
 */
app.get('/api/health', (req, res) => {
  res.json({ status: 'Backend is running' });
});

/**
 * GET /api/jobs - Paginated job listings with filters
 *
 * Query Parameters:
 *   - page: Page number (default: 1)
 *   - limit: Items per page, max 100 (default: 20)
 *   - skills: Comma-separated skill names
 *   - location: Location filter (partial match, case-insensitive)
 *   - company: Company filter (partial match, case-insensitive)
 *   - job_type: Job type filter (exact match)
 *
 * Example: /api/jobs?page=1&limit=10&skills=python,aws&location=New York
 */
app.get('/api/jobs', validatePagination, validateFilters, async (req, res, next) => {
  try {
    if (!db) {
      return res
        .status(503)
        .json({ error: 'Database unavailable', details: 'MongoDB connection failed' });
    }

    const result = await db.getJobs(req.pagination.page, req.pagination.limit, req.filters);
    res.json({
      success: true,
      data: result.jobs,
      pagination: result.pagination,
      filters_applied: req.filters,
    });
  } catch (error) {
    next(error);
  }
});

/**
 * GET /api/insights/top-skills - Ranked skills by frequency
 *
 * Query Parameters:
 *   - limit: Number of top skills to return (default: 10, max: 50)
 *
 * Example: /api/insights/top-skills?limit=15
 */
app.get('/api/insights/top-skills', async (req, res, next) => {
  try {
    if (!db) {
      return res
        .status(503)
        .json({ error: 'Database unavailable', details: 'MongoDB connection failed' });
    }

    let limit = parseInt(req.query.limit) || 10;
    limit = Math.min(Math.max(limit, 1), 50);

    const skills = await db.getTopSkills(limit);
    res.json({
      success: true,
      data: skills,
      count: skills.length,
    });
  } catch (error) {
    next(error);
  }
});

/**
 * GET /api/insights/top-companies - Ranked companies by job count
 *
 * Query Parameters:
 *   - limit: Number of top companies to return (default: 10, max: 50)
 *
 * Example: /api/insights/top-companies?limit=15
 */
app.get('/api/insights/top-companies', async (req, res, next) => {
  try {
    if (!db) {
      return res
        .status(503)
        .json({ error: 'Database unavailable', details: 'MongoDB connection failed' });
    }

    let limit = parseInt(req.query.limit) || 10;
    limit = Math.min(Math.max(limit, 1), 50);

    const companies = await db.getTopCompanies(limit);
    res.json({
      success: true,
      data: companies,
      count: companies.length,
    });
  } catch (error) {
    next(error);
  }
});

/**
 * GET /api/insights/hiring-trends - Jobs posted over time
 *
 * Query Parameters:
 *   - groupBy: Time grouping - 'day', 'week', or 'month' (default: 'day')
 *
 * Example: /api/insights/hiring-trends?groupBy=week
 */
app.get('/api/insights/hiring-trends', async (req, res, next) => {
  try {
    if (!db) {
      return res
        .status(503)
        .json({ error: 'Database unavailable', details: 'MongoDB connection failed' });
    }

    let groupBy = req.query.groupBy || 'day';
    const validGroupings = ['day', 'week', 'month'];

    if (!validGroupings.includes(groupBy)) {
      return res.status(400).json({
        error: 'Invalid groupBy parameter',
        details: `Must be one of: ${validGroupings.join(', ')}`,
        received: groupBy,
      });
    }

    const trends = await db.getHiringTrends(groupBy);
    res.json({
      success: true,
      data: trends,
      grouping: groupBy,
      data_points: trends.length,
    });
  } catch (error) {
    next(error);
  }
});

/**
 * GET /api/insights/stats - Overall job market statistics
 */
app.get('/api/insights/stats', async (req, res, next) => {
  try {
    if (!db) {
      return res
        .status(503)
        .json({ error: 'Database unavailable', details: 'MongoDB connection failed' });
    }

    const stats = await db.getJobStats();
    res.json({
      success: true,
      data: stats,
    });
  } catch (error) {
    next(error);
  }
});

/**
 * ==================== SCHEDULER MANAGEMENT ENDPOINTS ====================
 */

const schedulerManager = require('./scheduler-manager');

/**
 * POST /api/scheduler/start - Start the background scheduler
 */
app.post('/api/scheduler/start', (req, res, next) => {
  try {
    const result = schedulerManager.startScheduler();
    const statusCode = result.success ? 200 : 400;
    res.status(statusCode).json(result);
  } catch (error) {
    next(error);
  }
});

/**
 * POST /api/scheduler/stop - Stop the background scheduler
 */
app.post('/api/scheduler/stop', (req, res, next) => {
  try {
    const result = schedulerManager.stopScheduler();
    const statusCode = result.success ? 200 : 400;
    res.status(statusCode).json(result);
  } catch (error) {
    next(error);
  }
});

/**
 * GET /api/scheduler/status - Get scheduler status
 */
app.get('/api/scheduler/status', (req, res, next) => {
  try {
    const status = schedulerManager.getStatus();
    res.json({
      success: true,
      data: status,
    });
  } catch (error) {
    next(error);
  }
});

/**
 * Global error handling middleware
 */
app.use((err, req, res, next) => {
  console.error('[ERROR]', err.message);
  console.error(err.stack);

  res.status(err.status || 500).json({
    success: false,
    error: err.message || 'Internal Server Error',
    timestamp: new Date().toISOString(),
  });
});

/**
 * 404 handler
 */
app.use((req, res) => {
  res.status(404).json({
    success: false,
    error: 'Endpoint not found',
    path: req.path,
    method: req.method,
  });
});

/**
 * Start server and initialize database
 */
async function startServer() {
  try {
    console.log('[Startup] Attempting to initialize database...');
    console.log('[Startup] MongoDB URI:', process.env.MONGODB_URI ? 'Set' : 'Not set');
    console.log('[Startup] DB Name:', process.env.DB_NAME);
    console.log('[Startup] Port:', PORT);
    
    db = await initializeDatabase();
    console.log('[Startup] Database initialized successfully');

    const server = app.listen(PORT, '0.0.0.0', () => {
      console.log(`✅ Server running on port ${PORT}`);
      console.log(`✅ Environment: ${process.env.NODE_ENV}`);
    });

    // Graceful shutdown
    server.on('close', () => {
      console.log('[Shutdown] Server closed');
    });

  } catch (error) {
    console.error('[Startup] Failed to start server');
    console.error('[Startup] Error type:', error.constructor.name);
    console.error('[Startup] Error message:', error.message);
    console.error('[Startup] Error stack:', error.stack);
    
    // Still start the server even if DB fails (can retry)
    console.error('[Startup] Starting server without database...');
    const server = app.listen(PORT, '0.0.0.0', () => {
      console.log(`✅ Server running on port ${PORT} (Database unavailable)`);
      console.warn('⚠️  Database connection failed - API will return errors');
    });
  }
}

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log('\nShutting down gracefully...');
  process.exit(0);
});

startServer();
