/**
 * Scheduler Management Routes
 * Node.js bridge to Python scheduler
 */

const { spawn } = require('child_process');
const path = require('path');

let schedulerProcess = null;
let schedulerStatus = {
  running: false,
  started_at: null,
  last_check: null,
  status_message: 'Scheduler not started',
};

/**
 * Start the Python scheduler in background
 */
function startScheduler() {
  if (schedulerProcess) {
    return { success: false, message: 'Scheduler already running' };
  }

  try {
    const pythonPath = path.join(__dirname, '..', '.venv', 'Scripts', 'python.exe');
    const schedulerScript = path.join(__dirname, '..', 'scraper', 'scheduler_runner.py');

    schedulerProcess = spawn(pythonPath, [schedulerScript], {
      detached: true,
      stdio: 'pipe',
      env: {
        ...process.env,
        PYTHONUNBUFFERED: '1',
      },
    });

    schedulerStatus.running = true;
    schedulerStatus.started_at = new Date().toISOString();
    schedulerStatus.status_message = 'Scheduler started successfully';

    console.log('[Scheduler] Background process started with PID:', schedulerProcess.pid);

    schedulerProcess.on('error', (err) => {
      console.error('[Scheduler] Process error:', err);
      schedulerStatus.running = false;
    });

    return { success: true, message: 'Scheduler started', pid: schedulerProcess.pid };
  } catch (error) {
    console.error('[Scheduler] Failed to start:', error.message);
    return { success: false, message: error.message };
  }
}

/**
 * Stop the Python scheduler
 */
function stopScheduler() {
  if (!schedulerProcess) {
    return { success: false, message: 'Scheduler is not running' };
  }

  try {
    process.kill(-schedulerProcess.pid);
    schedulerProcess = null;
    schedulerStatus.running = false;
    schedulerStatus.status_message = 'Scheduler stopped';

    console.log('[Scheduler] Process stopped');
    return { success: true, message: 'Scheduler stopped' };
  } catch (error) {
    console.error('[Scheduler] Failed to stop:', error.message);
    return { success: false, message: error.message };
  }
}

/**
 * Get scheduler status
 */
function getStatus() {
  schedulerStatus.last_check = new Date().toISOString();
  return {
    running: schedulerProcess !== null && schedulerStatus.running,
    started_at: schedulerStatus.started_at,
    last_check: schedulerStatus.last_check,
    pid: schedulerProcess ? schedulerProcess.pid : null,
    status_message: schedulerStatus.status_message,
  };
}

module.exports = {
  startScheduler,
  stopScheduler,
  getStatus,
};
