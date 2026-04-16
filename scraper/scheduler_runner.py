#!/usr/bin/env python3
"""
Scheduler Runner
Starts the APScheduler in the foreground
Used by Node.js backend to run scheduler as subprocess
"""

import sys
import signal
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scraper.scheduler import get_scheduler, initialize_scheduler, shutdown_scheduler
from scraper.logger import logger


def signal_handler(sig, frame):
    """Handle shutdown signals"""
    logger.info('Shutdown signal received, stopping scheduler...')
    shutdown_scheduler()
    sys.exit(0)


def main():
    """Main entry point for scheduler runner"""
    try:
        # Register signal handlers
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        logger.info('='*70)
        logger.info('SCHEDULER RUNNER STARTED')
        logger.info('='*70)

        # Initialize scheduler
        if not initialize_scheduler():
            logger.error('Failed to initialize scheduler')
            sys.exit(1)

        # Keep the process running
        logger.info('Scheduler is running. Press Ctrl+C to stop.')
        signal.pause()

    except Exception as e:
        logger.error(f'Scheduler runner error: {str(e)}')
        shutdown_scheduler()
        sys.exit(1)


if __name__ == '__main__':
    main()
