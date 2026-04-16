"""
Scheduler Configuration
Manages APScheduler setup and job definitions
"""

import os
from datetime import datetime, timedelta

class SchedulerConfig:
    """Configuration for APScheduler"""

    # Scheduler settings
    SCHEDULER_ENABLED = os.getenv('SCHEDULER_ENABLED', 'true').lower() == 'true'
    SCHEDULER_TYPE = os.getenv('SCHEDULER_TYPE', 'background')  # background or blocking
    SCHEDULER_TIMEZONE = 'UTC'

    # Job scheduling intervals (in hours)
    SCRAPE_INTERVAL_HOURS = int(os.getenv('SCHEDULER_INTERVAL_HOURS', 24))
    ANALYTICS_INTERVAL_HOURS = int(os.getenv('ANALYTICS_INTERVAL_HOURS', 12))

    # Job execution settings
    JOB_MAX_INSTANCES = 1  # Prevent overlapping executions
    JOB_COALESCE = True  # Skip missed runs
    JOB_MISFIRE_GRACE_TIME = 300  # 5 minutes grace period

    # Job configuration
    JOBS = [
        {
            'id': 'scrape_jobs',
            'func': 'scraper.scheduler:scrape_jobs_task',
            'trigger': 'interval',
            'hours': SCRAPE_INTERVAL_HOURS,
            'args': [],
            'kwargs': {},
            'coalesce': JOB_COALESCE,
            'max_instances': JOB_MAX_INSTANCES,
            'misfire_grace_time': JOB_MISFIRE_GRACE_TIME,
            'name': 'Scrape and process jobs',
            'description': f'Runs every {SCRAPE_INTERVAL_HOURS} hours'
        },
        {
            'id': 'analyze_jobs',
            'func': 'scraper.scheduler:analyze_jobs_task',
            'trigger': 'interval',
            'hours': ANALYTICS_INTERVAL_HOURS,
            'args': [],
            'kwargs': {},
            'coalesce': JOB_COALESCE,
            'max_instances': JOB_MAX_INSTANCES,
            'misfire_grace_time': JOB_MISFIRE_GRACE_TIME,
            'name': 'Analyze job market trends',
            'description': f'Runs every {ANALYTICS_INTERVAL_HOURS} hours'
        }
    ]

    # Executor settings
    EXECUTORS = {
        'default': {
            'type': 'threadpool',
            'max_workers': 2
        }
    }

    # Job store settings
    JOB_STORES = {
        'default': {
            'type': 'memory'
        }
    }


def get_scheduler_config():
    """Get scheduler configuration dict"""
    return {
        'apscheduler.schedulers.default': SchedulerConfig.SCHEDULER_TIMEZONE,
        'apscheduler.executors.default.type': 'threadpool',
        'apscheduler.executors.default.max_workers': '2',
        'apscheduler.job_stores.default.type': 'memory',
        'apscheduler.timezone': SchedulerConfig.SCHEDULER_TIMEZONE,
    }
