"""
Scheduler Service
Manages background job scheduling using APScheduler
"""

import os
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

from scraper.logger import logger
from scraper.pipeline_main import run_full_pipeline
from scraper.analytics import SkillsAnalyzer, QualityAnalyzer
from scraper.db_manager import DatabaseManager
from scraper.db import get_database_manager

# Global scheduler instance
_scheduler = None
_scheduler_state = {
    'running': False,
    'last_scrape': None,
    'last_analysis': None,
    'total_jobs_scraped': 0,
    'total_jobs_processed': 0,
    'error_count': 0,
}


class JobScheduler:
    """Background job scheduler for data processing"""

    def __init__(self):
        self.scheduler = BackgroundScheduler(daemon=True)
        self.jobs_config = []

    def job_executed_listener(self, event):
        """Handle successful job execution"""
        logger.info(f'Job {event.job_id} executed successfully')
        _scheduler_state['last_execution'] = datetime.now()

    def job_error_listener(self, event):
        """Handle job execution errors"""
        logger.error(f'Job {event.job_id} failed: {event.exception}')
        _scheduler_state['error_count'] += 1

    def start(self):
        """Start the scheduler"""
        if self.scheduler.running:
            logger.warning('Scheduler is already running')
            return False

        try:
            # Add event listeners
            self.scheduler.add_listener(
                self.job_executed_listener,
                EVENT_JOB_EXECUTED
            )
            self.scheduler.add_listener(
                self.job_error_listener,
                EVENT_JOB_ERROR
            )

            # Add jobs
            self._add_jobs()

            # Start scheduler
            self.scheduler.start()
            _scheduler_state['running'] = True
            logger.info('✓ Scheduler started successfully')
            return True
        except Exception as e:
            logger.error(f'Failed to start scheduler: {str(e)}')
            return False

    def stop(self):
        """Stop the scheduler"""
        if not self.scheduler.running:
            logger.warning('Scheduler is not running')
            return False

        try:
            self.scheduler.shutdown(wait=False)
            _scheduler_state['running'] = False
            logger.info('✓ Scheduler stopped successfully')
            return True
        except Exception as e:
            logger.error(f'Failed to stop scheduler: {str(e)}')
            return False

    def pause(self):
        """Pause all scheduled jobs"""
        if not self.scheduler.running:
            return False

        try:
            self.scheduler.pause()
            logger.info('✓ Scheduler paused')
            return True
        except Exception as e:
            logger.error(f'Failed to pause scheduler: {str(e)}')
            return False

    def resume(self):
        """Resume all scheduled jobs"""
        if not self.scheduler.running:
            return False

        try:
            self.scheduler.resume()
            logger.info('✓ Scheduler resumed')
            return True
        except Exception as e:
            logger.error(f'Failed to resume scheduler: {str(e)}')
            return False

    def _add_jobs(self):
        """Add configured jobs to scheduler"""
        try:
            # Add scrape job
            self.scheduler.add_job(
                scrape_jobs_task,
                'interval',
                hours=int(os.getenv('SCHEDULER_INTERVAL_HOURS', 24)),
                id='scrape_jobs',
                name='Scrape and process jobs',
                coalesce=True,
                max_instances=1,
                misfire_grace_time=300
            )
            logger.info('Added job: scrape_jobs')

            # Add analysis job
            self.scheduler.add_job(
                analyze_jobs_task,
                'interval',
                hours=int(os.getenv('ANALYTICS_INTERVAL_HOURS', 12)),
                id='analyze_jobs',
                name='Analyze job market trends',
                coalesce=True,
                max_instances=1,
                misfire_grace_time=300
            )
            logger.info('Added job: analyze_jobs')
        except Exception as e:
            logger.error(f'Failed to add jobs: {str(e)}')

    def get_jobs(self):
        """Get list of scheduled jobs"""
        try:
            jobs = self.scheduler.get_jobs()
            return [
                {
                    'id': job.id,
                    'name': job.name,
                    'trigger': str(job.trigger),
                    'next_run_time': job.next_run_time.isoformat() if job.next_run_time else None,
                }
                for job in jobs
            ]
        except Exception as e:
            logger.error(f'Failed to get jobs: {str(e)}')
            return []

    def get_job_by_id(self, job_id):
        """Get specific job by ID"""
        try:
            job = self.scheduler.get_job(job_id)
            if job:
                return {
                    'id': job.id,
                    'name': job.name,
                    'trigger': str(job.trigger),
                    'next_run_time': job.next_run_time.isoformat() if job.next_run_time else None,
                }
            return None
        except Exception as e:
            logger.error(f'Failed to get job: {str(e)}')
            return None

    def trigger_job(self, job_id):
        """Manually trigger a job execution"""
        try:
            job = self.scheduler.get_job(job_id)
            if not job:
                logger.error(f'Job {job_id} not found')
                return False

            # Execute job immediately
            job.func(*job.args, **job.kwargs)
            logger.info(f'✓ Job {job_id} triggered manually')
            return True
        except Exception as e:
            logger.error(f'Failed to trigger job {job_id}: {str(e)}')
            return False

    def get_status(self):
        """Get scheduler status"""
        return {
            'running': self.scheduler.running,
            'paused': self.scheduler.paused if hasattr(self.scheduler, 'paused') else False,
            'jobs': len(self.scheduler.get_jobs()),
            'last_scrape': _scheduler_state['last_scrape'],
            'last_analysis': _scheduler_state['last_analysis'],
            'total_jobs_scraped': _scheduler_state['total_jobs_scraped'],
            'error_count': _scheduler_state['error_count'],
        }


def scrape_jobs_task():
    """Background task: Scrape and process jobs"""
    logger.info('[SCHEDULER] Starting scrape job task...')
    try:
        run_full_pipeline()
        _scheduler_state['last_scrape'] = datetime.now().isoformat()
        logger.info('[SCHEDULER] Scrape job task completed successfully')
    except Exception as e:
        logger.error(f'[SCHEDULER] Scrape job task failed: {str(e)}')
        _scheduler_state['error_count'] += 1


def analyze_jobs_task():
    """Background task: Analyze job market trends"""
    logger.info('[SCHEDULER] Starting analysis job task...')
    try:
        db_manager = get_database_manager()

        # Run skills analysis
        skills_analyzer = SkillsAnalyzer(db_manager)
        skills_report = skills_analyzer.generate_report()

        # Run quality analysis
        quality_analyzer = QualityAnalyzer(db_manager)
        quality_report = quality_analyzer.generate_report()

        _scheduler_state['last_analysis'] = datetime.now().isoformat()
        logger.info('[SCHEDULER] Analysis job task completed successfully')
        logger.info(f'[SCHEDULER] Skills Report: {len(skills_report)} skills analyzed')
        logger.info(f'[SCHEDULER] Quality Report: {quality_report.get("total_jobs", 0)} jobs analyzed')
    except Exception as e:
        logger.error(f'[SCHEDULER] Analysis job task failed: {str(e)}')
        _scheduler_state['error_count'] += 1


def get_scheduler():
    """Get or create global scheduler instance"""
    global _scheduler
    if _scheduler is None:
        _scheduler = JobScheduler()
    return _scheduler


def initialize_scheduler():
    """Initialize and start the scheduler"""
    scheduler = get_scheduler()
    if not os.getenv('SCHEDULER_ENABLED', 'false').lower() == 'true':
        logger.info('Scheduler is disabled in configuration')
        return False

    return scheduler.start()


def shutdown_scheduler():
    """Shutdown the scheduler"""
    global _scheduler
    if _scheduler and _scheduler.scheduler.running:
        _scheduler.stop()
        _scheduler = None
