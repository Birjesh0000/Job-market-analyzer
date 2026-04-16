import os
from dotenv import load_dotenv

load_dotenv(dotenv_path='../.env')

class Config:
    """Base configuration"""
    MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://localhost:27017')
    DB_NAME = os.getenv('DB_NAME', 'job_market_analyzer')
    DEBUG = os.getenv('DEBUG', 'False').lower() == 'true'

class ScraperConfig:
    """Scraper-specific configuration"""
    REQUEST_TIMEOUT = int(os.getenv('SCRAPER_REQUEST_TIMEOUT', '10'))
    MAX_RETRIES = int(os.getenv('SCRAPER_MAX_RETRIES', '3'))
    RETRY_DELAY = int(os.getenv('SCRAPER_RETRY_DELAY', '5'))
    DEFAULT_HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }

class SchedulerConfig:
    """Scheduler configuration"""
    INTERVAL_HOURS = int(os.getenv('SCHEDULER_INTERVAL_HOURS', '24'))
    TIMEZONE = 'UTC'
