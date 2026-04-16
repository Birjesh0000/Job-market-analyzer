"""
Job scraper module with pagination and error handling
Data source: GitHub Jobs API
"""
import requests
import json
import time
from typing import List, Dict, Optional
from datetime import datetime
from config import ScraperConfig
from logger import logger

class JobScraper:
    """Scraper for job listings with retry logic and pagination"""
    
    BASE_URL = "https://jobs.github.com/positions.json"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(ScraperConfig.DEFAULT_HEADERS)
        self.jobs = []
        self.failed_pages = []
    
    def fetch_page(self, page: int = 0) -> Optional[List[Dict]]:
        """
        Fetch a single page of job listings
        
        Args:
            page: Page number (0-indexed)
            
        Returns:
            List of job dictionaries or None if failed
        """
        params = {'page': page}
        retry_count = 0
        
        while retry_count < ScraperConfig.MAX_RETRIES:
            try:
                logger.info(f"Fetching page {page} (attempt {retry_count + 1})")
                response = self.session.get(
                    self.BASE_URL,
                    params=params,
                    timeout=ScraperConfig.REQUEST_TIMEOUT
                )
                response.raise_for_status()
                
                data = response.json()
                logger.info(f"Successfully fetched page {page}: {len(data)} jobs")
                return data
                
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout on page {page}, retrying...")
                retry_count += 1
                time.sleep(ScraperConfig.RETRY_DELAY)
                
            except requests.exceptions.ConnectionError:
                logger.warning(f"Connection error on page {page}, retrying...")
                retry_count += 1
                time.sleep(ScraperConfig.RETRY_DELAY)
                
            except requests.exceptions.HTTPError as e:
                logger.error(f"HTTP error on page {page}: {e.response.status_code}")
                if e.response.status_code == 404:
                    # 404 means no more pages
                    return []
                retry_count += 1
                time.sleep(ScraperConfig.RETRY_DELAY)
                
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON response on page {page}")
                retry_count += 1
                time.sleep(ScraperConfig.RETRY_DELAY)
                
            except Exception as e:
                logger.error(f"Unexpected error on page {page}: {str(e)}")
                retry_count += 1
                time.sleep(ScraperConfig.RETRY_DELAY)
        
        logger.error(f"Failed to fetch page {page} after {ScraperConfig.MAX_RETRIES} retries")
        self.failed_pages.append(page)
        return None
    
    def scrape_all_pages(self, max_pages: int = 10) -> List[Dict]:
        """
        Scrape multiple pages with pagination
        
        Args:
            max_pages: Maximum number of pages to scrape
            
        Returns:
            List of all job dictionaries
        """
        logger.info(f"Starting scraper - max pages: {max_pages}")
        page = 0
        
        while page < max_pages:
            data = self.fetch_page(page)
            
            if data is None:
                # Failed to fetch even after retries
                page += 1
                continue
            
            if len(data) == 0:
                # No more data available
                logger.info(f"Reached end of pages at page {page}")
                break
            
            self.jobs.extend(data)
            page += 1
            
            # Rate limiting - be respectful to the API
            time.sleep(1)
        
        logger.info(f"Scraping complete: {len(self.jobs)} total jobs, {len(self.failed_pages)} failed pages")
        return self.jobs
    
    def get_jobs(self) -> List[Dict]:
        """Get scraped jobs"""
        return self.jobs
    
    def get_stats(self) -> Dict:
        """Get scraping statistics"""
        return {
            'total_jobs': len(self.jobs),
            'failed_pages': self.failed_pages,
            'failed_page_count': len(self.failed_pages),
            'timestamp': datetime.now().isoformat()
        }
