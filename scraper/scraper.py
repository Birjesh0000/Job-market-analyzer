"""
Job scraper module with pagination and error handling
Data sources: RemoteOK API, JustJoinIT API with fallback to mock data
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
    
    SOURCES = {
        'remoteok': 'https://remoteok.io/api/remote-jobs',
        'justjoin': 'https://justjoin.it/api/offers'
    }
    
    BASE_URL = SOURCES['remoteok']
    
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
    
    def scrape_all_pages(self, max_pages: int = 10, use_fallback: bool = True) -> List[Dict]:
        """
        Scrape multiple pages with pagination
        
        Args:
            max_pages: Maximum number of pages to scrape
            use_fallback: Use mock data if API fails
            
        Returns:
            List of all job dictionaries
        """
        logger.info(f"Starting scraper - max pages: {max_pages}")
        page = 0
        empty_pages = 0
        
        while page < max_pages:
            data = self.fetch_page(page)
            
            if data is None:
                # Failed to fetch even after retries
                empty_pages += 1
                if empty_pages > 2:
                    logger.warning(f"Multiple failed pages reached, will use fallback if enabled")
                    break
                page += 1
                continue
            
            if len(data) == 0:
                # No more data available
                logger.info(f"Reached end of pages at page {page}")
                break
            
            empty_pages = 0
            self.jobs.extend(data)
            page += 1
            
            # Rate limiting - be respectful to the API
            time.sleep(0.5)
        
        # If no jobs collected and fallback enabled, use mock data
        if len(self.jobs) == 0 and use_fallback:
            logger.warning("No jobs fetched from API, using mock data for demonstration")
            self.jobs = self._get_mock_jobs()
        
        logger.info(f"Scraping complete: {len(self.jobs)} total jobs, {len(self.failed_pages)} failed pages")
        return self.jobs
    
    def _get_mock_jobs(self) -> List[Dict]:
        """Generate mock job data for demonstration/testing"""
        mock_jobs = [
            {
                "id": "mock-1",
                "title": "Senior Python Developer",
                "company": "TechCorp Inc",
                "location": "San Francisco, CA",
                "url": "https://example.com/jobs/1",
                "type": "Full-time",
                "posted_at": "2024-04-15T10:00:00Z",
                "description": "Looking for experienced Python developer with 5+ years experience"
            },
            {
                "id": "mock-2",
                "title": "Data Engineer",
                "company": "DataFlow Systems",
                "location": "New York, NY",
                "url": "https://example.com/jobs/2",
                "type": "Full-time",
                "posted_at": "2024-04-14T14:30:00Z",
                "description": "Build data pipelines using Python and Spark"
            },
            {
                "id": "mock-3",
                "title": "Full Stack Developer",
                "company": "WebDev Studio",
                "location": "Remote",
                "url": "https://example.com/jobs/3",
                "type": "Full-time",
                "posted_at": "2024-04-13T09:15:00Z",
                "description": "React, Node.js, MongoDB - build modern web applications"
            },
            {
                "id": "mock-4",
                "title": "Machine Learning Engineer",
                "company": "AI Solutions Ltd",
                "location": "Boston, MA",
                "url": "https://example.com/jobs/4",
                "type": "Full-time",
                "posted_at": "2024-04-12T11:45:00Z",
                "description": "TensorFlow, PyTorch, work on ML models at scale"
            },
            {
                "id": "mock-5",
                "title": "DevOps Engineer",
                "company": "CloudOps Inc",
                "location": "Seattle, WA",
                "url": "https://example.com/jobs/5",
                "type": "Full-time",
                "posted_at": "2024-04-11T13:20:00Z",
                "description": "Kubernetes, Docker, AWS - infrastructure automation"
            },
            {
                "id": "mock-6",
                "title": "Database Administrator",
                "company": "DataVault Corp",
                "location": "Austin, TX",
                "url": "https://example.com/jobs/6",
                "type": "Full-time",
                "posted_at": "2024-04-10T10:00:00Z",
                "description": "PostgreSQL, MySQL, MongoDB - manage enterprise databases"
            },
            {
                "id": "mock-7",
                "title": "Frontend Developer",
                "company": "UI Masters",
                "location": "Los Angeles, CA",
                "url": "https://example.com/jobs/7",
                "type": "Full-time",
                "posted_at": "2024-04-09T15:30:00Z",
                "description": "React, Vue.js, Tailwind CSS - beautiful interfaces"
            },
            {
                "id": "mock-8",
                "title": "Backend Engineer",
                "company": "ServerTech",
                "location": "Chicago, IL",
                "url": "https://example.com/jobs/8",
                "type": "Full-time",
                "posted_at": "2024-04-08T09:00:00Z",
                "description": "Node.js, Java, Spring Boot - scalable backend systems"
            },
            {
                "id": "mock-9",
                "title": "QA Engineer",
                "company": "Quality First",
                "location": "Denver, CO",
                "url": "https://example.com/jobs/9",
                "type": "Full-time",
                "posted_at": "2024-04-07T12:15:00Z",
                "description": "Selenium, Cypress, Manual testing - ensure product quality"
            },
            {
                "id": "mock-10",
                "title": "Cloud Solutions Architect",
                "company": "CloudGiant",
                "location": "Remote",
                "url": "https://example.com/jobs/10",
                "type": "Full-time",
                "posted_at": "2024-04-06T14:45:00Z",
                "description": "Design cloud infrastructure for enterprise clients"
            }
        ]
        return mock_jobs
    
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
