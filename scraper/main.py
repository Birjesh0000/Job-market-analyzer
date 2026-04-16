"""
Main scraper entry point
"""
import sys
import os

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scraper import JobScraper
from utils import DataExporter, DataValidator, DataAnalyzer
from logger import logger

def main():
    """Main execution"""
    logger.info("=== Job Market Analyzer - Scraper ===")
    
    try:
        # Initialize scraper
        scraper = JobScraper()
        
        # Scrape jobs (limit to 5 pages for initial run)
        logger.info("Starting scrape process...")
        jobs = scraper.scrape_all_pages(max_pages=5)
        
        # Validate data
        logger.info("Validating scraped data...")
        valid_jobs, invalid_jobs = DataValidator.validate_jobs(jobs)
        logger.info(f"Valid: {len(valid_jobs)}, Invalid: {len(invalid_jobs)}")
        
        # Export valid jobs
        if valid_jobs:
            export_path = DataExporter.export_to_json(valid_jobs)
            logger.info(f"Data exported to: {export_path}")
        
        # Analyze data
        analysis = DataAnalyzer.analyze(valid_jobs)
        logger.info(f"Data Analysis: {analysis}")
        
        # Print statistics
        stats = scraper.get_stats()
        logger.info(f"Scraping Statistics: {stats}")
        
        logger.info("=== Scraper completed successfully ===")
        
    except Exception as e:
        logger.error(f"Scraper failed: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()
