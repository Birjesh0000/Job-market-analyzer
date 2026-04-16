"""
Test cleaning pipeline with sample data
"""
import sys
import os

# Add scraper directory to path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'scraper'))

from cleaner import DataCleaner
from logger import logger

def test_deduplication():
    """Test deduplication feature"""
    logger.info("\n" + "=" * 60)
    logger.info("TESTING: Deduplication with Sample Data")
    logger.info("=" * 60)
    
    cleaner = DataCleaner()
    # Load from data directory
    raw_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'raw', 'raw_sample_for_cleaning.json')
    cleaner.load_raw_data(raw_file)
    
    logger.info(f"\nLoaded {cleaner.cleaning_stats['initial_count']} raw records")
    logger.info("Sample duplicates in input:")
    logger.info("  - Senior Python Developer / TechCorp Inc / San Francisco")
    logger.info("  - Full Stack Developer / WebDev Studio / Remote")
    
    cleaned = cleaner.clean()
    
    stats = cleaner.get_stats()
    logger.info(f"\nCleaning Results:")
    logger.info(f"  Initial records: {stats['initial_count']}")
    logger.info(f"  Invalid records removed: {stats['invalid_records']}")
    logger.info(f"  Duplicate records removed: {stats['duplicates_removed']}")
    logger.info(f"  Final cleaned records: {stats['final_count']}")
    
    dedup_rate = (stats['duplicates_removed'] / stats['initial_count'] * 100) if stats['initial_count'] > 0 else 0
    logger.info(f"  Deduplication rate: {dedup_rate:.1f}%")
    
    # Show normalized data
    logger.info(f"\nSample cleaned records:")
    for i, job in enumerate(cleaned[:3]):
        logger.info(f"\n  Record {i+1}:")
        logger.info(f"    Title: {job.get('title')}")
        logger.info(f"    Company: {job.get('company')}")
        logger.info(f"    Location: {job.get('location')}")
        logger.info(f"    Skills: {', '.join(job.get('skills', [])[:5])}")
    
    logger.info("\n" + "=" * 60 + "\n")

if __name__ == '__main__':
    test_deduplication()
