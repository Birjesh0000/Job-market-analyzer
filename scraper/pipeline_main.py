"""
Main entry point for full pipeline: scrape → clean → store in database
"""
import sys
import os
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from cleaner import DataCleaner
from db_pipeline import PipelineIntegration, integrate_full_pipeline
from logger import logger
from db_manager import MongoConnection


def main_integrated_pipeline():
    """
    Run complete integrated pipeline:
    1. Load raw data
    2. Clean data
    3. Insert into database
    """
    logger.info("\n" + "=" * 70)
    logger.info("JOB MARKET ANALYZER - COMPLETE PIPELINE")
    logger.info("Raw Data → Cleaning → Database Storage")
    logger.info("=" * 70 + "\n")
    
    try:
        # Initialize components
        cleaner = DataCleaner()
        db_pipeline = PipelineIntegration()
        
        # STAGE 1: Load raw data
        logger.info("\n[STAGE 1] Loading Raw Data...")
        raw_data_dir = "../data/raw"
        os.makedirs(raw_data_dir, exist_ok=True)
        
        raw_files = [f for f in os.listdir(raw_data_dir) 
                    if f.startswith('jobs_') and f.endswith('.json')]
        
        if not raw_files:
            sample_file = os.path.join(raw_data_dir, 'sample_jobs.json')
            if os.path.exists(sample_file):
                raw_file = sample_file
            else:
                raise FileNotFoundError(f"No raw data files found in {raw_data_dir}")
        else:
            raw_file = os.path.join(raw_data_dir, sorted(raw_files)[-1])
        
        logger.info(f"Using: {raw_file}")
        cleaner.load_raw_data(raw_file)
        
        # STAGE 2: Clean data
        logger.info("\n[STAGE 2] Cleaning Data...")
        cleaned_jobs = cleaner.clean()
        stats = cleaner.get_stats()
        
        logger.info(f"Cleaning Summary:")
        logger.info(f"  Initial: {stats['initial_count']}")
        logger.info(f"  Invalid removed: {stats['invalid_records']}")
        logger.info(f"  Duplicates removed: {stats['duplicates_removed']}")
        logger.info(f"  Final cleaned: {stats['final_count']}")
        
        # STAGE 3: Insert to database
        logger.info("\n[STAGE 3] Inserting into Database...")
        
        # Process and store
        results = db_pipeline.process_and_store(cleaned_jobs, export_to_db=True)
        
        logger.info(f"Database Insertion Summary:")
        logger.info(f"  Inserted: {results['inserted']}")
        logger.info(f"  Updated: {results['updated']}")
        logger.info(f"  Failed: {results['failed']}")
        logger.info(f"  Mode: {results['database_mode']}")
        
        # STAGE 4: Verification
        logger.info("\n[STAGE 4] Data Verification...")
        verification = results.get('verification', {})
        logger.info(f"  Expected: {verification.get('expected_count')}")
        logger.info(f"  In Database: {verification.get('actual_count')}")
        logger.info(f"  Status: {verification.get('verified', False)}")
        
        # STAGE 5: Analytics
        logger.info("\n[STAGE 5] Analytics...")
        db_stats = results.get('database_stats', {})
        logger.info(f"  Total jobs in DB: {db_stats.get('total_jobs')}")
        
        if db_stats.get('top_skills'):
            logger.info("\n  Top 5 Most Required Skills:")
            for skill, count in db_stats['top_skills'][:5]:
                logger.info(f"    - {skill}: {count} jobs")
        
        if db_stats.get('top_companies'):
            logger.info("\n  Top 5 Hiring Companies:")
            for company, count in db_stats['top_companies'][:5]:
                logger.info(f"    - {company}: {count} jobs")
        
        # Summary
        logger.info("\n" + "=" * 70)
        logger.info("PIPELINE EXECUTION COMPLETED SUCCESSFULLY")
        logger.info("=" * 70)
        logger.info(f"Total jobs processed: {len(cleaned_jobs)}")
        logger.info(f"Database records: {db_stats.get('total_jobs')}")
        logger.info(f"Mode: {results['database_mode']}")
        logger.info("=" * 70 + "\n")
        
        return results
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main_integrated_pipeline()
