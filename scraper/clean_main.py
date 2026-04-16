"""
Main entry point for data cleaning and transformation
"""
import sys
import os
import json

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from cleaner import DataCleaner
from logger import logger
from utils import DataExporter

def main():
    """Main execution for data cleaning"""
    logger.info("\n" + "=" * 60)
    logger.info("Job Market Analyzer - Data Cleaning Pipeline")
    logger.info("=" * 60 + "\n")
    
    try:
        # Initialize cleaner
        cleaner = DataCleaner()
        
        # Find latest raw data file
        raw_data_dir = "../data/raw"
        os.makedirs(raw_data_dir, exist_ok=True)
        
        raw_files = [f for f in os.listdir(raw_data_dir) if f.startswith('jobs_') and f.endswith('.json')]
        
        if not raw_files:
            # Try sample data
            sample_file = os.path.join(raw_data_dir, 'sample_jobs.json')
            if os.path.exists(sample_file):
                raw_file = sample_file
                logger.info(f"Using sample data: {sample_file}")
            else:
                raise FileNotFoundError(f"No raw job data files found in {raw_data_dir}")
        else:
            # Use most recent file
            raw_file = os.path.join(raw_data_dir, sorted(raw_files)[-1])
            logger.info(f"Using raw data file: {raw_file}")
        
        # Load raw data
        logger.info("\n[Step 1] Loading raw data...")
        cleaner.load_raw_data(raw_file)
        
        # Clean data
        logger.info("\n[Step 2] Executing cleaning pipeline...")
        cleaned_jobs = cleaner.clean()
        
        # Export cleaned data
        logger.info("\n[Step 3] Exporting cleaned data...")
        export_path = cleaner.export_cleaned_data()
        logger.info(f"✓ Cleaned data saved to: {export_path}")
        
        # Print summary
        stats = cleaner.get_stats()
        logger.info("\n[Summary]")
        logger.info(f"  Initial records: {stats['initial_count']}")
        logger.info(f"  Invalid records: {stats['invalid_records']}")
        logger.info(f"  Duplicates removed: {stats['duplicates_removed']}")
        logger.info(f"  Final cleaned records: {stats['final_count']}")
        logger.info(f"  Records with missing values filled: {stats['records_with_missing_values']}")
        
        # Sample output
        if cleaned_jobs:
            logger.info("\n[Sample Record]")
            logger.info(json.dumps(cleaned_jobs[0], indent=2, default=str))
        
        logger.info("\n" + "=" * 60)
        logger.info("✅ Data cleaning completed successfully!")
        logger.info("=" * 60 + "\n")
        
    except Exception as e:
        logger.error(f"Data cleaning failed: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()
