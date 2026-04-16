"""
Database insertion pipeline
Integrates cleaned data into MongoDB
"""
import json
import os
from datetime import datetime
from typing import List, Dict, Tuple
from logger import logger
from db_manager import DatabaseManager, MongoConnection


class DatabaseInserter:
    """Orchestrate database insertion from cleaned data"""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.insertion_stats = {
            'total_processed': 0,
            'inserted': 0,
            'updated': 0,
            'failed': 0,
            'errors': [],
            'timestamp': None
        }
    
    def insert_cleaned_data(self, cleaned_jobs: List[Dict]) -> Tuple[int, int, int]:
        """
        Insert cleaned job data into database
        
        Args:
            cleaned_jobs: List of cleaned job records
            
        Returns:
            Tuple of (inserted, updated, failed) counts
        """
        self.insertion_stats['total_processed'] = len(cleaned_jobs)
        
        logger.info("\n" + "=" * 60)
        logger.info("Starting database insertion pipeline")
        logger.info("=" * 60)
        
        # Insert jobs
        inserted, updated, error_ids = self.db_manager.insert_jobs(cleaned_jobs)
        
        self.insertion_stats['inserted'] = inserted
        self.insertion_stats['updated'] = updated
        self.insertion_stats['failed'] = len(error_ids)
        self.insertion_stats['errors'] = error_ids
        self.insertion_stats['timestamp'] = datetime.utcnow().isoformat()
        
        logger.info(f"\nInsertion Results:")
        logger.info(f"  Total processed: {len(cleaned_jobs)}")
        logger.info(f"  Inserted (new): {inserted}")
        logger.info(f"  Updated (existing): {updated}")
        logger.info(f"  Failed: {len(error_ids)}")
        
        if error_ids:
            logger.warning(f"  Failed IDs: {', '.join(error_ids[:5])}")
            if len(error_ids) > 5:
                logger.warning(f"  ... and {len(error_ids) - 5} more")
        
        logger.info("=" * 60 + "\n")
        
        return inserted, updated, len(error_ids)
    
    def verify_insertion(self, expected_count: int) -> Dict:
        """
        Verify data was inserted successfully
        
        Args:
            expected_count: Expected number of records in database
            
        Returns:
            Verification results dictionary
        """
        logger.info("Verifying database insertion...")
        
        actual_count = self.db_manager.get_job_count()
        
        verification = {
            'expected_count': expected_count,
            'actual_count': actual_count,
            'verified': actual_count >= expected_count,
            'sample_jobs': self.db_manager.get_jobs(limit=3),
            'top_skills': self.db_manager.get_top_skills(limit=5),
            'top_companies': self.db_manager.get_top_companies(limit=5)
        }
        
        logger.info(f"\nVerification Results:")
        logger.info(f"  Expected: {expected_count}")
        logger.info(f"  Actual in DB: {actual_count}")
        logger.info(f"  Status: {'VERIFIED' if verification['verified'] else 'MISMATCH'}")
        
        if verification['top_skills']:
            logger.info(f"\n  Top 5 Skills:")
            for skill, count in verification['top_skills']:
                logger.info(f"    - {skill}: {count} jobs")
        
        if verification['top_companies']:
            logger.info(f"\n  Top 5 Companies:")
            for company, count in verification['top_companies']:
                logger.info(f"    - {company}: {count} jobs")
        
        return verification
    
    def get_insertion_stats(self) -> Dict:
        """Get insertion statistics"""
        return self.insertion_stats


class PipelineIntegration:
    """Integrate cleaning and database insertion into single pipeline"""
    
    def __init__(self):
        self.db_inserter = DatabaseInserter()
    
    def process_and_store(self, cleaned_jobs: List[Dict], export_to_db: bool = True) -> Dict:
        """
        Process cleaned jobs and store in database
        
        Args:
            cleaned_jobs: List of cleaned job records
            export_to_db: Whether to export to database
            
        Returns:
            Comprehensive results dictionary
        """
        logger.info("\n" + "=" * 60)
        logger.info("PIPELINE: Cleaning → Database")
        logger.info("=" * 60 + "\n")
        
        results = {
            'stage': 'cleaning_to_database',
            'input_records': len(cleaned_jobs),
            'database_mode': 'MongoDB' if self.db_inserter.db_manager.db is not None else 'Local Storage'
        }
        
        if not export_to_db:
            logger.info("Database export disabled - verification only")
            results['skipped'] = True
            return results
        
        # Insert into database
        inserted, updated, failed = self.db_inserter.insert_cleaned_data(cleaned_jobs)
        results['inserted'] = inserted
        results['updated'] = updated
        results['failed'] = failed
        
        # Verify insertion
        verification = self.db_inserter.verify_insertion(len(cleaned_jobs))
        results['verification'] = verification
        
        # Get statistics
        stats = self.db_inserter.get_insertion_stats()
        results['stats'] = stats
        
        # Database statistics
        db_stats = self.db_inserter.db_manager.get_database_stats()
        results['database_stats'] = db_stats
        
        logger.info("\n" + "=" * 60)
        logger.info("Pipeline Complete")
        logger.info("=" * 60 + "\n")
        
        return results
    
    def get_sample_data(self, limit: int = 5) -> Dict:
        """
        Get sample data from database for inspection
        
        Args:
            limit: Number of records to retrieve
            
        Returns:
            Sample jobs with metadata
        """
        jobs = self.db_inserter.db_manager.get_jobs(limit=limit)
        
        sample_data = {
            'count': len(jobs),
            'jobs': jobs,
            'sample_skills': [],
            'sample_companies': []
        }
        
        if jobs:
            # Extract sample skills
            all_skills = set()
            for job in jobs:
                all_skills.update(job.get('skills', []))
            sample_data['sample_skills'] = sorted(list(all_skills))[:10]
            
            # Extract sample companies
            companies = set(j.get('company') for j in jobs)
            sample_data['sample_companies'] = sorted(list(companies))
        
        return sample_data


def integrate_full_pipeline(cleaned_data_path: str) -> Dict:
    """
    Run complete pipeline: load cleaned data → insert to database
    
    Args:
        cleaned_data_path: Path to cleaned data JSON file
        
    Returns:
        Full pipeline results
    """
    logger.info("\n" + "=" * 60)
    logger.info("FULL PIPELINE EXECUTION")
    logger.info("=" * 60 + "\n")
    
    pipeline = PipelineIntegration()
    
    try:
        # Load cleaned data
        logger.info(f"Loading cleaned data from {cleaned_data_path}...")
        with open(cleaned_data_path, 'r', encoding='utf-8') as f:
            cleaned_jobs = json.load(f)
        
        logger.info(f"Loaded {len(cleaned_jobs)} cleaned records")
        
        # Process and store
        results = pipeline.process_and_store(cleaned_jobs, export_to_db=True)
        
        # Get sample results
        sample = pipeline.get_sample_data(limit=3)
        results['sample'] = sample
        
        logger.info("\nFull Pipeline Execution Complete")
        return results
        
    except FileNotFoundError:
        logger.error(f"Cleaned data file not found: {cleaned_data_path}")
        return {'error': 'File not found'}
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in cleaned data: {e}")
        return {'error': 'Invalid JSON'}
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}", exc_info=True)
        return {'error': str(e)}
