"""
Utility functions for scraper
"""
import json
import os
from datetime import datetime
from typing import List, Dict
from logger import logger

class DataExporter:
    """Handle data export to JSON"""
    
    @staticmethod
    def export_to_json(data: List[Dict], filename: str = None) -> str:
        """
        Export data to JSON file
        
        Args:
            data: List of job dictionaries
            filename: Output filename (optional, auto-generated if not provided)
            
        Returns:
            Path to exported file
        """
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"jobs_{timestamp}.json"
        
        output_path = f"../data/raw/{filename}"
        os.makedirs("../data/raw", exist_ok=True)
        
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Exported {len(data)} jobs to {output_path}")
            return output_path
            
        except IOError as e:
            logger.error(f"Failed to export data: {e}")
            raise
    
    @staticmethod
    def load_from_json(filepath: str) -> List[Dict]:
        """
        Load data from JSON file
        
        Args:
            filepath: Path to JSON file
            
        Returns:
            List of job dictionaries
        """
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            logger.info(f"Loaded {len(data)} jobs from {filepath}")
            return data
        except IOError as e:
            logger.error(f"Failed to load data: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in {filepath}: {e}")
            raise


class DataValidator:
    """Validate scraped job data"""
    
    REQUIRED_FIELDS = ['id', 'title', 'company', 'location', 'url']
    
    @staticmethod
    def validate_job(job: Dict) -> bool:
        """
        Validate a single job record
        
        Args:
            job: Job dictionary
            
        Returns:
            True if valid, False otherwise
        """
        for field in DataValidator.REQUIRED_FIELDS:
            if field not in job or not job[field]:
                return False
        return True
    
    @staticmethod
    def validate_jobs(jobs: List[Dict]) -> tuple:
        """
        Validate all jobs and separate valid from invalid
        
        Args:
            jobs: List of job dictionaries
            
        Returns:
            Tuple of (valid_jobs, invalid_jobs)
        """
        valid = []
        invalid = []
        
        for job in jobs:
            if DataValidator.validate_job(job):
                valid.append(job)
            else:
                invalid.append(job)
        
        if invalid:
            logger.warning(f"Found {len(invalid)} invalid jobs")
        
        return valid, invalid


class DataAnalyzer:
    """Analyze scraped data"""
    
    @staticmethod
    def analyze(jobs: List[Dict]) -> Dict:
        """
        Analyze job data for insights
        
        Args:
            jobs: List of job dictionaries
            
        Returns:
            Dictionary with analysis results
        """
        if not jobs:
            return {}
        
        locations = set()
        companies = set()
        job_types = {}
        
        for job in jobs:
            if 'location' in job:
                locations.add(job['location'])
            if 'company' in job:
                companies.add(job['company'])
            if 'type' in job:
                job_type = job['type']
                job_types[job_type] = job_types.get(job_type, 0) + 1
        
        return {
            'total_jobs': len(jobs),
            'unique_locations': len(locations),
            'unique_companies': len(companies),
            'locations_sample': list(locations)[:5],
            'companies_sample': list(companies)[:5],
            'job_types': job_types
        }
