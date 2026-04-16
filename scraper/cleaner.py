"""
Data cleaning and transformation pipeline
"""
import json
import os
from datetime import datetime
from typing import List, Dict, Tuple
from transformations import SkillNormalizer, TextCleaner, DuplicateDetector, MissingValueHandler
from logger import logger


class DataCleaner:
    """Orchestrate data cleaning pipeline"""
    
    def __init__(self):
        self.raw_jobs = []
        self.cleaned_jobs = []
        self.cleaning_stats = {
            'initial_count': 0,
            'after_validation': 0,
            'after_deduplication': 0,
            'duplicates_removed': 0,
            'invalid_records': 0,
            'final_count': 0,
            'records_with_missing_values': 0
        }
    
    def load_raw_data(self, filepath: str) -> List[Dict]:
        """
        Load raw job data from JSON file
        
        Args:
            filepath: Path to raw JSON file
            
        Returns:
            List of job dictionaries
        """
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                self.raw_jobs = json.load(f)
            
            self.cleaning_stats['initial_count'] = len(self.raw_jobs)
            logger.info(f"Loaded {len(self.raw_jobs)} raw jobs from {filepath}")
            return self.raw_jobs
            
        except IOError as e:
            logger.error(f"Failed to load raw data: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in {filepath}: {e}")
            raise
    
    def _validate_records(self, jobs: List[Dict]) -> Tuple[List[Dict], int]:
        """
        Validate and filter records with required fields
        
        Args:
            jobs: List of job records
            
        Returns:
            Tuple of (valid_jobs, invalid_count)
        """
        valid_jobs = []
        invalid_count = 0
        
        for job in jobs:
            if MissingValueHandler.is_record_valid(job):
                valid_jobs.append(job)
            else:
                invalid_count += 1
                logger.debug(f"Skipping invalid record: {job}")
        
        logger.info(f"Validation: {len(valid_jobs)} valid, {invalid_count} invalid")
        return valid_jobs, invalid_count
    
    def _remove_duplicates(self, jobs: List[Dict]) -> Tuple[List[Dict], int]:
        """
        Remove duplicate job records
        
        Args:
            jobs: List of job records
            
        Returns:
            Tuple of (unique_jobs, duplicates_removed_count)
        """
        unique_jobs, duplicates_removed = DuplicateDetector.remove_duplicates(jobs)
        logger.info(f"Deduplication: Removed {duplicates_removed} duplicates")
        return unique_jobs, duplicates_removed
    
    def _normalize_text_fields(self, jobs: List[Dict]) -> List[Dict]:
        """
        Clean and normalize text fields
        
        Args:
            jobs: List of job records
            
        Returns:
            List with normalized text fields
        """
        for job in jobs:
            job['title'] = TextCleaner.clean_text(job.get('title', ''))
            job['company'] = TextCleaner.clean_company_name(job.get('company', ''))
            job['location'] = TextCleaner.clean_location(job.get('location', ''))
            job['description'] = TextCleaner.clean_text(job.get('description', ''), max_length=1000)
        
        logger.info("Normalized text fields")
        return jobs
    
    def _extract_and_normalize_skills(self, jobs: List[Dict]) -> List[Dict]:
        """
        Extract and normalize skills from job descriptions
        
        Args:
            jobs: List of job records
            
        Returns:
            List with normalized skills field
        """
        for job in jobs:
            # Extract skills from description (simple heuristic)
            skills_text = ''
            description = job.get('description', '')
            
            # Try to extract skills section from description
            # This is a simple implementation - in production, use ML/NLP
            import re
            skills_match = re.search(r'(?:skills?|requirements?|qualifications?)[\s:]*([^.]*)', 
                                     description, re.IGNORECASE)
            if skills_match:
                skills_text = skills_match.group(1)
            else:
                # Fallback: extract tech keywords from title and description
                skills_text = job.get('title', '') + ' ' + description
            
            # Normalize skills
            job['skills'] = SkillNormalizer.normalize_skills_list(skills_text)
        
        logger.info("Extracted and normalized skills")
        return jobs
    
    def _handle_missing_values(self, jobs: List[Dict]) -> List[Dict]:
        """
        Fill missing values with sensible defaults
        
        Args:
            jobs: List of job records
            
        Returns:
            List with missing values handled
        """
        records_with_missing = 0
        
        for job in jobs:
            before = len(job)
            job = MissingValueHandler.fill_missing_fields(job)
            after = len(job)
            
            if after > before:
                records_with_missing += 1
        
        self.cleaning_stats['records_with_missing_values'] = records_with_missing
        logger.info(f"Handled missing values: {records_with_missing} records updated")
        return jobs
    
    def clean(self) -> List[Dict]:
        """
        Execute full cleaning pipeline
        
        Returns:
            List of cleaned job records
        """
        logger.info("=" * 50)
        logger.info("Starting data cleaning pipeline")
        logger.info("=" * 50)
        
        # Step 1: Validate
        jobs, invalid = self._validate_records(self.raw_jobs)
        self.cleaning_stats['invalid_records'] = invalid
        self.cleaning_stats['after_validation'] = len(jobs)
        
        # Step 2: Deduplicate
        jobs, duplicates = self._remove_duplicates(jobs)
        self.cleaning_stats['duplicates_removed'] = duplicates
        self.cleaning_stats['after_deduplication'] = len(jobs)
        
        # Step 3: Normalize text
        jobs = self._normalize_text_fields(jobs)
        
        # Step 4: Extract and normalize skills
        jobs = self._extract_and_normalize_skills(jobs)
        
        # Step 5: Handle missing values
        jobs = self._handle_missing_values(jobs)
        
        self.cleaned_jobs = jobs
        self.cleaning_stats['final_count'] = len(jobs)
        
        logger.info("=" * 50)
        logger.info("Data cleaning completed successfully")
        logger.info("=" * 50)
        
        self._print_stats()
        
        return self.cleaned_jobs
    
    def _print_stats(self):
        """Print cleaning statistics"""
        logger.info("Cleaning Statistics:")
        for key, value in self.cleaning_stats.items():
            logger.info(f"  {key}: {value}")
    
    def export_cleaned_data(self, output_filepath: str = None) -> str:
        """
        Export cleaned data to JSON file
        
        Args:
            output_filepath: Output file path (optional, auto-generated if not provided)
            
        Returns:
            Path to exported file
        """
        if output_filepath is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filepath = f"../data/cleaned/jobs_cleaned_{timestamp}.json"
        
        os.makedirs("../data/cleaned", exist_ok=True)
        
        try:
            with open(output_filepath, 'w', encoding='utf-8') as f:
                json.dump(self.cleaned_jobs, f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"Exported {len(self.cleaned_jobs)} cleaned jobs to {output_filepath}")
            return output_filepath
            
        except IOError as e:
            logger.error(f"Failed to export cleaned data: {e}")
            raise
    
    def get_stats(self) -> Dict:
        """Get cleaning statistics"""
        return self.cleaning_stats
