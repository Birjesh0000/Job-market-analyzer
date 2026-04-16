"""
Advanced analytics for cleaned data
"""
import json
from typing import List, Dict, Tuple
from collections import Counter
from logger import logger


class SkillsAnalyzer:
    """Analyze normalized skills from cleaned data"""
    
    @staticmethod
    def get_top_skills(jobs: List[Dict], top_n: int = 10) -> List[Tuple[str, int]]:
        """
        Get top N most frequently required skills
        
        Args:
            jobs: List of cleaned job records
            top_n: Number of top skills to return
            
        Returns:
            List of (skill, count) tuples sorted by frequency
        """
        skill_counter = Counter()
        
        for job in jobs:
            skills = job.get('skills', [])
            if isinstance(skills, list):
                skill_counter.update(skills)
        
        return skill_counter.most_common(top_n)
    
    @staticmethod
    def get_skill_stats(jobs: List[Dict]) -> Dict:
        """
        Get comprehensive skill statistics
        
        Args:
            jobs: List of cleaned job records
            
        Returns:
            Dictionary with skill statistics
        """
        all_skills = []
        jobs_with_skills = 0
        
        for job in jobs:
            skills = job.get('skills', [])
            if isinstance(skills, list) and len(skills) > 0:
                jobs_with_skills += 1
                all_skills.extend(skills)
        
        unique_skills = len(set(all_skills))
        
        return {
            'total_unique_skills': unique_skills,
            'jobs_with_skills': jobs_with_skills,
            'avg_skills_per_job': len(all_skills) / len(jobs) if jobs else 0,
            'total_skill_requirements': len(all_skills)
        }


class LocationAnalyzer:
    """Analyze job locations"""
    
    @staticmethod
    def get_top_locations(jobs: List[Dict], top_n: int = 10) -> List[Tuple[str, int]]:
        """
        Get top N locations with most job postings
        
        Args:
            jobs: List of cleaned job records
            top_n: Number of top locations to return
            
        Returns:
            List of (location, count) tuples sorted by frequency
        """
        location_counter = Counter()
        
        for job in jobs:
            location = job.get('location', 'Unknown')
            if location:
                location_counter[location] += 1
        
        return location_counter.most_common(top_n)
    
    @staticmethod
    def count_remote_jobs(jobs: List[Dict]) -> int:
        """Count jobs that are remote"""
        return sum(1 for job in jobs if job.get('location', '').lower() == 'remote')


class CompanyAnalyzer:
    """Analyze company data"""
    
    @staticmethod
    def get_top_companies(jobs: List[Dict], top_n: int = 10) -> List[Tuple[str, int]]:
        """
        Get top N companies with most job postings
        
        Args:
            jobs: List of cleaned job records
            top_n: Number of top companies to return
            
        Returns:
            List of (company, count) tuples sorted by frequency
        """
        company_counter = Counter()
        
        for job in jobs:
            company = job.get('company', 'Unknown')
            if company:
                company_counter[company] += 1
        
        return company_counter.most_common(top_n)


class DeduplicationAnalyzer:
    """Analyze deduplication effectiveness"""
    
    @staticmethod
    def analyze_deduplication(original_count: int, final_count: int) -> Dict:
        """
        Analyze deduplication statistics
        
        Args:
            original_count: Number of records before deduplication
            final_count: Number of records after deduplication
            
        Returns:
            Dictionary with deduplication statistics
        """
        duplicates_removed = original_count - final_count
        dedup_percentage = (duplicates_removed / original_count * 100) if original_count > 0 else 0
        
        return {
            'original_count': original_count,
            'final_count': final_count,
            'duplicates_removed': duplicates_removed,
            'dedup_percentage': round(dedup_percentage, 2)
        }


class QualityAnalyzer:
    """Analyze data quality metrics"""
    
    @staticmethod
    def analyze_data_quality(jobs: List[Dict]) -> Dict:
        """
        Analyze overall data quality
        
        Args:
            jobs: List of cleaned job records
            
        Returns:
            Dictionary with quality metrics
        """
        required_fields = ['id', 'title', 'company', 'location', 'url']
        
        # Count records with all required fields
        complete_records = 0
        records_with_skills = 0
        records_with_description = 0
        
        for job in jobs:
            # Check required fields
            if all(job.get(field) for field in required_fields):
                complete_records += 1
            
            # Check optional fields
            if job.get('skills') and len(job.get('skills', [])) > 0:
                records_with_skills += 1
            
            if job.get('description') and len(job.get('description', '').strip()) > 0:
                records_with_description += 1
        
        completeness_percentage = (complete_records / len(jobs) * 100) if jobs else 0
        
        return {
            'total_records': len(jobs),
            'complete_records': complete_records,
            'completeness_percentage': round(completeness_percentage, 2),
            'records_with_skills': records_with_skills,
            'records_with_description': records_with_description,
            'avg_record_fields': sum(len(job) for job in jobs) / len(jobs) if jobs else 0
        }


def generate_analysis_report(cleaned_jobs: List[Dict], cleaning_stats: Dict) -> str:
    """
    Generate comprehensive analysis report
    
    Args:
        cleaned_jobs: List of cleaned job records
        cleaning_stats: Statistics from cleaning process
        
    Returns:
        Formatted analysis report string
    """
    report_lines = []
    
    report_lines.append("\n" + "=" * 60)
    report_lines.append("DATA QUALITY & ANALYSIS REPORT")
    report_lines.append("=" * 60 + "\n")
    
    # Deduplication Analysis
    dedup_stats = DeduplicationAnalyzer.analyze_deduplication(
        cleaning_stats.get('initial_count', 0),
        cleaning_stats.get('final_count', 0)
    )
    report_lines.append("[Deduplication Analysis]")
    for key, value in dedup_stats.items():
        report_lines.append(f"  {key}: {value}")
    
    # Data Quality
    quality_stats = QualityAnalyzer.analyze_data_quality(cleaned_jobs)
    report_lines.append("\n[Data Quality Metrics]")
    for key, value in quality_stats.items():
        if isinstance(value, float):
            report_lines.append(f"  {key}: {value:.2f}")
        else:
            report_lines.append(f"  {key}: {value}")
    
    # Skills Analysis
    skill_stats = SkillsAnalyzer.get_skill_stats(cleaned_jobs)
    top_skills = SkillsAnalyzer.get_top_skills(cleaned_jobs, top_n=5)
    report_lines.append("\n[Skills Analysis]")
    for key, value in skill_stats.items():
        if isinstance(value, float):
            report_lines.append(f"  {key}: {value:.2f}")
        else:
            report_lines.append(f"  {key}: {value}")
    
    report_lines.append("\n  Top 5 Skills:")
    for skill, count in top_skills:
        report_lines.append(f"    - {skill}: {count} occurrences")
    
    # Location Analysis
    top_locations = LocationAnalyzer.get_top_locations(cleaned_jobs, top_n=5)
    remote_count = LocationAnalyzer.count_remote_jobs(cleaned_jobs)
    report_lines.append("\n[Location Analysis]")
    report_lines.append(f"  Remote jobs: {remote_count}")
    report_lines.append("\n  Top 5 Locations:")
    for location, count in top_locations:
        report_lines.append(f"    - {location}: {count} jobs")
    
    # Company Analysis
    top_companies = CompanyAnalyzer.get_top_companies(cleaned_jobs, top_n=5)
    report_lines.append("\n[Company Analysis]")
    report_lines.append(f"  Unique companies: {len(set(j.get('company', '') for j in cleaned_jobs))}")
    report_lines.append("\n  Top 5 Companies:")
    for company, count in top_companies:
        report_lines.append(f"    - {company}: {count} jobs")
    
    report_lines.append("\n" + "=" * 60 + "\n")
    
    return "\n".join(report_lines)
