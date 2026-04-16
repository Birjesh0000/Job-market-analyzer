"""
Data transformation and normalization functions
"""
import re
import pandas as pd
from typing import List, Dict, Set

class SkillNormalizer:
    """Normalize and standardize skill names"""
    
    # Skill mappings for normalization
    SKILL_MAPPINGS = {
        # JavaScript/Node variations
        'javascript': ['javascript', 'js', 'ecmascript', 'es6', 'es5'],
        'typescript': ['typescript', 'ts'],
        'nodejs': ['node.js', 'node js', 'nodejs', 'node', 'expressjs', 'express'],
        'react': ['react.js', 'react js', 'reactjs', 'react'],
        'vue': ['vue.js', 'vue js', 'vuejs', 'vue'],
        'angular': ['angular.js', 'angular js', 'angularjs', 'angular'],
        
        # Python
        'python': ['python', 'python3', 'py'],
        'django': ['django', 'django rest', 'drf'],
        'flask': ['flask', 'flask-restful'],
        'fastapi': ['fastapi', 'fast api'],
        'pandas': ['pandas', 'pd'],
        'numpy': ['numpy', 'np'],
        'tensorflow': ['tensorflow', 'tf'],
        'pytorch': ['pytorch', 'torch'],
        'scikit-learn': ['scikit-learn', 'sklearn', 'scikit learn'],
        
        # Databases
        'postgresql': ['postgresql', 'postgres', 'psql', 'pg'],
        'mysql': ['mysql', 'mariadb'],
        'mongodb': ['mongodb', 'mongo', 'nosql'],
        'redis': ['redis', 'memcached'],
        'elasticsearch': ['elasticsearch', 'elastic search'],
        'sql': ['sql', 't-sql', 'plsql'],
        
        # DevOps/Cloud
        'docker': ['docker', 'dockerfile', 'dockerization'],
        'kubernetes': ['kubernetes', 'k8s', 'k8'],
        'aws': ['aws', 'amazon web services', 'amazon aws'],
        'gcp': ['gcp', 'google cloud', 'google cloud platform'],
        'azure': ['azure', 'microsoft azure'],
        'kubernetes': ['kubernetes', 'k8s'],
        
        # Frontend
        'html': ['html', 'html5'],
        'css': ['css', 'css3', 'scss', 'sass'],
        'tailwind': ['tailwind', 'tailwind css'],
        'bootstrap': ['bootstrap'],
        'webpack': ['webpack', 'bundler'],
        
        # Testing
        'selenium': ['selenium', 'webdriver'],
        'cypress': ['cypress', 'e2e testing'],
        'jest': ['jest', 'unit testing'],
        'pytest': ['pytest', 'py test'],
        'junit': ['junit', 'testng'],
        
        # Other
        'git': ['git', 'github', 'gitlab', 'bitbucket'],
        'rest': ['rest api', 'restful', 'rest'],
        'graphql': ['graphql', 'apollo'],
        'microservices': ['microservices', 'microservice'],
        'agile': ['agile', 'scrum', 'kanban'],
        'ci/cd': ['ci/cd', 'cicd', 'continuous integration', 'continuous deployment'],
        'linux': ['linux', 'unix', 'bash dash shell'],
        'windows': ['windows', 'powershell'],
    }
    
    @classmethod
    def normalize_skill(cls, skill: str) -> str:
        """
        Normalize a single skill name
        
        Args:
            skill: Raw skill name
            
        Returns:
            Normalized skill name
        """
        if not skill or not isinstance(skill, str):
            return ''
        
        # Clean: trim, lowercase, remove extra spaces
        cleaned = skill.strip().lower()
        cleaned = re.sub(r'\s+', ' ', cleaned)
        
        # Check against mappings
        for normalized, variations in cls.SKILL_MAPPINGS.items():
            if cleaned in variations:
                return normalized
        
        # If no match, return cleaned version
        # Remove common suffixes that don't add value
        cleaned = re.sub(r'\s*(framework|library|platform|tool|language)$', '', cleaned)
        
        return cleaned.strip()
    
    @classmethod
    def normalize_skills_list(cls, skills_str: str) -> List[str]:
        """
        Parse and normalize a skills string from job description
        
        Args:
            skills_str: Comma or space separated skills string
            
        Returns:
            List of normalized unique skills
        """
        if not skills_str or not isinstance(skills_str, str):
            return []
        
        # Split by common delimiters
        parts = re.split(r'[,;/\n]', skills_str)
        
        normalized_skills = set()
        for part in parts:
            skill = cls.normalize_skill(part.strip())
            if skill:
                normalized_skills.add(skill)
        
        return sorted(list(normalized_skills))


class TextCleaner:
    """Clean and standardize text fields"""
    
    @staticmethod
    def clean_text(text: str, max_length: int = None) -> str:
        """
        Clean and standardize text
        
        Args:
            text: Text to clean
            max_length: Maximum length to truncate to
            
        Returns:
            Cleaned text
        """
        if not text or not isinstance(text, str):
            return ''
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text.strip())
        
        # Truncate if needed
        if max_length and len(text) > max_length:
            text = text[:max_length].rsplit(' ', 1)[0] + '...'
        
        return text
    
    @staticmethod
    def clean_company_name(company: str) -> str:
        """
        Standardize company names
        
        Args:
            company: Company name
            
        Returns:
            Standardized company name
        """
        if not company or not isinstance(company, str):
            return ''
        
        # Remove extra spaces
        company = company.strip().title()
        
        # Remove common suffixes that vary
        company = re.sub(r'\s+(Inc|Ltd|LLC|Corp|Company|Corporation|Co\.?)$', '', company, flags=re.I)
        
        return company
    
    @staticmethod
    def clean_location(location: str) -> str:
        """
        Standardize location names
        
        Args:
            location: Location string
            
        Returns:
            Standardized location
        """
        if not location or not isinstance(location, str):
            return ''
        
        location = location.strip()
        
        # Normalize "Remote" variations
        if location.lower() in ['remote', 'remote work', 'work from home', 'wfh', 'distributed']:
            return 'Remote'
        
        # Standardize city, state format
        location = re.sub(r',\s+', ', ', location)
        
        return location


class DuplicateDetector:
    """Detect and handle duplicate records"""
    
    @staticmethod
    def get_duplicate_key(job: Dict) -> tuple:
        """
        Generate a deduplication key from job record
        
        Args:
            job: Job record dictionary
            
        Returns:
            Tuple of (title, company, location) for deduplication
        """
        title = job.get('title', '').strip().lower()
        company = job.get('company', '').strip().lower()
        location = job.get('location', '').strip().lower()
        
        return (title, company, location)
    
    @staticmethod
    def remove_duplicates(jobs: List[Dict]) -> tuple:
        """
        Remove duplicate job records
        
        Args:
            jobs: List of job dictionaries
            
        Returns:
            Tuple of (unique_jobs, duplicate_count)
        """
        seen = set()
        unique_jobs = []
        duplicates_removed = 0
        
        for job in jobs:
            key = DuplicateDetector.get_duplicate_key(job)
            
            # Skip if we've seen this combination before
            if key not in seen:
                seen.add(key)
                unique_jobs.append(job)
            else:
                duplicates_removed += 1
        
        return unique_jobs, duplicates_removed


class MissingValueHandler:
    """Handle missing values in data"""
    
    @staticmethod
    def fill_missing_fields(job: Dict) -> Dict:
        """
        Fill missing fields with defaults or derived values
        
        Args:
            job: Job record dictionary
            
        Returns:
            Job record with filled missing values
        """
        # Set defaults for optional fields
        if not job.get('type'):
            job['type'] = 'Unknown'
        
        if not job.get('description'):
            job['description'] = ''
        
        if not job.get('salary_min'):
            job['salary_min'] = None
        
        if not job.get('salary_max'):
            job['salary_max'] = None
        
        if not job.get('currency'):
            job['currency'] = 'USD'
        
        return job
    
    @staticmethod
    def is_record_valid(job: Dict) -> bool:
        """
        Check if record has minimum required fields
        
        Args:
            job: Job record dictionary
            
        Returns:
            True if record is valid, False otherwise
        """
        required_fields = ['id', 'title', 'company', 'location', 'url']
        
        for field in required_fields:
            if field not in job or not job[field]:
                return False
        
        return True
