"""
MongoDB models and schema definitions
"""
from datetime import datetime
from typing import List, Dict, Optional


class JobModel:
    """
    MongoDB Job Document Model
    
    Schema structure for jobs collection:
    {
        "_id": ObjectId,                   # MongoDB auto-generated
        "jobId": "unique-job-id",          # Unique identifier from source
        "title": "Job Title",              # Normalized job title
        "company": "Company Name",         # Normalized company name
        "location": "City, State",         # Normalized location
        "url": "https://...",              # Link to original posting
        "description": "Full description", # Job description
        "skills": ["python", "django"],    # Normalized skill list
        "jobType": "Full-time",            # Employment type
        "salary_min": 80000,               # Optional minimum salary
        "salary_max": 120000,              # Optional maximum salary
        "currency": "USD",                 # Salary currency
        "posted_date": datetime,           # When job was posted
        "scraped_date": datetime,          # When data was scraped
        "source": "github_jobs",           # Data source
        "is_active": True,                 # Whether still active
        "created_at": datetime,            # When record created in DB
        "updated_at": datetime             # Last update timestamp
    }
    """
    
    COLLECTION_NAME = "jobs"
    
    # Fields that form unique constraint
    DEDUP_FIELDS = ["title", "company", "location"]
    
    # Required fields for a valid job record
    REQUIRED_FIELDS = ["title", "company", "location", "url"]
    
    # Optional fields with defaults
    OPTIONAL_FIELDS = {
        "description": "",
        "skills": [],
        "jobType": "Unknown",
        "salary_min": None,
        "salary_max": None,
        "currency": "USD",
        "posted_date": None,
        "source": "unknown",
        "is_active": True
    }
    
    @staticmethod
    def validate(job: Dict) -> bool:
        """Validate job record has required fields"""
        # Check for id or jobId (at least one must exist)
        if not job.get("jobId") and not job.get("id"):
            return False
        
        # Check other required fields
        for field in JobModel.REQUIRED_FIELDS:
            if field not in job or not job[field]:
                return False
        return True
    
    @staticmethod
    def normalize(job: Dict) -> Dict:
        """
        Normalize and prepare job record for database insertion
        
        Args:
            job: Raw job dictionary
            
        Returns:
            Normalized job record with all fields
        """
        # Handle both "id" and "jobId" field names
        job_id = job.get("jobId") or job.get("id", "")
        
        normalized = {
            "jobId": job_id,
            "title": job.get("title", "").strip(),
            "company": job.get("company", "").strip(),
            "location": job.get("location", "").strip(),
            "url": job.get("url", "").strip(),
            "description": job.get("description", "").strip(),
            "skills": job.get("skills", []) if isinstance(job.get("skills"), list) else [],
            "jobType": job.get("type", job.get("jobType", "Unknown")),
            "salary_min": job.get("salary_min"),
            "salary_max": job.get("salary_max"),
            "currency": job.get("currency", "USD"),
            "posted_date": job.get("posted_date") or job.get("posted_at"),
            "source": job.get("source", "unknown"),
            "is_active": True,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }
        return normalized
    
    @staticmethod
    def get_upsert_filter(job: Dict) -> Dict:
        """
        Generate MongoDB filter for upsert operation
        Uses jobId as primary key, falls back to title+company+location
        
        Args:
            job: Job dictionary
            
        Returns:
            MongoDB filter dictionary
        """
        job_id = job.get("jobId")
        
        if job_id:
            return {"jobId": job_id}
        else:
            # Fallback to content-based deduplication
            return {
                "title": job.get("title", "").lower().strip(),
                "company": job.get("company", "").lower().strip(),
                "location": job.get("location", "").lower().strip()
            }


class SkillModel:
    """
    MongoDB Skills Collection Model
    Aggregated data for skill analytics
    """
    
    COLLECTION_NAME = "skills"
    
    SCHEMA = {
        "_id": "ObjectId",
        "skillName": "string (unique index)",
        "frequency": "integer (documents with this skill)",
        "trend": "array of {date, count}",
        "lastUpdated": "datetime"
    }
    
    @staticmethod
    def create_skill_aggregation(skill_name: str, frequency: int) -> Dict:
        """Create skill aggregation record"""
        return {
            "skillName": skill_name.lower(),
            "frequency": frequency,
            "lastUpdated": datetime.utcnow()
        }


class CompanyModel:
    """
    MongoDB Companies Collection Model
    Aggregated data for company analytics
    """
    
    COLLECTION_NAME = "companies"
    
    SCHEMA = {
        "_id": "ObjectId",
        "companyName": "string (unique index)",
        "jobCount": "integer",
        "locations": "array of strings",
        "skills": "array of strings",
        "lastUpdated": "datetime"
    }
    
    @staticmethod
    def create_company_aggregation(company_name: str, job_count: int, locations: List[str] = None) -> Dict:
        """Create company aggregation record"""
        return {
            "companyName": company_name,
            "jobCount": job_count,
            "locations": locations or [],
            "lastUpdated": datetime.utcnow()
        }


class IndexDefinition:
    """
    MongoDB Index Definitions
    Specifies all indexes for optimal query performance
    """
    
    # Jobs collection indexes
    JOBS_INDEXES = [
        # Primary indexes
        {
            "name": "jobId_unique",
            "keys": [("jobId", 1)],
            "options": {"unique": True, "sparse": True}
        },
        # Query performance indexes
        {
            "name": "company_index",
            "keys": [("company", 1)],
            "options": {"background": True}
        },
        {
            "name": "location_index",
            "keys": [("location", 1)],
            "options": {"background": True}
        },
        {
            "name": "skills_index",
            "keys": [("skills", 1)],
            "options": {"background": True}
        },
        # Date range queries
        {
            "name": "posted_date_index",
            "keys": [("posted_date", -1)],
            "options": {"background": True}
        },
        # Compound indexes for common queries
        {
            "name": "company_location_index",
            "keys": [("company", 1), ("location", 1)],
            "options": {"background": True}
        },
        {
            "name": "location_posted_date_index",
            "keys": [("location", 1), ("posted_date", -1)],
            "options": {"background": True}
        },
        # Active jobs queries
        {
            "name": "active_index",
            "keys": [("is_active", 1)],
            "options": {"background": True}
        }
    ]
    
    # Skills collection indexes
    SKILLS_INDEXES = [
        {
            "name": "skillName_unique",
            "keys": [("skillName", 1)],
            "options": {"unique": True}
        },
        {
            "name": "frequency_index",
            "keys": [("frequency", -1)],
            "options": {"background": True}
        }
    ]
    
    # Companies collection indexes
    COMPANIES_INDEXES = [
        {
            "name": "companyName_unique",
            "keys": [("companyName", 1)],
            "options": {"unique": True}
        },
        {
            "name": "jobCount_index",
            "keys": [("jobCount", -1)],
            "options": {"background": True}
        }
    ]
