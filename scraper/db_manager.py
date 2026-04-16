"""
MongoDB connection and database operations
"""
import os
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from pymongo import MongoClient, ASCENDING, DESCENDING, errors
from pymongo.errors import ServerSelectionTimeoutError, DuplicateKeyError
from logger import logger
from db_models import JobModel, SkillModel, CompanyModel, IndexDefinition
from config import Config


class MongoConnection:
    """Manages MongoDB connection"""
    
    _client = None
    _db = None
    
    @classmethod
    def connect(cls, connection_string: str = None):
        """
        Establish MongoDB connection
        
        Args:
            connection_string: MongoDB connection URI
        """
        try:
            conn_str = connection_string or Config.MONGODB_URI
            
            if not conn_str or conn_str.startswith('mongodb+srv://<'):
                logger.warning("MongoDB URI not configured. Please set MONGODB_URI environment variable.")
                logger.info("Using local MongoDB simulation mode for testing")
                return None
            
            logger.info(f"Connecting to MongoDB...")
            cls._client = MongoClient(conn_str, serverSelectionTimeoutMS=5000)
            
            # Verify connection
            cls._client.admin.command('ping')
            cls._db = cls._client[Config.DB_NAME]
            
            logger.info(f"Connected to database: {Config.DB_NAME}")
            return cls._db
            
        except ServerSelectionTimeoutError:
            logger.warning("Cannot connect to MongoDB - server not available")
            logger.info("Service will use local mode for demonstration")
            return None
        except Exception as e:
            logger.warning(f"MongoDB connection failed: {e}")
            return None
    
    @classmethod
    def get_db(cls):
        """Get database instance"""
        if cls._db is None:
            cls.connect()
        return cls._db
    
    @classmethod
    def close(cls):
        """Close database connection"""
        if cls._client:
            cls._client.close()
            cls._db = None
            logger.info("MongoDB connection closed")


class DatabaseManager:
    """Handle all database operations"""
    
    def __init__(self):
        self.db = MongoConnection.get_db()
        self.local_storage = {}  # Fallback local storage
        self._ensure_indexes()
    
    def _ensure_indexes(self):
        """Create all required indexes"""
        if self.db is None:
            logger.info("Skipping index creation (local mode)")
            return
        
        try:
            jobs_collection = self.db[JobModel.COLLECTION_NAME]
            
            logger.info("Creating database indexes...")
            
            # Create jobs indexes
            for index_def in IndexDefinition.JOBS_INDEXES:
                try:
                    jobs_collection.create_index(
                        index_def["keys"],
                        name=index_def["name"],
                        **index_def["options"]
                    )
                    logger.debug(f"  Created index: {index_def['name']}")
                except Exception as e:
                    logger.warning(f"  Failed to create index {index_def['name']}: {e}")
            
            logger.info("Index creation completed")
            
        except Exception as e:
            logger.error(f"Failed to create indexes: {e}")
    
    def insert_jobs(self, jobs: List[Dict]) -> Tuple[int, int, List[str]]:
        """
        Insert or update jobs in database using upsert
        
        Args:
            jobs: List of job records
            
        Returns:
            Tuple of (inserted_count, updated_count, error_ids)
        """
        if not jobs:
            return 0, 0, []
        
        inserted_count = 0
        updated_count = 0
        error_ids = []
        
        logger.info(f"Inserting {len(jobs)} jobs into database...")
        
        if self.db is None:
            # Use local storage mode
            logger.info("Using local storage mode (MongoDB unavailable)")
            for job in jobs:
                if JobModel.validate(job):
                    job_id = job.get("jobId")
                    normalized = JobModel.normalize(job)
                    self.local_storage[job_id] = normalized
                    inserted_count += 1
                else:
                    error_ids.append(job.get("jobId", "unknown"))
            
            logger.info(f"Stored {inserted_count} jobs in local storage")
            return inserted_count, updated_count, error_ids
        
        # MongoDB mode
        try:
            jobs_collection = self.db[JobModel.COLLECTION_NAME]
            
            for job in jobs:
                try:
                    # Validate record
                    if not JobModel.validate(job):
                        error_ids.append(job.get("jobId", "unknown"))
                        logger.debug(f"Skipping invalid job: {job.get('jobId')}")
                        continue
                    
                    # Normalize for database
                    normalized_job = JobModel.normalize(job)
                    
                    # Generate filter for upsert
                    filter_doc = JobModel.get_upsert_filter(job)
                    
                    # Prepare update operation
                    update_doc = {
                        "$set": normalized_job
                    }
                    
                    # Perform upsert
                    result = jobs_collection.update_one(
                        filter_doc,
                        update_doc,
                        upsert=True
                    )
                    
                    if result.upserted_id:
                        inserted_count += 1
                    elif result.modified_count > 0:
                        updated_count += 1
                    
                except DuplicateKeyError:
                    # Already exists (handled by upsert)
                    updated_count += 1
                except Exception as e:
                    error_ids.append(job.get("jobId", "unknown"))
                    logger.warning(f"Failed to insert job {job.get('jobId')}: {e}")
            
            logger.info(f"Database insert complete: {inserted_count} new, {updated_count} updated, {len(error_ids)} errors")
            
        except Exception as e:
            logger.error(f"Database insertion failed: {e}")
        
        return inserted_count, updated_count, error_ids
    
    def get_job_count(self) -> int:
        """Get total job count in database"""
        if self.db is None:
            return len(self.local_storage)
        
        try:
            jobs_collection = self.db[JobModel.COLLECTION_NAME]
            return jobs_collection.count_documents({})
        except Exception as e:
            logger.error(f"Failed to get job count: {e}")
            return 0
    
    def get_jobs(self, limit: int = 10) -> List[Dict]:
        """Get sample jobs from database"""
        if self.db is None:
            return list(self.local_storage.values())[:limit]
        
        try:
            jobs_collection = self.db[JobModel.COLLECTION_NAME]
            return list(jobs_collection.find({}).limit(limit))
        except Exception as e:
            logger.error(f"Failed to retrieve jobs: {e}")
            return []
    
    def get_jobs_by_company(self, company: str, limit: int = 10) -> List[Dict]:
        """Get jobs by company"""
        if self.db is None:
            return [j for j in self.local_storage.values() 
                   if j.get("company", "").lower() == company.lower()][:limit]
        
        try:
            jobs_collection = self.db[JobModel.COLLECTION_NAME]
            return list(jobs_collection.find({"company": company}).limit(limit))
        except Exception as e:
            logger.error(f"Failed to query by company: {e}")
            return []
    
    def get_jobs_by_location(self, location: str, limit: int = 10) -> List[Dict]:
        """Get jobs by location"""
        if self.db is None:
            return [j for j in self.local_storage.values() 
                   if j.get("location", "").lower() == location.lower()][:limit]
        
        try:
            jobs_collection = self.db[JobModel.COLLECTION_NAME]
            return list(jobs_collection.find({"location": location}).limit(limit))
        except Exception as e:
            logger.error(f"Failed to query by location: {e}")
            return []
    
    def get_jobs_by_skill(self, skill: str, limit: int = 10) -> List[Dict]:
        """Get jobs requiring a specific skill"""
        if self.db is None:
            return [j for j in self.local_storage.values() 
                   if skill.lower() in [s.lower() for s in j.get("skills", [])]][:limit]
        
        try:
            jobs_collection = self.db[JobModel.COLLECTION_NAME]
            return list(jobs_collection.find({"skills": skill}).limit(limit))
        except Exception as e:
            logger.error(f"Failed to query by skill: {e}")
            return []
    
    def get_top_skills(self, limit: int = 10) -> List[Tuple[str, int]]:
        """Get top N skills from all jobs"""
        from collections import Counter
        
        if self.db is None:
            # Local mode: count from local storage
            skill_counter = Counter()
            for job in self.local_storage.values():
                skill_counter.update(job.get("skills", []))
            return skill_counter.most_common(limit)
        
        try:
            jobs_collection = self.db[JobModel.COLLECTION_NAME]
            pipeline = [
                {"$unwind": "$skills"},
                {"$group": {"_id": "$skills", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": limit}
            ]
            results = list(jobs_collection.aggregate(pipeline))
            return [(r["_id"], r["count"]) for r in results]
        except Exception as e:
            logger.error(f"Failed to get top skills: {e}")
            return []
    
    def get_top_companies(self, limit: int = 10) -> List[Tuple[str, int]]:
        """Get top N companies by job count"""
        if self.db is None:
            from collections import Counter
            company_counter = Counter(j.get("company") for j in self.local_storage.values())
            return company_counter.most_common(limit)
        
        try:
            jobs_collection = self.db[JobModel.COLLECTION_NAME]
            pipeline = [
                {"$group": {"_id": "$company", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": limit}
            ]
            results = list(jobs_collection.aggregate(pipeline))
            return [(r["_id"], r["count"]) for r in results]
        except Exception as e:
            logger.error(f"Failed to get top companies: {e}")
            return []
    
    def get_database_stats(self) -> Dict:
        """Get comprehensive database statistics"""
        stats = {
            "mode": "MongoDB" if self.db else "Local Storage",
            "total_jobs": self.get_job_count(),
            "top_skills": self.get_top_skills(5),
            "top_companies": self.get_top_companies(5),
            "timestamp": datetime.utcnow().isoformat()
        }
        return stats
