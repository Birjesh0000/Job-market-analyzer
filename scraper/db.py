"""
Database connection and initialization module
"""
import os
from pymongo import MongoClient
from config import Config

class Database:
    _client = None
    _db = None
    
    @classmethod
    def connect(cls):
        """Initialize database connection"""
        try:
            cls._client = MongoClient(Config.MONGODB_URI)
            cls._db = cls._client[Config.DB_NAME]
            print(f"Connected to database: {Config.DB_NAME}")
            return cls._db
        except Exception as e:
            print(f"Database connection error: {e}")
            raise
    
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
            print("Database connection closed")
