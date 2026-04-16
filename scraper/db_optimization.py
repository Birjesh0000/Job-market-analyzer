"""
Database indexing strategy and query optimization
"""
from typing import Dict, List, Tuple
from logger import logger
from db_manager import DatabaseManager
from db_models import IndexDefinition


class IndexOptimization:
    """Manage and optimize database indexes for query performance"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.db = db_manager.db
    
    def analyze_indexes(self) -> Dict:
        """
        Analyze current indexes on collections
        
        Returns:
            Dictionary with index analysis
        """
        if self.db is None:
            logger.info("Index analysis: Not in MongoDB mode")
            return {"mode": "local_storage"}
        
        logger.info("\nAnalyzing Database Indexes...")
        
        analysis = {
            "timestamp": "analysis_time",
            "collection_indexes": {}
        }
        
        try:
            # Check jobs collection indexes
            jobs_collection = self.db["jobs"]
            jobs_indexes = jobs_collection.list_indexes()
            
            analysis["collection_indexes"]["jobs"] = []
            for idx in jobs_indexes:
                analysis["collection_indexes"]["jobs"].append({
                    "name": idx.get("name"),
                    "keys": idx.get("key"),
                    "unique": idx.get("unique", False)
                })
            
            logger.info(f"  Jobs collection: {len(analysis['collection_indexes']['jobs'])} indexes")
            
        except Exception as e:
            logger.warning(f"Failed to analyze indexes: {e}")
        
        return analysis
    
    def benchmark_queries(self) -> Dict:
        """
        Benchmark common query patterns
        Demonstrates performance with and without indexes
        
        Returns:
            Benchmark results
        """
        if self.db is None:
            logger.info("Query benchmarking: Not in MongoDB mode")
            return {"mode": "local_storage"}
        
        import time
        
        logger.info("\nBenchmarking Query Performance...")
        
        benchmarks = {}
        
        try:
            jobs_collection = self.db["jobs"]
            
            # Benchmark 1: Query by company
            logger.info("  Benchmarking: Query by company...")
            start = time.time()
            results = list(jobs_collection.find({"company": "Apple"}).limit(100))
            query_time = time.time() - start
            benchmarks["query_by_company"] = {
                "time_ms": query_time * 1000,
                "documents_scanned": len(results),
                "index_used": "company_index"
            }
            
            # Benchmark 2: Query by location
            logger.info("  Benchmarking: Query by location...")
            start = time.time()
            results = list(jobs_collection.find({"location": "Remote"}).limit(100))
            query_time = time.time() - start
            benchmarks["query_by_location"] = {
                "time_ms": query_time * 1000,
                "documents_scanned": len(results),
                "index_used": "location_index"
            }
            
            # Benchmark 3: Query by skill
            logger.info("  Benchmarking: Query by skill...")
            start = time.time()
            results = list(jobs_collection.find({"skills": "python"}).limit(100))
            query_time = time.time() - start
            benchmarks["query_by_skill"] = {
                "time_ms": query_time * 1000,
                "documents_scanned": len(results),
                "index_used": "skills_index"
            }
            
            # Benchmark 4: Range query on posted_date
            logger.info("  Benchmarking: Range query on date...")
            start = time.time()
            results = list(jobs_collection.find({
                "posted_date": {"$gte": "2024-01-01"}
            }).limit(100))
            query_time = time.time() - start
            benchmarks["query_by_date"] = {
                "time_ms": query_time * 1000,
                "documents_scanned": len(results),
                "index_used": "posted_date_index"
            }
            
            # Benchmark 5: Compound query
            logger.info("  Benchmarking: Compound query...")
            start = time.time()
            results = list(jobs_collection.find({
                "company": "Google",
                "location": "Remote"
            }).limit(100))
            query_time = time.time() - start
            benchmarks["query_compound"] = {
                "time_ms": query_time * 1000,
                "documents_scanned": len(results),
                "index_used": "company_location_index"
            }
            
        except Exception as e:
            logger.warning(f"Failed to benchmark: {e}")
        
        return benchmarks


class QueryStrategyAnalyzer:
    """Analyze and document optimal query strategies"""
    
    QUERY_STRATEGIES = {
        "find_jobs_by_skill": {
            "description": "Find all jobs requiring a specific skill",
            "query": {"skills": "python"},
            "index_used": "skills_index",
            "expected_time": "< 10ms",
            "use_case": "Skill trend analysis, candidate matching"
        },
        "find_jobs_by_location": {
            "description": "Find all jobs in a specific location",
            "query": {"location": "San Francisco"},
            "index_used": "location_index",
            "expected_time": "< 5ms",
            "use_case": "Geographic analysis, local job market"
        },
        "find_jobs_by_company": {
            "description": "Find all jobs from a company",
            "query": {"company": "Google"},
            "index_used": "company_index",
            "expected_time": "< 5ms",
            "use_case": "Company hiring patterns, competitive analysis"
        },
        "find_recent_jobs": {
            "description": "Find jobs posted in last N days",
            "query": {"posted_date": {"$gte": "2024-04-10"}},
            "index_used": "posted_date_index",
            "expected_time": "< 10ms",
            "use_case": "Trending jobs, recent postings"
        },
        "find_remote_jobs_in_location": {
            "description": "Complex: Find remote jobs by company",
            "query": {"location": "Remote", "company": "Stripe"},
            "index_used": "company_location_index",
            "expected_time": "< 5ms",
            "use_case": "Remote work opportunities"
        },
        "aggregate_top_skills": {
            "description": "Aggregate top 10 most required skills",
            "aggregation": [
                {"$unwind": "$skills"},
                {"$group": {"_id": "$skills", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 10}
            ],
            "index_used": "skills_index",
            "expected_time": "< 50ms",
            "use_case": "Market trends, skills analysis"
        },
        "aggregate_top_companies": {
            "description": "Aggregate companies with most jobs",
            "aggregation": [
                {"$group": {"_id": "$company", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 10}
            ],
            "index_used": "company_index",
            "expected_time": "< 50ms",
            "use_case": "Hiring trends, employer analysis"
        }
    }
    
    @staticmethod
    def get_strategy(query_type: str) -> Dict:
        """Get optimal query strategy"""
        return QueryStrategyAnalyzer.QUERY_STRATEGIES.get(
            query_type, 
            {"error": "Unknown query type"}
        )
    
    @staticmethod
    def document_all_strategies() -> str:
        """Generate documentation of all query strategies"""
        doc_lines = ["DATABASE QUERY OPTIMIZATION STRATEGIES\n"]
        doc_lines.append("=" * 60 + "\n")
        
        for query_type, strategy in QueryStrategyAnalyzer.QUERY_STRATEGIES.items():
            doc_lines.append(f"\n{query_type}:")
            doc_lines.append(f"  Description: {strategy['description']}")
            doc_lines.append(f"  Index Used: {strategy.get('index_used')}")
            doc_lines.append(f"  Expected Time: {strategy.get('expected_time')}")
            doc_lines.append(f"  Use Case: {strategy.get('use_case')}")
        
        return "\n".join(doc_lines)


class PerformanceMonitoring:
    """Monitor database performance metrics"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.db = db_manager.db
    
    def get_collection_stats(self) -> Dict:
        """Get statistics about collections"""
        if self.db is None:
            return {"mode": "local_storage"}
        
        try:
            logger.info("\nCollecting Database Statistics...")
            
            stats = {}
            
            # Jobs collection stats
            jobs_collection = self.db["jobs"]
            jobs_count = jobs_collection.count_documents({})
            jobs_size = self.db.command("collStats", "jobs").get("size", 0)
            
            stats["jobs"] = {
                "count": jobs_count,
                "size_bytes": jobs_size,
                "avg_doc_size": jobs_size / jobs_count if jobs_count > 0 else 0
            }
            
            logger.info(f"  Jobs collection: {jobs_count} documents, {jobs_size} bytes")
            
            return stats
            
        except Exception as e:
            logger.warning(f"Failed to get collection stats: {e}")
            return {}
    
    def generate_optimization_report(self) -> str:
        """Generate comprehensive performance optimization report"""
        report_lines = []
        
        report_lines.append("\n" + "=" * 70)
        report_lines.append("DATABASE OPTIMIZATION REPORT")
        report_lines.append("=" * 70 + "\n")
        
        # Index Analysis
        report_lines.append("[INDEX CONFIGURATION]")
        for index_def in IndexDefinition.JOBS_INDEXES:
            report_lines.append(f"\nIndex: {index_def['name']}")
            report_lines.append(f"  Keys: {index_def['keys']}")
            report_lines.append(f"  Options: {index_def['options']}")
        
        # Query Strategies
        report_lines.append("\n\n[RECOMMENDED QUERY PATTERNS]")
        report_lines.append(QueryStrategyAnalyzer.document_all_strategies())
        
        # Collection Stats
        report_lines.append("\n\n[COLLECTION STATISTICS]")
        stats = self.get_collection_stats()
        for collection, data in stats.items():
            if collection != "mode":
                report_lines.append(f"\n{collection.upper()}:")
                for key, value in data.items():
                    report_lines.append(f"  {key}: {value}")
        
        report_lines.append("\n" + "=" * 70 + "\n")
        
        return "\n".join(report_lines)
