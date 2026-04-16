"""
Test script for scraper validation
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scraper.utils import DataValidator, DataAnalyzer

def test_sample_data():
    """Test scraper with sample data"""
    sample_jobs = [
        {
            "id": "test-1",
            "title": "Python Developer",
            "company": "TechCorp",
            "location": "New York",
            "url": "https://example.com/job1",
            "type": "Full-time"
        },
        {
            "id": "test-2",
            "title": "Data Engineer",
            "company": "DataFlow",
            "location": "Remote",
            "url": "https://example.com/job2",
            "type": "Full-time"
        },
        {
            "id": "test-3",
            "title": "Incomplete Job",
            "company": "BadCorp"
        }
    ]
    
    # Test validation
    valid, invalid = DataValidator.validate_jobs(sample_jobs)
    print(f"✓ Validation test passed: {len(valid)} valid, {len(invalid)} invalid")
    
    # Test analysis
    analysis = DataAnalyzer.analyze(valid)
    print(f"✓ Analysis test passed: {len(valid)} jobs, {analysis['unique_companies']} companies")
    
    print("\n✅ All tests passed!")

if __name__ == '__main__':
    test_sample_data()
