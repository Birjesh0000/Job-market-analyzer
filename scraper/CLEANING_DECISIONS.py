"""
Data cleaning decisions and validation rules
This module documents all data quality decisions made during the cleaning process
"""

CLEANING_DECISIONS = {
    "deduplication": {
        "description": "Remove duplicate job records",
        "key_used": ["title", "company", "location"],
        "reason": "Even slight variations (San Francisco vs San Francisco, CA) are treated as duplicates if other fields match",
        "example": "Senior Python Developer / TechCorp Inc / San Francisco (title+company+location based dedup)"
    },
    
    "missing_values": {
        "description": "Strategy for handling missing fields",
        "rules": {
            "type": "Default to 'Unknown' if missing",
            "description": "Keep empty string if missing",
            "salary_min": "Set to None if missing",
            "salary_max": "Set to None if missing",
            "currency": "Default to 'USD' if missing",
            "skills": "Extract from title and description, default to empty list"
        },
        "reason": "Prevents NULL values while preserving data integrity"
    },
    
    "text_normalization": {
        "description": "Standardize text fields",
        "rules": {
            "company_name": "Title case, remove Inc/Ltd/LLC/Corp suffixes (reduces company duplicates)",
            "location": "Normalize 'Remote', 'work from home' → 'Remote'",
            "title": "Trim whitespace, normalize spaces",
            "description": "Max 1000 chars, trim extra whitespace"
        },
        "reason": "Ensures consistency and improves data quality"
    },
    
    "skill_normalization": {
        "description": "Standardize technology skill names",
        "mappings": {
            "nodejs": ["node.js", "node js", "nodejs", "express"],
            "react": ["react.js", "react js", "reactjs"],
            "kubernetes": ["kubernetes", "k8s"],
            "python": ["python", "python3", "py"],
            "docker": ["docker"]
        },
        "strategy": "Use exact match mapping, fallback to cleaned version",
        "reason": "Job descriptions use inconsistent naming (React.js vs react vs ReactJS)"
    },
    
    "record_validation": {
        "description": "Required fields for a valid job record",
        "required_fields": ["id", "title", "company", "location", "url"],
        "invalid_handling": "Drop records missing any required field",
        "reason": "These fields are essential for displaying job information"
    },
    
    "data_quality_thresholds": {
        "description": "Quality gates during processing",
        "completeness": "Target 100% of required fields present",
        "dedup_rate": "Expect 5-15% duplicates in web-scraped data",
        "invalid_rate": "Expect <5% invalid records"
    }
}

VALIDATION_RULES = """
VALIDATION RULES APPLIED:

1. REQUIRED FIELDS:
   - Every job must have: id, title, company, location, url
   - Records missing any required field are DROPPED
   
2. DUPLICATE DETECTION:
   - Based on (title, company, location) tuple
   - Case-insensitive comparison
   - First occurrence is KEPT, subsequent duplicates are REMOVED
   
3. SKILL EXTRACTION:
   - Extracted from job description and title
   - Normalized using predefined skill mapping
   - Resulting skill list is deduplicated and sorted
   
4. TEXT CLEANUP:
   - Whitespace trimmed and normalized
   - Company names standardized by removing legal entity suffixes
   - Locations normalized (Remote variations consolidated)
   
5. MISSING VALUE HANDLING:
   - Type: Unknown (if empty)
   - Salary fields: Null (if empty)
   - Currency: USD (if empty, default assumption)
   - Skills: Extracted from description (if explicit skills field missing)
   
DECISION RATIONALE:
- Deduplication key (title+company+location) chosen to match industry standards
- Text normalization reduces false duplicates and improves data consistency
- Skill normalization critical for downstream analysis (trending, matching)
- Required field validation ensures data fitness for analysis
"""
