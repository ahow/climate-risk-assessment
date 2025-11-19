"""
Database Module - PostgreSQL Integration
Handles all database operations and schema initialization
"""
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
import logging
from datetime import datetime
from typing import Optional, Dict, List
import json

logger = logging.getLogger(__name__)

# Database connection pool
_pool = None

def get_pool():
    """Get or create database connection pool"""
    global _pool
    if _pool is None:
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            raise ValueError("DATABASE_URL environment variable not set")
        
        # Heroku uses postgres://, but psycopg2 needs postgresql://
        if database_url.startswith('postgres://'):
            database_url = database_url.replace('postgres://', 'postgresql://', 1)
        
        _pool = SimpleConnectionPool(1, 20, database_url)
        logger.info("Database connection pool created")
    
    return _pool

def get_connection():
    """Get a database connection from the pool"""
    pool = get_pool()
    return pool.getconn()

def release_connection(conn):
    """Release a connection back to the pool"""
    pool = get_pool()
    pool.putconn(conn)

def init_database():
    """Initialize database schema"""
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            # Create companies table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS companies (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    isin VARCHAR(12) UNIQUE NOT NULL,
                    sector VARCHAR(100),
                    industry VARCHAR(100),
                    country VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create processprompt_versions table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS processprompt_versions (
                    id SERIAL PRIMARY KEY,
                    version_name VARCHAR(255) NOT NULL,
                    content TEXT NOT NULL,
                    file_size INTEGER,
                    notes TEXT,
                    is_active BOOLEAN DEFAULT FALSE,
                    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create assessment_jobs table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS assessment_jobs (
                    id SERIAL PRIMARY KEY,
                    company_id INTEGER REFERENCES companies(id),
                    processprompt_version_id INTEGER REFERENCES processprompt_versions(id),
                    status VARCHAR(20) DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    error_message TEXT
                )
            """)
            
            # Create assessments table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS assessments (
                    id SERIAL PRIMARY KEY,
                    job_id INTEGER REFERENCES assessment_jobs(id),
                    company_id INTEGER REFERENCES companies(id),
                    processprompt_version_id INTEGER REFERENCES processprompt_versions(id),
                    
                    -- Assessment results
                    overall_risk_rating VARCHAR(20),
                    physical_risk_score DECIMAL(3,1),
                    transition_risk_score DECIMAL(3,1),
                    
                    -- Physical risks
                    acute_risks TEXT,
                    chronic_risks TEXT,
                    geographic_exposure TEXT,
                    
                    -- Transition risks  
                    policy_regulatory_risks TEXT,
                    technology_risks TEXT,
                    market_risks TEXT,
                    reputation_risks TEXT,
                    
                    -- Opportunities
                    resource_efficiency TEXT,
                    energy_source TEXT,
                    products_services TEXT,
                    markets TEXT,
                    resilience TEXT,
                    
                    -- Strategy and governance
                    climate_strategy TEXT,
                    governance_structure TEXT,
                    risk_management TEXT,
                    metrics_targets TEXT,
                    
                    -- Additional context
                    data_sources TEXT,
                    confidence_level VARCHAR(20),
                    limitations TEXT,
                    recommendations TEXT,
                    
                    -- Full output
                    full_assessment TEXT,
                    
                    -- Detailed measures (44 measures with score, confidence, rationale, evidence, source, ai_model)
                    measures_detail JSONB,
                    
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_companies_isin ON companies(isin)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON assessment_jobs(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_company ON assessment_jobs(company_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_assessments_job ON assessments(job_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_assessments_company ON assessments(company_id)")
            
            conn.commit()
            logger.info("Database schema initialized successfully")
            
    except Exception as e:
        conn.rollback()
        logger.error(f"Database initialization failed: {e}")
        raise
    finally:
        release_connection(conn)

class Database:
    """Database operations wrapper"""
    
    def __init__(self):
        self.pool = get_pool()
    
    def get_connection(self):
        """Get a connection from the pool"""
        return self.pool.getconn()
    
    def release_connection(self, conn):
        """Release a connection back to the pool"""
        self.pool.putconn(conn)
    
    def get_or_create_company(self, name: str, isin: str, sector: str = None, 
                              industry: str = None, country: str = None) -> int:
        """Get existing company or create new one"""
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Try to get existing
                cursor.execute("SELECT id FROM companies WHERE isin = %s", (isin,))
                result = cursor.fetchone()
                
                if result:
                    return result['id']
                
                # Create new
                cursor.execute("""
                    INSERT INTO companies (name, isin, sector, industry, country)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                """, (name, isin, sector, industry, country))
                
                company_id = cursor.fetchone()['id']
                conn.commit()
                return company_id
                
        finally:
            self.release_connection(conn)
    
    def create_job(self, company_id: int) -> int:
        """Create a new assessment job"""
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    INSERT INTO assessment_jobs (company_id, status)
                    VALUES (%s, 'pending')
                    RETURNING id
                """, (company_id,))
                
                job_id = cursor.fetchone()['id']
                conn.commit()
                return job_id
                
        finally:
            self.release_connection(conn)
    
    def get_pending_jobs(self, limit: int = 1) -> List[Dict]:
        """Get pending jobs"""
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    UPDATE assessment_jobs
                    SET status = 'processing', started_at = CURRENT_TIMESTAMP
                    WHERE id IN (
                        SELECT id FROM assessment_jobs
                        WHERE status = 'pending'
                        ORDER BY created_at ASC
                        LIMIT %s
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING id, company_id
                """, (limit,))
                
                jobs = cursor.fetchall()
                conn.commit()
                
                # Get company details for each job
                result = []
                for job in jobs:
                    cursor.execute("""
                        SELECT id, name, isin, sector, industry, country
                        FROM companies WHERE id = %s
                    """, (job['company_id'],))
                    company = cursor.fetchone()
                    
                    result.append({
                        'id': job['id'],
                        'company_id': job['company_id'],
                        'company': company['name'],
                        'isin': company['isin'],
                        'sector': company.get('sector'),
                        'industry': company.get('industry'),
                        'country': company.get('country')
                    })
                
                return result
                
        finally:
            self.release_connection(conn)
    
    def update_job_status(self, job_id: int, status: str, error_message: str = None):
        """Update job status"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                if status == 'completed':
                    cursor.execute("""
                        UPDATE assessment_jobs
                        SET status = %s, completed_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """, (status, job_id))
                elif status == 'failed':
                    cursor.execute("""
                        UPDATE assessment_jobs
                        SET status = %s, completed_at = CURRENT_TIMESTAMP, error_message = %s
                        WHERE id = %s
                    """, (status, error_message, job_id))
                else:
                    cursor.execute("""
                        UPDATE assessment_jobs
                        SET status = %s
                        WHERE id = %s
                    """, (status, job_id))
                
                conn.commit()
                
        finally:
            self.release_connection(conn)
    
    def save_assessment(self, job_id: int, company_id: int, assessment_data: Dict):
        """Save assessment results"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                # Get processprompt version from job
                cursor.execute("""
                    SELECT processprompt_version_id FROM assessment_jobs WHERE id = %s
                """, (job_id,))
                result = cursor.fetchone()
                processprompt_version_id = result[0] if result else None
                
                cursor.execute("""
                    INSERT INTO assessments (
                        job_id, company_id, processprompt_version_id,
                        overall_risk_rating, physical_risk_score, transition_risk_score,
                        full_assessment, measures_detail
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    job_id, company_id, processprompt_version_id,
                    assessment_data.get('overall_risk_rating'),
                    assessment_data.get('physical_risk_score'),
                    assessment_data.get('transition_risk_score'),
                    json.dumps(assessment_data),
                    json.dumps(assessment_data.get('measures', {}))
                ))
                
                conn.commit()
                
        finally:
            self.release_connection(conn)
    
    def get_active_processprompt(self) -> Optional[Dict]:
        """Get active ProcessPrompt version"""
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT id, version_name, content
                    FROM processprompt_versions
                    WHERE is_active = TRUE
                    ORDER BY uploaded_at DESC
                    LIMIT 1
                """)
                return cursor.fetchone()
                
        finally:
            self.release_connection(conn)
    
    def get_stats(self) -> Dict:
        """Get system statistics"""
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                stats = {}
                
                cursor.execute("SELECT COUNT(*) as count FROM companies")
                stats['companies'] = cursor.fetchone()['count']
                
                cursor.execute("SELECT COUNT(*) as count FROM assessments")
                stats['completed'] = cursor.fetchone()['count']
                
                cursor.execute("SELECT COUNT(*) as count FROM assessment_jobs WHERE status = 'processing'")
                stats['processing'] = cursor.fetchone()['count']
                
                cursor.execute("SELECT COUNT(*) as count FROM assessment_jobs WHERE status = 'pending'")
                stats['pending'] = cursor.fetchone()['count']
                
                return stats
                
        finally:
            self.release_connection(conn)

    def upload_processprompt(self, version_name: str, content: str, notes: str = None, 
                            set_active: bool = False) -> int:
        """Upload a new ProcessPrompt version"""
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                file_size = len(content.encode('utf-8'))
                
                # If set_active, deactivate all others first
                if set_active:
                    cursor.execute("UPDATE processprompt_versions SET is_active = FALSE")
                
                cursor.execute("""
                    INSERT INTO processprompt_versions 
                    (version_name, content, file_size, notes, is_active)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                """, (version_name, content, file_size, notes, set_active))
                
                processprompt_id = cursor.fetchone()['id']
                conn.commit()
                
                logger.info(f"ProcessPrompt uploaded: {version_name} ({file_size} bytes)")
                return processprompt_id
                
        finally:
            self.release_connection(conn)
    
    def get_all_processprompts(self) -> List[Dict]:
        """Get all ProcessPrompt versions"""
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT id, version_name, file_size, notes, is_active, uploaded_at
                    FROM processprompt_versions
                    ORDER BY uploaded_at DESC
                """)
                return cursor.fetchall()
                
        finally:
            self.release_connection(conn)
    
    def activate_processprompt(self, processprompt_id: int):
        """Activate a specific ProcessPrompt version"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                # Deactivate all
                cursor.execute("UPDATE processprompt_versions SET is_active = FALSE")
                
                # Activate specified
                cursor.execute("""
                    UPDATE processprompt_versions 
                    SET is_active = TRUE 
                    WHERE id = %s
                """, (processprompt_id,))
                
                conn.commit()
                logger.info(f"ProcessPrompt {processprompt_id} activated")
                
        finally:
            self.release_connection(conn)
    
    def download_processprompt(self, processprompt_id: int) -> Optional[Dict]:
        """Get a specific ProcessPrompt version with content"""
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT id, version_name, content, file_size, notes, is_active, uploaded_at
                    FROM processprompt_versions
                    WHERE id = %s
                """, (processprompt_id,))
                return cursor.fetchone()
                
        finally:
            self.release_connection(conn)
    
    def get_recent_jobs(self, limit: int = 20) -> List[Dict]:
        """Get recent assessment jobs"""
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        j.id,
                        j.status,
                        j.created_at,
                        j.completed_at,
                        c.name as company_name,
                        c.isin
                    FROM assessment_jobs j
                    JOIN companies c ON j.company_id = c.id
                    ORDER BY j.created_at DESC
                    LIMIT %s
                """, (limit,))
                return cursor.fetchall()
                
        finally:
            self.release_connection(conn)
    
    def get_latest_assessments(self) -> List[Dict]:
        """Get latest assessment for each company (one per company)"""
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT DISTINCT ON (c.isin)
                        c.name as company_name,
                        c.isin,
                        c.sector,
                        c.industry,
                        c.country,
                        a.overall_risk_rating,
                        a.physical_risk_score,
                        a.transition_risk_score,
                        a.measures_detail,
                        a.created_at
                    FROM assessments a
                    JOIN companies c ON a.company_id = c.id
                    ORDER BY c.isin, a.created_at DESC
                """)
                return cursor.fetchall()
                
        finally:
            self.release_connection(conn)
    
    def get_all_assessments(self) -> List[Dict]:
        """Get all assessments including re-runs"""
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        c.name as company_name,
                        c.isin,
                        c.sector,
                        c.industry,
                        c.country,
                        a.overall_risk_rating,
                        a.physical_risk_score,
                        a.transition_risk_score,
                        a.measures_detail,
                        a.created_at
                    FROM assessments a
                    JOIN companies c ON a.company_id = c.id
                    ORDER BY a.created_at DESC
                """)
                return cursor.fetchall()
                
        finally:
            self.release_connection(conn)
