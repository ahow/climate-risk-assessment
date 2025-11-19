"""
Database Extensions for External Sync
Additional database methods needed for external spreadsheet integration
"""
from typing import Optional, Dict, List
from psycopg2.extras import RealDictCursor
import logging

logger = logging.getLogger(__name__)

def add_sync_methods_to_database(Database):
    """Add external sync methods to Database class"""
    
    def get_company_by_isin(self, isin: str) -> Optional[Dict]:
        """Get company by ISIN"""
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT id, name, isin, sector, industry, country, created_at
                    FROM companies
                    WHERE isin = %s
                """, (isin,))
                return cursor.fetchone()
        finally:
            self.release_connection(conn)
    
    def add_company(self, name: str, isin: str, sector: str = None, 
                   industry: str = None, country: str = None) -> int:
        """Add a new company"""
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    INSERT INTO companies (name, isin, sector, industry, country)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                """, (name, isin, sector, industry, country))
                
                company_id = cursor.fetchone()['id']
                conn.commit()
                logger.info(f"Added company: {name} ({isin})")
                return company_id
        finally:
            self.release_connection(conn)
    
    def update_company(self, company_id: int, name: str = None, sector: str = None,
                      industry: str = None, country: str = None) -> bool:
        """Update company information"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                updates = []
                params = []
                
                if name:
                    updates.append("name = %s")
                    params.append(name)
                if sector:
                    updates.append("sector = %s")
                    params.append(sector)
                if industry:
                    updates.append("industry = %s")
                    params.append(industry)
                if country:
                    updates.append("country = %s")
                    params.append(country)
                
                if not updates:
                    return False
                
                params.append(company_id)
                
                cursor.execute(f"""
                    UPDATE companies
                    SET {', '.join(updates)}
                    WHERE id = %s
                """, params)
                
                conn.commit()
                return True
        finally:
            self.release_connection(conn)
    
    def get_all_companies(self) -> List[Dict]:
        """Get all companies"""
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT id, name, isin, sector, industry, country, created_at
                    FROM companies
                    ORDER BY name
                """)
                return cursor.fetchall()
        finally:
            self.release_connection(conn)
    
    def get_company_jobs(self, company_id: int) -> List[Dict]:
        """Get all jobs for a company"""
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT id, company_id, status, created_at, started_at, completed_at
                    FROM assessment_jobs
                    WHERE company_id = %s
                    ORDER BY created_at DESC
                """, (company_id,))
                return cursor.fetchall()
        finally:
            self.release_connection(conn)
    
    def create_assessment_job(self, company_id: int) -> int:
        """Create assessment job for a company"""
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
    
    # Add methods to Database class
    Database.get_company_by_isin = get_company_by_isin
    Database.add_company = add_company
    Database.update_company = update_company
    Database.get_all_companies = get_all_companies
    Database.get_company_jobs = get_company_jobs
    Database.create_assessment_job = create_assessment_job
