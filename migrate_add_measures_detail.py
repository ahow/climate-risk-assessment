#!/usr/bin/env python3
"""
Database Migration: Add measures_detail JSONB column to assessments table
"""
import os
import sys
import psycopg2
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def migrate():
    """Add measures_detail column if it doesn't exist"""
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        logger.error("DATABASE_URL not set")
        return False
    
    # Fix postgres:// to postgresql://
    if database_url.startswith('postgres://'):
        database_url = database_url.replace('postgres://', 'postgresql://', 1)
    
    try:
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        # Check if column exists
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='assessments' AND column_name='measures_detail'
        """)
        
        if cursor.fetchone():
            logger.info("✓ measures_detail column already exists")
        else:
            logger.info("Adding measures_detail column...")
            cursor.execute("""
                ALTER TABLE assessments 
                ADD COLUMN measures_detail JSONB
            """)
            conn.commit()
            logger.info("✓ measures_detail column added successfully")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        return False

if __name__ == "__main__":
    success = migrate()
    sys.exit(0 if success else 1)
