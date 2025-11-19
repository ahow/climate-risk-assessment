#!/usr/bin/env python3
"""
Background Worker for Climate Risk Assessment System
Processes pending assessment jobs in parallel using DeepSeek V3
"""
import os
import sys
import time
import logging

# Add app to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.database import Database, init_database
from app.assessment_engine_batched import BatchedAssessmentEngine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AssessmentWorker:
    """Background worker that processes assessment jobs from the queue"""
    
    def __init__(self):
        # Initialize database schema if needed
        try:
            init_database()
        except Exception as e:
            logger.warning(f"Database initialization: {e}")
        
        self.db = Database()
        self.engine = BatchedAssessmentEngine()
        self.running = True
        self.worker_id = os.getenv('DYNO', 'worker')
        logger.info(f"[{self.worker_id}] Batched assessment worker initialized (DeepSeek V3)")
    
    def process_job(self, job):
        """Process a single assessment job"""
        job_id = job['id']
        company_name = job['company']
        isin = job['isin']
        
        try:
            logger.info(f"[{self.worker_id}] Processing Job #{job_id}: {company_name} ({isin})")
            
            # Prepare company data
            company_data = {
                'company_id': job['company_id'],
                'name': company_name,
                'isin': isin,
                'sector': job.get('sector'),
                'industry': job.get('industry'),
                'country': job.get('country')
            }
            
            # Run assessment
            self.engine.process_company(
                job_id=job_id,
                company_data=company_data
            )
            
            logger.info(f"[{self.worker_id}] ✓ Completed Job #{job_id}: {company_name}")
            return True
            
        except Exception as e:
            logger.error(f"[{self.worker_id}] ✗ Failed Job #{job_id}: {str(e)}", exc_info=True)
            return False
    
    def run(self):
        """Main worker loop"""
        logger.info("=" * 60)
        logger.info(f"[{self.worker_id}] Climate Risk Assessment Worker Started")
        logger.info(f"[{self.worker_id}] Using Batched DeepSeek V3 (5 batches per company)")
        logger.info(f"[{self.worker_id}] Comprehensive detail: 44 measures with full rationale & evidence")
        logger.info("=" * 60)
        
        consecutive_errors = 0
        max_consecutive_errors = 5
        
        while self.running:
            try:
                # Get pending jobs (one at a time per worker)
                logger.info(f"[{self.worker_id}] Checking for pending jobs...")
                pending_jobs = self.db.get_pending_jobs(limit=1)
                logger.info(f"[{self.worker_id}] Found {len(pending_jobs)} pending job(s)")
                
                if pending_jobs:
                    for job in pending_jobs:
                        success = self.process_job(job)
                        
                        if success:
                            consecutive_errors = 0
                        else:
                            consecutive_errors += 1
                        
                        # Brief pause between jobs
                        time.sleep(2)
                else:
                    # No pending jobs, wait before checking again
                    time.sleep(10)
                
            except KeyboardInterrupt:
                logger.info(f"[{self.worker_id}] Received shutdown signal, stopping...")
                self.running = False
                break
                
            except Exception as e:
                consecutive_errors += 1
                logger.error(f"[{self.worker_id}] Worker error: {str(e)}", exc_info=True)
                
                if consecutive_errors >= max_consecutive_errors:
                    logger.critical(f"[{self.worker_id}] Too many consecutive errors, stopping")
                    break
                
                # Wait before retrying
                time.sleep(30)
        
        logger.info(f"[{self.worker_id}] Worker stopped")

def main():
    """Entry point for worker"""
    worker = AssessmentWorker()
    
    try:
        worker.run()
    except Exception as e:
        logger.critical(f"Fatal error: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
