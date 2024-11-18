import sys
sys.path.append('.')  # Add current directory to Python path

from task_processor import TaskProcessor
import logging
import redis
from config import REDIS_URL
import json

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def diagnose_job_tracking():
    """Provide detailed diagnostics about job tracking in Redis"""
    redis_client = redis.StrictRedis.from_url(REDIS_URL)
    
    # Find all job tracking keys
    job_tracking_keys = redis_client.keys("job_tracking:*")
    
    logger.info(f"Found {len(job_tracking_keys)} job tracking entries:")
    
    for key in job_tracking_keys:
        job_data = redis_client.hgetall(key)
        
        # Convert byte keys and values to strings
        job_data = {k.decode(): v.decode() for k, v in job_data.items()}
        
        logger.info(f"\nJob Tracking Key: {key.decode()}")
        for k, v in job_data.items():
            logger.info(f"  {k}: {v}")

def run_job_recovery():
    logger.info("Starting comprehensive job recovery diagnostics...")
    
    # First, show existing job tracking entries
    logger.info("\n--- BEFORE RECOVERY ---")
    diagnose_job_tracking()
    
    # Run recovery
    logger.info("\n--- RUNNING RECOVERY ---")
    TaskProcessor.recover_stuck_jobs()
    
    # Show job tracking entries after recovery
    logger.info("\n--- AFTER RECOVERY ---")
    diagnose_job_tracking()

if __name__ == "__main__":
    run_job_recovery()