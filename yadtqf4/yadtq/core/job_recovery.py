import sys
sys.path.append('.')  # Add current directory to Python path

from yadtq.core.task_processor import TaskProcessor
from yadtq.core.message_queue import JobProducer
import logging
import redis
from yadtq.core.config import REDIS_URL
from yadtq.core.result_store import ResultStore
import json
import time

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def diagnose_job_tracking():
    redis_client = redis.StrictRedis.from_url(REDIS_URL)
    job_tracking_keys = redis_client.keys("job_tracking:*")
    
    recoverable_jobs = []
    keys_to_delete = []
    
    for key in job_tracking_keys:
        try:
            job_data = redis_client.hgetall(key)
            job_data = {k.decode(): v.decode() for k, v in job_data.items()}
            
            # Check job status in result store
            job_id = key.decode().split(':')[-1]
            result_store = ResultStore()
            job_status = result_store.get_job_status(job_id)
            
            # Only recover jobs that are actually queued or in progress
            if job_status['status'] in ['Queued', 'Processing', 'Retrying']:
                original_job = json.loads(job_data.get('original_job', '{}'))
                if original_job:
                    recoverable_jobs.append(original_job)
            else:
                # Mark completed/failed jobs for deletion
                keys_to_delete.append(key)
        
        except Exception as e:
            logger.error(f"Error processing job tracking key {key}: {e}")
    
    # Delete completed job tracking entries
    if keys_to_delete:
        redis_client.delete(*keys_to_delete)
        logger.info(f"Deleted {len(keys_to_delete)} completed job tracking entries")
    
    return recoverable_jobs

def run_job_recovery():
    logger.info("Starting comprehensive job recovery diagnostics...")
    
    # First, show existing job tracking entries
    logger.info("\n--- BEFORE RECOVERY ---")
    jobs_to_recover = diagnose_job_tracking()
    
    # Attempt to recover jobs
    logger.info("\n--- RECOVERING JOBS ---")
    producer = JobProducer()
    redis_client = redis.StrictRedis.from_url(REDIS_URL)
    
    # Track the number of jobs recovered per worker
    worker_job_count = {}
    
    for job in jobs_to_recover:
        try:
            # Check job age
            tracking_key = f"job_tracking:{job.get('job_id')}"
            job_tracking_data = redis_client.hgetall(tracking_key)
            job_tracking_data = {k.decode(): v.decode() for k, v in job_tracking_data.items()}
            
            timestamp = float(job_tracking_data.get('timestamp', 0))
            current_time = time.time()
            
            # Only recover jobs less than 1 hour old
            if current_time - timestamp > 3600:  # 1 hour in seconds
                logger.info(f"Skipping old job {job.get('job_id')} from {time.ctime(timestamp)}")
                redis_client.delete(tracking_key)
                continue
            
            logger.info(f"Attempting to recover job: {job.get('job_id')}")
            
            # Reset job status and retry count
            job['status'] = 'queued'
            job['retry_count'] = job.get('retry_count', 0) + 1
            
            # Remove previous worker assignment
            job.pop('assigned_worker', None)
            
            # Resubmit the job
            recovered_job = producer.submit_job(job)
            
            # Track number of jobs per worker
            worker = recovered_job.get('assigned_worker')
            worker_job_count[worker] = worker_job_count.get(worker, 0) + 1
            
            # Delete the job tracking entry after successful recovery
            redis_client.delete(tracking_key)
            
            logger.info(f"Successfully recovered job {recovered_job['job_id']} to worker {worker}")
        
        except Exception as e:
            logger.error(f"Failed to recover job {job.get('job_id')}: {e}")
    
    # Log job distribution
    logger.info("\n--- JOB DISTRIBUTION ---")
    for worker, count in worker_job_count.items():
        logger.info(f"Worker {worker}: {count} jobs recovered")
    
    # Show job tracking entries after recovery
    logger.info("\n--- AFTER RECOVERY ---")
    diagnose_job_tracking()

if __name__ == "__main__":
    run_job_recovery()