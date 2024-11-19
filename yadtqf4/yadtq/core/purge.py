import redis
import sys
sys.path.append('.')  # Add current directory to Python path

from yadtq.core.config import REDIS_URL
import logging

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def purge_job_related_redis_entries():
    """Comprehensive purge of job-related Redis entries"""
    redis_client = redis.StrictRedis.from_url(REDIS_URL)
    
    # Patterns to delete
    patterns = [
        "job_tracking:*",  # Job tracking entries
        "worker:*:task_count",  # Worker task counts
        "worker:*:heartbeat",  # Worker heartbeat data
        "worker:*:status",  # Worker status entries
        "worker:*:last_assigned",  # Last worker assignment times
        "active_workers"  # Active workers set
    ]
    
    total_deleted = 0
    
    for pattern in patterns:
        keys = redis_client.keys(pattern)
        if keys:
            logger.info(f"Deleting {len(keys)} keys matching pattern: {pattern}")
            redis_client.delete(*keys)
            total_deleted += len(keys)
    
    logger.info(f"Total entries deleted: {total_deleted}")

if __name__ == "__main__":
    purge_job_related_redis_entries()