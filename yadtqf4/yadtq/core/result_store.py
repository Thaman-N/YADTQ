import redis
from yadtq.core.config import REDIS_URL
import logging
import time

class ResultStore:
    def __init__(self):
        self.redis = redis.StrictRedis.from_url(REDIS_URL)
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def set_job_status(self, job_id, status, result=None):
        data = {
            "status": status,
            "result": result or '',
            "last_updated": time.time()
        }
        if status == 'retry_pending':
            data["retry_timestamp"] = time.time()
        
        # hset instead of hmset
        for key, value in data.items():
            self.redis.hset(job_id, key, value)

    def get_job_status(self, job_id):
        try:
            data = self.redis.hgetall(job_id)
            if not data:
                return {
                    "status": "Unknown",
                    "result": None
                }
            
            status_mapping = {
                'queued': 'Queued',  # Add this line to include 'queued' status
                'in_progress': 'Processing',
                'processing': 'Processing',
                'success': 'Completed',
                'completed': 'Completed',
                'failed': 'Failed',
                'retry_pending': 'Retrying'
            }
            
            original_status = data.get(b'status', b'unknown').decode()
            status = status_mapping.get(original_status, original_status.capitalize())
            result = data.get(b'result', b'').decode()
            
            return {
                "status": status,
                "result": result
            }
        except Exception as e:
            self.logger.error(f"Error getting job status: {e}")
            return {
                "status": "Error",
                "result": None
            }

    def set_worker_status(self, worker_id, status):
        """Standardize status values across the system"""
        # Map any incoming status to our standard values
        status_mapping = {
            'initializing': 'initializing',
            'free': 'free',
            'processing': 'processing',
            'inactive': 'inactive'
        }
        standardized_status = status_mapping.get(status, status)
        self.redis.set(f"worker:{worker_id}:status", standardized_status)

    def get_worker_status(self, worker_id):
        """Get standardized worker status"""
        status = self.redis.get(f"worker:{worker_id}:status")
        return status.decode() if status else 'inactive'
