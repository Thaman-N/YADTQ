import redis
from config import REDIS_URL
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
                self.logger.warning(f"No job status found for job_id: {job_id}")
                return {
                    "status": "unknown",
                    "result": None,
                    "last_updated": 0,
                    "retry_timestamp": None
                }
            status = data.get(b'status', b'unknown').decode()
            result = data.get(b'result', b'').decode()
            last_updated = float(data.get(b'last_updated', 0))
            retry_timestamp = float(data.get(b'retry_timestamp', 0)) if b'retry_timestamp' in data else None
            return {
                "status": status,
                "result": result,
                "last_updated": last_updated,
                "retry_timestamp": retry_timestamp
            }
        except Exception as e:
            self.logger.error(f"Error getting job status for job_id: {job_id}, Error: {e}")
            return {
                "status": "unknown",
                "result": None,
                "last_updated": 0,
                "retry_timestamp": None
            }

    def set_worker_status(self, worker_id, status):
        self.redis.set(f"worker:{worker_id}:status", status)

    def get_worker_status(self, worker_id):
        status = self.redis.get(f"worker:{worker_id}:status")
        return status.decode() if status else 'inactive'
