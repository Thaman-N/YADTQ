import redis
from config import REDIS_URL

class ResultStore:
    def __init__(self):
        self.redis = redis.StrictRedis.from_url(REDIS_URL)

    def set_job_status(self, job_id, status, result=None):
        self.redis.hmset(job_id, {"status": status, "result": result or ''})

    def get_job_status(self, job_id):
        data = self.redis.hgetall(job_id)
        return {
            "status": data.get(b'status').decode(),
            "result": data.get(b'result').decode() if data.get(b'result') else None
        }