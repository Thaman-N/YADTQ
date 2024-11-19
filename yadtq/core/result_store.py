
# result_store.py
import redis
from yadtq.core.config import REDIS_URL
import time,json

class ResultStore:
    def __init__(self):
        self.redis = redis.StrictRedis.from_url(REDIS_URL)

    # Store job status
    def set_job_status(self, job_id, status, result=None):
        self.redis.hmset(job_id, {"status": status, "result": result or ''})

    def get_job_status(self, job_id):
        data = self.redis.hgetall(job_id)
        return {
            "status": data.get(b'status').decode(),
            "result": data.get(b'result').decode() if data.get(b'result') else None
        }

    # Set worker heartbeat status
    def set_worker_status(self, worker_id, status, task_data=None):
        status_key = f"worker:{worker_id}:status"
        
        # Ensure the key is a hash; delete it if it's the wrong type
        if self.redis.exists(status_key) and self.redis.type(status_key) != b'hash':
            print(f"Key {status_key} exists but is not a hash. Deleting it.")
            self.redis.delete(status_key)
        
        # Set the worker status in the hash
        self.redis.hset(status_key, mapping={"status": status})
        
        # Update last task (separate key)
        if task_data:
            task_data_serialized = json.dumps(task_data)
            self.redis.set(f"worker:{worker_id}:last_task", task_data_serialized)

    def get_worker_status(self, worker_topic):

        status_key = f"worker:{worker_topic}:status"
        status_data = self.redis.hget(status_key, "status")
        return status_data.decode("utf-8") if status_data else None
        
    def initialize_workers(self, worker_topics):
        """Set all workers to 'inactive' at startup."""
        for worker_id in worker_topics:
            self.set_worker_status(worker_id, "inactive")
        print("All workers initialized to 'inactive' state.")


    def get_worker_last_task(self, worker_topic):
        """
        Get the last task assigned to a worker from Redis.
        """
        last_task_key = f"worker:{worker_topic}:last_task"
        last_task_data = self.redis.get(last_task_key)
        return json.loads(last_task_data) if last_task_data else None
