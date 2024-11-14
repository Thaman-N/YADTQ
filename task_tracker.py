
# task_tracker.py
import redis
from config import REDIS_URL, WORKER_TOPICS


redis_client = redis.StrictRedis.from_url(REDIS_URL)


for topic in WORKER_TOPICS:
    redis_client.setnx(f"task_count:{topic}", 0)  
def increment_task_count(topic):
    redis_client.incr(f"task_count:{topic}")

def decrement_task_count(topic):
    if int(redis_client.get(f"task_count:{topic}")) > 0:
        redis_client.decr(f"task_count:{topic}")

def get_task_count(topic):
    return int(redis_client.get(f"task_count:{topic}"))

