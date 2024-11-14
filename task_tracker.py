
# task_tracker.py
import redis
from config import REDIS_URL, WORKER_TOPICS

# Initialize Redis client
redis_client = redis.StrictRedis.from_url(REDIS_URL)

# Initialize task counts in Redis (optional, ensure keys exist)
for topic in WORKER_TOPICS:
    redis_client.setnx(f"task_count:{topic}", 0)  # Only set if key doesn't exist

def increment_task_count(topic):
    """Increment the task count for a given topic in Redis."""
    redis_client.incr(f"task_count:{topic}")

def decrement_task_count(topic):
    """Decrement the task count for a given topic in Redis."""
    if int(redis_client.get(f"task_count:{topic}")) > 0:
        redis_client.decr(f"task_count:{topic}")

def get_task_count(topic):
    """Get the current task count for a given topic from Redis."""
    return int(redis_client.get(f"task_count:{topic}"))

