
import redis
from config import REDIS_URL, WORKER_TOPICS

def reset_task_counts():
    # Connect to Redis
    r = redis.StrictRedis.from_url(REDIS_URL)
    
    # Reset task count for each worker topic
    for topic in WORKER_TOPICS:
        r.set(f"task_count:{topic}", 0)
        print(f"Task count for {topic} reset to 0")

if __name__ == "__main__":
    reset_task_counts()
