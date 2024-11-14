KAFKA_BROKER_URL = 'localhost:9092'
REDIS_URL = 'redis://localhost:6379'
JOB_QUEUE_TOPIC = 'job_queue'
WORKER_TOPICS = [
    "worker_1_topic",
    "worker_2_topic",
    "worker_3_topic",
]
MAX_RETRIES = 3
RETRY_DELAY = 5
RETRY_BACKOFF_FACTOR = 2 