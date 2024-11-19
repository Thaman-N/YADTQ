from kafka import KafkaProducer, KafkaConsumer
import json
import redis
import time
from yadtq.core.config import KAFKA_BROKER_URL, JOB_QUEUE_TOPIC, WORKER_TOPICS, REDIS_URL
from yadtq.core.result_store import ResultStore



class JobProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER_URL,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        self.result_store = ResultStore()

        # Initialize task count dictionary
        self.task_counts = {topic: 0 for topic in WORKER_TOPICS}

        # Connect to Redis to store the last task details
        self.redis = redis.StrictRedis.from_url(REDIS_URL)

    def get_least_loaded_free_worker(self):
        """Find the free worker with the least number of assigned tasks."""
        free_workers = [
            topic
            for topic in WORKER_TOPICS
            if self.result_store.get_worker_status(topic) == "free"
        ]

        if not free_workers:
            return None

        # If there are multiple free workers, choose the one with the least tasks
        least_loaded_worker = min(
            free_workers, key=lambda topic: self.task_counts[topic]
        )
        return least_loaded_worker

    def store_last_task_for_worker(self, worker_topic, job):
        """
        Store the last task sent to a worker in Redis.
        """
        task_key = f"worker:{worker_topic}:last_task"
        self.redis.set(task_key, json.dumps(job))

    def submit_job(self, job):
        job_id=job["job_id"]
        self.result_store.set_job_status(job_id,"queued")
        target_topic = self.get_least_loaded_free_worker()
        job["status"] = "queued"
        if target_topic:
            # Assign the job and increment the task count for that worker
            
            self.producer.send(target_topic, job)
            self.producer.flush()
            self.task_counts[target_topic] += 1
            print(
                f"Job {job['job_id']} sent to {target_topic}. Task count: {self.task_counts[target_topic]}"
            )

            # Store the last task sent to the worker
            self.store_last_task_for_worker(target_topic, job)
        else:
            #print("No free workers available. Retrying...")
            time.sleep(0.25)
            self.submit_job(job)


class JobConsumer:
    def __init__(self, topic):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BROKER_URL,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

    def consume_jobs(self):
        for message in self.consumer:
            yield message.value
