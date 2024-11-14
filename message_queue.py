
from kafka import KafkaProducer, KafkaConsumer
import json
import redis
import time
from config import KAFKA_BROKER_URL, JOB_QUEUE_TOPIC, WORKER_TOPICS, REDIS_URL
from result_store import ResultStore
import task_tracker  



class JobProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER_URL,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.result_store = ResultStore()
        
        self.task_counts = {topic: 0 for topic in WORKER_TOPICS}

    def get_least_loaded_free_worker(self):
        free_workers = [topic for topic in WORKER_TOPICS if self.result_store.get_worker_status(topic) == "free"]
        
        if not free_workers:
            return None
        
        least_loaded_worker = min(free_workers, key=lambda topic: self.task_counts[topic])
        return least_loaded_worker

    def submit_job(self, job):
       
        target_topic = self.get_least_loaded_free_worker()
        
        if target_topic:
           
            job['status'] = 'queued'
            self.producer.send(target_topic, job)
            self.producer.flush()
            self.task_counts[target_topic] += 1
            print(f"Job {job['job_id']} sent to {target_topic}. Task count: {self.task_counts[target_topic]}")
        else:
            print("No free workers available. Retrying...")

    def update_task_count(self, topic, count):
       
        self.task_counts[topic] = count


class JobConsumer:
    def __init__(self, topic):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BROKER_URL,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

    def consume_jobs(self):
        for message in self.consumer:
            yield message.value
