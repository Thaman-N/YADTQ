from kafka import KafkaProducer, KafkaConsumer
import json
from config import KAFKA_BROKER_URL, JOB_QUEUE_TOPIC , WORKER_TOPICS


class JobProducer:
    def __init__(self):
        worker_topics=WORKER_TOPICS
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER_URL,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        # Initialize task count dictionary with each worker topic and a count of 0
        self.task_counts = {topic: 0 for topic in worker_topics}

    def submit_job(self, job):
        # Find the worker topic with the lowest count
        target_topic = min(self.task_counts, key=self.task_counts.get)
        
        # Send job to selected topic
        job['status'] = 'queued'
        self.producer.send(target_topic, job)
        self.producer.flush()
        
        # Increment task count for the chosen topic
        self.task_counts[target_topic] += 1
        print(f"Job {job['job_id']} sent to {target_topic}. Task count: {self.task_counts[target_topic]}")

    def update_task_count(self, topic, count):
        '''for future use to update if wanted'''
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
