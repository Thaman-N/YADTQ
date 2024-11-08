from kafka import KafkaProducer, KafkaConsumer
import json
from config import KAFKA_BROKER_URL, JOB_QUEUE_TOPIC

class JobProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER_URL,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def submit_job(self, job):
        job['status'] = 'queued'
        self.producer.send(JOB_QUEUE_TOPIC, job)
        self.producer.flush()

class JobConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            JOB_QUEUE_TOPIC,
            bootstrap_servers=KAFKA_BROKER_URL,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

    def consume_jobs(self):
        for message in self.consumer:
            yield message.value