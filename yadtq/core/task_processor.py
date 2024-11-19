# task_processor.py (Worker with fault tolerance)
import time
import sys
import threading
from yadtq.core.result_store import ResultStore
from yadtq.core.message_queue import JobConsumer, JobProducer
from yadtq.core.task_functions import TASK_FUNCTIONS
import redis,json


class TaskProcessor:
    def __init__(self, topic):
        self.consumer = JobConsumer(topic)
        self.result_store = ResultStore()
        self.producer = JobProducer()  # Kafka producer to re-queue jobs
        self.topic = topic  # Using topic as worker ID
        self.running = True  # Control flag for the heartbeat thread
        self.current_job = None  # Store the current job being processed

    def send_heartbeat(self, status):
        self.result_store.set_worker_status(self.topic, status)
        self.result_store.redis.set(f"worker:{self.topic}:heartbeat_timestamp", time.time())

    def heartbeat_loop(self):
        while self.running:
            # Send a regular heartbeat with current status
            current_status = self.result_store.get_worker_status(self.topic)
            self.send_heartbeat(current_status)
            time.sleep(1)  # Send heartbeat every 2 seconds

    def process_job(self, job):
        self.current_job = job  # Track the current job
        job_id = job['job_id']
        job_type = job['job_type']
        args = job.get('args', [])
        kwargs = job.get('kwargs', {})

        # Set status to processing
        self.send_heartbeat("processing")
        self.result_store.set_job_status(job_id, 'processing')
        #time.sleep(20)  # Simulate task processing

        try:
            if job_type in TASK_FUNCTIONS:
                result = TASK_FUNCTIONS[job_type](*args, **kwargs)
                self.result_store.set_job_status(job_id, 'success', result=result)
            else:
                raise ValueError(f"Unknown job type: {job_type}")
        except Exception as e:
            self.result_store.set_job_status(job_id, 'failed', result=str(e))
        
        # Job completed successfully; clear current job and set worker to free
        self.current_job = None
        self.send_heartbeat("free")


    def run(self):
        # Start with status as inactive, then set to free
        #self.send_heartbeat("inactive")
        print("Task Processor is now waiting for jobs...")
        self.send_heartbeat("free")
        self.result_store.set_worker_status(self.topic, "free")
        # Start the heartbeat thread
        heartbeat_thread = threading.Thread(target=self.heartbeat_loop, daemon=True)
        heartbeat_thread.start()

        try:
            for job in self.consumer.consume_jobs():
                print(f"Received job {job['job_id']} of type {job['job_type']}")
                self.process_job(job)
        except KeyboardInterrupt:
            print("Worker shutting down...")
        finally:
            self.running = False
            heartbeat_thread.join()
            self.send_heartbeat("inactive")  # Mark worker as inactive on shutdown


if __name__ == "__main__":
    topic = sys.argv[1]  # The topic name acts as the worker ID
    task_processor = TaskProcessor(topic)
    task_processor.run()
