
# task_processor.py (Worker)
import time,sys
from result_store import ResultStore
from message_queue import JobConsumer
from task_functions import TASK_FUNCTIONS

class TaskProcessor:
    def __init__(self, topic):
        self.consumer = JobConsumer(topic)
        self.result_store = ResultStore()
        self.topic = topic  # Using topic as worker ID

    def send_heartbeat(self, status):
        # Use topic as the worker_id in Redis
        self.result_store.set_worker_status(self.topic, status)

    def process_job(self, job):
        job_id = job['job_id']
        job_type = job['job_type']
        args = job.get('args', [])
        kwargs = job.get('kwargs', {})

        
        self.send_heartbeat("processing")
        self.result_store.set_job_status(job_id, 'processing')
        time.sleep(2)
        try:
            if job_type in TASK_FUNCTIONS:
                result = TASK_FUNCTIONS[job_type](*args, **kwargs)
                self.result_store.set_job_status(job_id, 'success', result=result)
            else:
                raise ValueError(f"Unknown job type: {job_type}")
        except Exception as e:
            self.result_store.set_job_status(job_id, 'failed', result=str(e))
        
        
        # self.send_heartbeat("done")
        # time.sleep(1)  # Simulate task completion
        self.send_heartbeat("free")

    def run(self):
        
        self.send_heartbeat("inactive")
        print("Task Processor is now waiting for jobs...")

        
        self.send_heartbeat("free")

        for job in self.consumer.consume_jobs():
            print(f"Received job {job['job_id']} of type {job['job_type']}")
            self.process_job(job)

if __name__ == "__main__":
    topic = sys.argv[1]  
    task_processor = TaskProcessor(topic)
    task_processor.run()
