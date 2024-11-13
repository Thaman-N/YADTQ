# task_processor.py
from message_queue import JobConsumer
from result_store import ResultStore
from task_functions import TASK_FUNCTIONS
import sys

class TaskProcessor:
    def __init__(self, topic):
        self.consumer = JobConsumer(topic)
        self.result_store = ResultStore()

    def process_job(self, job):
        job_id = job['job_id']
        job_type = job['job_type']
        args = job.get('args', [])
        kwargs = job.get('kwargs', {})

        self.result_store.set_job_status(job_id, 'processing')

        try:
            if job_type in TASK_FUNCTIONS:
                result = TASK_FUNCTIONS[job_type](*args, **kwargs)
                self.result_store.set_job_status(job_id, 'success', result=result)
            else:
                raise ValueError(f"Unknown job type: {job_type}")
        except Exception as e:
            self.result_store.set_job_status(job_id, 'failed', result=str(e))

    def run(self):
        print("Task Processor is now waiting for jobs...")
        for job in self.consumer.consume_jobs():
            print(f"Received job {job['job_id']} of type {job['job_type']}")
            self.process_job(job)

if __name__ == "__main__":
    topic = sys.argv[1] 
    task_processor = TaskProcessor(topic)
    task_processor.run()
