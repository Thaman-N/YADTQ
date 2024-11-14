# task_processor.py
import redis,sys,time
from message_queue import JobConsumer
from result_store import ResultStore
from task_functions import TASK_FUNCTIONS
from config import REDIS_URL

class TaskProcessor:
    def __init__(self, topic):
        self.consumer = JobConsumer(topic)
        self.result_store = ResultStore()
        self.redis = redis.StrictRedis.from_url(REDIS_URL)
        self.topic = topic

    def process_job(self, job):
        job_id = job['job_id']
        job_type = job['job_type']
        args = job.get('args', [])
        kwargs = job.get('kwargs', {})

        self.result_store.set_job_status(job_id, 'processing')
        time.sleep(10)
        try:
            if job_type in TASK_FUNCTIONS:
                result = TASK_FUNCTIONS[job_type](*args, **kwargs)
                self.result_store.set_job_status(job_id, 'success', result=result)
            else:
                raise ValueError(f"Unknown job type: {job_type}")
        except Exception as e:
            self.result_store.set_job_status(job_id, 'failed', result=str(e))
        finally:
            # Update task count in Redis upon job completion
            self.redis.decr(f"task_count:{self.topic}")

    def run(self):
        print("Task Processor is now waiting for jobs...")
        for job in self.consumer.consume_jobs():
            print(f"Received job {job['job_id']} of type {job['job_type']}")
            self.redis.incr(f"task_count:{self.topic}")  # Increment count when job is received
            self.process_job(job)

if __name__ == "__main__":
    topic = sys.argv[1]  # Specify the topic this worker listens to
    task_processor = TaskProcessor(topic)
    task_processor.run()
