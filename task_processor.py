import time
import sys
import logging
from result_store import ResultStore
from message_queue import JobConsumer, JobProducer
from task_functions import TASK_FUNCTIONS
from config import MAX_RETRIES, RETRY_DELAY, RETRY_BACKOFF_FACTOR

class TaskProcessor:
    def __init__(self, topic):
        self.consumer = JobConsumer(topic)
        self.producer = JobProducer()
        self.result_store = ResultStore()
        self.topic = topic
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)  # Set default logging level to INFO
        self.logger = logging.getLogger(__name__)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        # Create handlers for logging to both terminal and file
        # Terminal output handler
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(formatter)
        
        # File output handler
        file_handler = logging.FileHandler('task_processor.log')  # Log to a file
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)

        # Add both handlers to the logger
        self.logger.addHandler(stream_handler)
        self.logger.addHandler(file_handler)

    def send_heartbeat(self, status):
        self.result_store.set_worker_status(self.topic, status)

    def process_job(self, job):
        job_id = job['job_id']
        job_type = job['job_type']
        args = job.get('args', [])
        kwargs = job.get('kwargs', {})
        retry_count = job.get('retry_count', 0)

        self.send_heartbeat("processing")
        self.result_store.set_job_status(job_id, 'processing')

        try:
            if job_type in TASK_FUNCTIONS:
                result = TASK_FUNCTIONS[job_type](*args, **kwargs)
                
                self.result_store.set_job_status(job_id, 'success', result=result)
                self.send_heartbeat("free")
                self.logger.info(f"Job {job_id} of type {job_type} completed successfully with result: {result}")
                return True
            else:
                error_msg = f"Unknown job type: {job_type}"
                self.result_store.set_job_status(job_id, 'failed', result=error_msg)
                self.send_heartbeat("free")
                self.logger.error(f"Job {job_id} failed: {error_msg}")
                return False

        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"Job {job_id} failed: {error_msg}")

            if retry_count < MAX_RETRIES:
                delay = RETRY_DELAY * (RETRY_BACKOFF_FACTOR ** retry_count)
                retry_message = f"Failed attempt {retry_count + 1}/{MAX_RETRIES}: {error_msg}. Retrying in {delay} seconds..."
                
                self.result_store.set_job_status(job_id, 'retry_pending', result=retry_message)
                self.send_heartbeat("free")

                time.sleep(delay)

                job['retry_count'] = retry_count + 1
                return self.process_job(job)
            else:
                final_error_msg = f"Failed after {MAX_RETRIES} attempts. Final error: {error_msg}"
                self.result_store.set_job_status(job_id, 'failed', result=final_error_msg)
                self.send_heartbeat("free")
                return False


    def run(self):
        self.send_heartbeat("inactive")
        self.logger.info("Task Processor is now waiting for jobs...")
        self.send_heartbeat("free")

        for job in self.consumer.consume_jobs():
            self.logger.info(f"Received job {job['job_id']} of type {job['job_type']}")
            self.process_job(job)

if __name__ == "__main__":
    topic = sys.argv[1]
    task_processor = TaskProcessor(topic)
    task_processor.run()
