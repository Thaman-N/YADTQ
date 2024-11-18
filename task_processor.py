import time
import sys
import logging
import traceback
import redis
import json
import threading
from result_store import ResultStore
from message_queue import JobConsumer, JobProducer
from task_functions import TASK_FUNCTIONS
from config import (
    MAX_RETRIES, 
    RETRY_DELAY, 
    RETRY_BACKOFF_FACTOR, 
    REDIS_URL, 
    WORKER_TOPICS,
    HEARTBEAT_INTERVAL,
    WORKER_TIMEOUT
)

class TaskProcessor:
    def __init__(self, topic):
        self.consumer = JobConsumer(topic)
        self.producer = JobProducer()
        self.result_store = ResultStore()
        self.topic = topic
        self.redis_client = redis.StrictRedis.from_url(REDIS_URL)
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(formatter)
        
        file_handler = logging.FileHandler('task_processor.log')
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)

        self.logger.addHandler(stream_handler)
        self.logger.addHandler(file_handler)

        # Heartbeat tracking
        self.stop_event = threading.Event()
        self.heartbeat_thread = None

    def send_heartbeat(self, status):
        """Enhanced heartbeat with more robust status tracking"""
        try:
            self.result_store.set_worker_status(self.topic, status)
            # Additional tracking in Redis for more granular monitoring
            self.redis_client.hset(f"worker:{self.topic}:heartbeat", mapping={
                'status': status,
                'last_heartbeat': time.time()
            })
        except Exception as e:
            self.logger.error(f"Heartbeat error: {e}")

    def periodic_heartbeat(self):
        """Background thread for periodic worker status updates"""
        while not self.stop_event.is_set():
            try:
                self.send_heartbeat("active")
                self.stop_event.wait(HEARTBEAT_INTERVAL)
            except Exception as e:
                self.logger.error(f"Heartbeat thread error: {e}")
                self.stop_event.wait(HEARTBEAT_INTERVAL)

    @classmethod
    def recover_stuck_jobs(cls):
        """
        Enhanced job recovery with more robust tracking and recovery strategy
        """
        redis_client = redis.StrictRedis.from_url(REDIS_URL)
        producer = JobProducer()
        logger = logging.getLogger(__name__)

        # Find all job tracking keys in Redis
        job_tracking_keys = redis_client.keys("job_tracking:*")
        
        for key in job_tracking_keys:
            job_data = redis_client.hgetall(key)
            
            # Convert byte keys and values to strings
            job_data = {k.decode(): v.decode() for k, v in job_data.items()}
            
            current_time = time.time()
            job_timestamp = float(job_data.get('timestamp', 0))
            
            # More granular stuck job detection
            stuck_conditions = [
                job_data.get('status') in ['queued', 'in_progress'],
                current_time - job_timestamp > WORKER_TIMEOUT,
                int(job_data.get('recovery_attempts', 0)) < MAX_RETRIES
            ]
            
            if all(stuck_conditions):
                try:
                    original_job = json.loads(job_data.get('original_job', '{}'))
                    
                    if original_job:
                        logger.warning(f"Recovering potentially stuck job: {original_job.get('job_id')}")
                        
                        # Exponential backoff for recovery
                        recovery_attempts = int(job_data.get('recovery_attempts', 0)) + 1
                        backoff_delay = RETRY_DELAY * (RETRY_BACKOFF_FACTOR ** recovery_attempts)
                        
                        original_job['retry_count'] = original_job.get('retry_count', 0) + 1
                        original_job['status'] = 'queued'
                        
                        # Resubmit with delay and tracking
                        producer.submit_job(original_job)
                        
                        # Mark as recovered in Redis with attempt tracking
                        redis_client.hset(key, mapping={
                            'status': 'recovered',
                            'recovery_attempts': recovery_attempts,
                            'last_recovery_timestamp': current_time
                        })
                        
                except Exception as e:
                    logger.error(f"Error recovering job: {e}")

    def process_job(self, job):
        # Existing process_job method remains the same
        job_id = job['job_id']
        job_type = job['job_type']
        args = job.get('args', [])
        kwargs = job.get('kwargs', {})
        retry_count = job.get('retry_count', 0)
        tracking_id = job.get('tracking_id')

        self.send_heartbeat("processing")
        self.result_store.set_job_status(job_id, 'processing')

        try:
            if job_type in TASK_FUNCTIONS:
                result = TASK_FUNCTIONS[job_type](*args, **kwargs)
                
                # Update job tracking in Redis
                if tracking_id:
                    self.redis_client.hset(f"job_tracking:{tracking_id}", 
                                           mapping={
                                               'status': 'completed',
                                               'result': json.dumps(result),
                                               'timestamp': time.time()
                                           })
                
                self.result_store.set_job_status(job_id, 'success', result=result)
                self.send_heartbeat("free")
                self.logger.info(f"Job {job_id} of type {job_type} completed successfully with result: {result}")
                self.producer.update_task_count(self.topic, self.producer.task_counts[self.topic] - 1)
                return True
            else:
                return self._handle_job_requeue(job, "Unknown job type", retry_count)

        except Exception as e:
            error_msg = str(e)
            full_traceback = traceback.format_exc()
            self.logger.error(f"Job {job_id} failed: {full_traceback}")

            return self._handle_job_requeue(job, error_msg, retry_count)


    def _handle_job_requeue(self, job, error_msg, retry_count):
        """Centralized method to handle job requeueing"""
        job_id = job['job_id']
        tracking_id = job.get('tracking_id')
        full_traceback = traceback.format_exc()

        # Increment retry count
        job['retry_count'] = retry_count + 1

        # Log detailed requeue information
        retry_message = f"Failed attempt {retry_count + 1}: {error_msg}. Requeueing..."
        self.logger.warning(retry_message)

        # Update job tracking in Redis
        if tracking_id:
            self.redis_client.hset(f"job_tracking:{tracking_id}", 
                                   mapping={
                                       'status': 'failed',
                                       'error': full_traceback,
                                       'timestamp': time.time()
                                   })

        try:
            # Always attempt to update worker status and task count
            self.producer.update_task_count(self.topic, self.producer.task_counts[self.topic] - 1)
        except Exception as e:
            self.logger.error(f"Failed to update task count: {e}")

        try:
            # Requeue to a different worker, forcing a new worker selection
            if job['retry_count'] <= MAX_RETRIES:
                # Requeue to the original job queue, not just reassign workers
                job['status'] = 'queued'
                job.pop('assigned_worker', None)
                self.producer.submit_job(job)  # This will find a new worker
                return False
            else:
                # Final failure handling remains the same
                final_error_msg = f"Failed after {MAX_RETRIES} attempts. Final error: {error_msg}"
                self.result_store.set_job_status(job_id, 'failed', result=final_error_msg)
                return False
        except Exception as e:
            self.logger.error(f"Critical error in job requeue: {e}")
            return False


    def run(self):
        # Start heartbeat thread before processing jobs
        self.heartbeat_thread = threading.Thread(target=self.periodic_heartbeat)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()

        self.send_heartbeat("inactive")
        self.logger.info("Task Processor is now waiting for jobs...")
        self.send_heartbeat("free")

        try:
            for job in self.consumer.consume_jobs():
                self.logger.info(f"Received job {job['job_id']} of type {job['job_type']}")
                self.process_job(job)
        except Exception as e:
            self.logger.critical(f"Job processing failed: {e}")
        finally:
            # Graceful shutdown
            self.stop_event.set()
            if self.heartbeat_thread:
                self.heartbeat_thread.join(timeout=5)

if __name__ == "__main__":
    topic = sys.argv[1]
    task_processor = TaskProcessor(topic)
    task_processor.run()