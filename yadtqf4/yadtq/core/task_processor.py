import time
import sys
import logging
import traceback
import redis
import json
import threading
from yadtq.core.result_store import ResultStore
from yadtq.core.message_queue import JobConsumer, JobProducer
from yadtq.core.task_functions import TASK_FUNCTIONS
from yadtq.core.config import (
    MAX_RETRIES, 
    RETRY_DELAY, 
    RETRY_BACKOFF_FACTOR, 
    REDIS_URL, 
    WORKER_TOPICS,
    HEARTBEAT_INTERVAL,
    WORKER_TIMEOUT,
    KAFKA_BROKER_URL
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
        self.redis_client.sadd("active_workers", topic)
        # Initialize task counts with Redis for persistence
        self._initialize_task_counts()
        # Set initial status
        self.send_heartbeat("initializing")

    def _initialize_task_counts(self):
        """Initialize or load task counts from Redis"""
        count_key = f"worker:{self.topic}:task_count"
        if not self.redis_client.exists(count_key):
            self.redis_client.set(count_key, 0)

    def send_heartbeat(self, status):
        """Enhanced heartbeat with standardized status values"""
        try:
            pipeline = self.redis_client.pipeline()
            current_time = time.time()
            
            # Update both status markers atomically
            pipeline.set(f"worker:{self.topic}:status", status)
            pipeline.hset(f"worker:{self.topic}:heartbeat", mapping={
                'status': status,
                'last_heartbeat': current_time
            })
            
            # Execute updates
            pipeline.execute()
            
            # Update result store status separately (it has its own Redis connection)
            self.result_store.set_worker_status(self.topic, status)
            
        except Exception as e:
            self.logger.error(f"Heartbeat error: {e}")

    def periodic_heartbeat(self):
        """Background thread for periodic worker status updates"""
        while not self.stop_event.is_set():
            try:
                # Set status to "free" when not processing a job
                current_status = "processing" if self._processing_job else "free"
                self.send_heartbeat(current_status)
                self.stop_event.wait(HEARTBEAT_INTERVAL)
            except Exception as e:
                self.logger.error(f"Heartbeat thread error: {e}")
                self.stop_event.wait(HEARTBEAT_INTERVAL)

    def stop(self):
        """Graceful shutdown method"""
        self.logger.info("Shutting down worker...")
        self.stop_event.set()
        if self.heartbeat_thread:
            self.heartbeat_thread.join(timeout=5)
        # Update status to inactive before removing
        self.send_heartbeat("inactive")
        # Remove from active workers
        self.redis_client.srem("active_workers", self.topic)
        # Clear heartbeat and task count
        self.redis_client.delete(f"worker:{self.topic}:heartbeat")
        self.redis_client.delete(f"worker:{self.topic}:task_count")

    def __del__(self):
        """Cleanup when worker is shutting down"""
        try:
            # Remove from active workers set
            self.redis_client.srem("active_workers", self.topic)
            # Clear status, heartbeat and task count
            self.redis_client.delete(f"worker:{self.topic}:status")
            self.redis_client.delete(f"worker:{self.topic}:heartbeat")
            self.redis_client.delete(f"worker:{self.topic}:task_count")
        except:
            pass


    def process_job(self, job):
        job_id = job['job_id']
        job_type = job['job_type']
        args = job.get('args', [])
        kwargs = job.get('kwargs', {})
        retry_count = job.get('retry_count', 0)
        tracking_id = job.get('tracking_id')

        try:
            # Mark that we're processing a job and update status
            self._processing_job = True
            self.send_heartbeat("processing")
            self.result_store.set_job_status(job_id, 'processing')

            if job_type in TASK_FUNCTIONS:
                result = TASK_FUNCTIONS[job_type](*args, **kwargs)
                
                if tracking_id:
                    self.redis_client.hset(f"job_tracking:{tracking_id}", 
                                        mapping={
                                            'status': 'completed',
                                            'result': json.dumps(result),
                                            'timestamp': time.time()
                                        })
                
                self.result_store.set_job_status(job_id, 'success', result=result)
                self.logger.info(f"Job {job_id} of type {job_type} completed successfully with result: {result}")
                
                # Decrement task count in Redis
                count_key = f"worker:{self.topic}:task_count"
                current_count = int(self.redis_client.get(count_key) or 0)
                if current_count > 0:
                    self.redis_client.decr(count_key)
                
                return True
            else:
                return self._handle_job_requeue(job, "Unknown job type", retry_count)

        except Exception as e:
            error_msg = str(e)
            full_traceback = traceback.format_exc()
            self.logger.error(f"Job {job_id} failed: {full_traceback}")
            return self._handle_job_requeue(job, error_msg, retry_count)
        finally:
            # Mark that we're done processing and update status
            self._processing_job = False
            self.send_heartbeat("free")

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
            # Update task count in Redis
            count_key = f"worker:{self.topic}:task_count"
            current_count = int(self.redis_client.get(count_key) or 0)
            self.redis_client.set(count_key, max(0, current_count - 1))
            
            if job['retry_count'] <= MAX_RETRIES:
                # Requeue to the original job queue
                job['status'] = 'queued'
                job.pop('assigned_worker', None)
                self.producer.submit_job(job)  # This will find a new worker
                return False
            else:
                final_error_msg = f"Failed after {MAX_RETRIES} attempts. Final error: {error_msg}"
                self.result_store.set_job_status(job_id, 'failed', result=final_error_msg)
                return False
        except Exception as e:
            self.logger.error(f"Critical error in job requeue: {e}")
            return False

    def run(self):
        self.heartbeat_thread = threading.Thread(target=self.periodic_heartbeat)
        self.heartbeat_thread.daemon = True
        self._processing_job = False
        self.heartbeat_thread.start()

        # Start worker heartbeat monitoring
        self.worker_monitor_thread = threading.Thread(target=self.monitor_worker_heartbeats)
        self.worker_monitor_thread.daemon = True
        self.worker_monitor_thread.start()

        self.logger.info("Task Processor is now waiting for jobs...")
        self.send_heartbeat("free")  # Start as free instead of inactive

        try:
            for job in self.consumer.consume_jobs():
                self.logger.info(f"Received job {job['job_id']} of type {job['job_type']}")
                self.process_job(job)
        except Exception as e:
            self.logger.critical(f"Job processing failed: {e}")
        finally:
            self.stop_event.set()
            if self.heartbeat_thread:
                self.heartbeat_thread.join(timeout=5)

    @classmethod
    def redistribute_worker_jobs(cls, dead_worker_topic):
        """Redistribute jobs from a dead worker to active workers"""
        redis_client = redis.StrictRedis.from_url(REDIS_URL)
        producer = JobProducer()
        logger = logging.getLogger(__name__)

        # Find job tracking keys for this worker
        job_keys = redis_client.keys("job_tracking:*")
        
        for key in job_keys:
            job_data = redis_client.hgetall(key)
            job_data = {k.decode(): v.decode() for k, v in job_data.items()}
            
            # Check if this job was assigned to the dead worker
            if (job_data.get('status') in ['queued', 'in_progress'] and 
                json.loads(job_data.get('original_job', '{}')).get('assigned_worker') == dead_worker_topic):
                
                try:
                    original_job = json.loads(job_data.get('original_job', '{}'))
                    
                    # Reset job status and remove worker assignment
                    original_job['retry_count'] = original_job.get('retry_count', 0) + 1
                    original_job.pop('assigned_worker', None)
                    
                    # Resubmit job to a new worker
                    producer.submit_job(original_job)
                    
                    logger.info(f"Redistributed job {original_job.get('job_id')} from dead worker {dead_worker_topic}")
                    
                except Exception as e:
                    logger.error(f"Error redistributing job: {e}")

    @classmethod
    def monitor_worker_heartbeats(cls):
        """Background thread to monitor worker heartbeats and redistribute jobs"""
        redis_client = redis.StrictRedis.from_url(REDIS_URL)
        logger = logging.getLogger(__name__)
        
        while True:
            try:
                # Get all active workers
                active_workers = redis_client.smembers("active_workers")
                current_time = time.time()
                
                for worker in active_workers:
                    worker = worker.decode()
                    heartbeat_data = redis_client.hgetall(f"worker:{worker}:heartbeat")
                    
                    # Convert byte data to strings
                    heartbeat_data = {k.decode(): v.decode() for k, v in heartbeat_data.items()}
                    
                    # Check if heartbeat is stale (e.g., more than 30 seconds old)
                    last_heartbeat = float(heartbeat_data.get('last_heartbeat', 0))
                    if current_time - last_heartbeat > 30:
                        logger.warning(f"Worker {worker} appears to be dead. Redistributing its jobs.")
                        
                        # Remove from active workers
                        redis_client.srem("active_workers", worker)
                        
                        # Redistribute jobs
                        cls.redistribute_worker_jobs(worker)
            
            except Exception as e:
                logger.error(f"Error in heartbeat monitoring: {e}")
            
            # Wait before next check
            time.sleep(5)  # Check every 15 seconds

if __name__ == "__main__":
    import signal
    
    topic = sys.argv[1]
    task_processor = TaskProcessor(topic)
    
    def signal_handler(signum, frame):
        print(f"\nReceived shutdown signal. Stopping worker {topic}...")
        task_processor.stop()
        sys.exit(0)
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)   # Handle Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # Handle termination requests
    
    try:
        task_processor.run()
    except Exception as e:
        print(f"Error in worker {topic}: {e}")
        task_processor.stop()
        sys.exit(1)